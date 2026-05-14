"""Partition writer with file locking, atomic same-ts replace, and tail-repair.

Closes P0-1..P0-5 of the file-robustness plan:
  - P0-1: same-ts replace via tmp+rename (ATOMIC) instead of truncate+append.
  - P0-2: fcntl.flock around the entire read_last_ts -> decide -> write sequence.
  - P0-3: no fallback delegation after a partial-write error.
  - P0-4: duplicate-header race prevented by lock-around-decision.
  - P0-5: ROBUST append via _write_all + tail-repair on next access (NOT
    literal atomic append — see note below).

Atomicity profile (honest framing):
  - "replaced_last" is the only TRULY atomic path: writes a unique tmp file
    in the same directory, fsyncs, then `os.replace(tmp, csv_path)` makes
    the swap atomic.
  - "created" uses `O_TRUNC | O_CREAT` + `_write_all`. It is robust to
    short-writes and EINTR, but readers can observe partial state between
    the truncate and the final byte of `_write_all`. A crash mid-write is
    repaired on the next call via `_repair_incomplete_tail`.
  - "appended" is ROBUST: short-write tolerant, EINTR-tolerant, and any
    crash-window partial trailing bytes are repaired on the next call's
    tail-repair step (idempotent, self-healing). Not literal atomic append.

  All non-atomic paths are protected by the partition lock (so no two
  writers race) and by `_repair_incomplete_tail` (so any partial trailing
  bytes from a prior crash are removed before the next decision).

Scope: this writer is specific to candle partition CSV files with the schema
`FAST_APPEND_HEADER = (timestamp, open, close, high, low, volume)` where all
fields are numeric. Tail-repair walks bytes (not csv-reader) because the
schema guarantees no quoted embedded newlines.

Fallback contract:
  - `fallback_fn` MUST be the original (un-wrapped) write function.
  - It is invoked under the partition lock and MUST operate on the same
    `csv_path`.
  - It MUST NOT re-enter `partition_writer.write_batch` for the same path.
"""
from __future__ import annotations

import csv
import errno
import fcntl
import io
import os
import uuid
from typing import Any, Callable, Dict, Literal, Optional, Tuple

from .candle_iterator import (
    FAST_APPEND_HEADER,
    Fore,
    Style,
    WARNING,
)


DurabilityMode = Literal["fsync", "none"]
WriteResult = Literal["created", "appended", "replaced_last", "delegated"]

_VALID_DURABILITY_MODES = ("fsync", "none")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _lock_path_for(csv_path: str) -> str:
    """Return the lock-file path, a hidden sibling in the same directory."""
    directory = os.path.dirname(os.path.abspath(csv_path))
    basename = os.path.basename(csv_path)
    return os.path.join(directory, "." + basename + ".lock")


def _tmp_path_for(csv_path: str) -> str:
    """Return a unique tmp path in the same directory as csv_path."""
    directory = os.path.dirname(os.path.abspath(csv_path))
    basename = os.path.basename(csv_path)
    return os.path.join(
        directory, f".{basename}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
    )


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)


def _write_all(fd: int, data: bytes) -> None:
    """Loop until all bytes are written. Raises OSError on hard failures.

    Tolerates EINTR; surfaces ENOSPC and other errno conditions to caller.
    """
    view = memoryview(data)
    total = len(data)
    written = 0
    while written < total:
        try:
            n = os.write(fd, view[written:])
        except InterruptedError:
            # POSIX EINTR — retry
            continue
        if n == 0:
            # Defensive: 0-byte write on a non-blocking fd; treat as error
            raise OSError(errno.EIO, "os.write returned 0 with bytes remaining")
        written += n


def _fsync_file(fd: int) -> None:
    os.fsync(fd)


def _fsync_dir(path: str) -> None:
    """Fsync the parent directory of `path`. Best-effort on platforms that
    do not support dir fsync (logs but does not raise).
    """
    parent = os.path.dirname(os.path.abspath(path))
    try:
        dir_fd = os.open(parent, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    except OSError:
        # Some filesystems (e.g. older NFS) don't support dir fsync; ignore
        pass
    finally:
        os.close(dir_fd)


def _file_ends_with_newline(csv_path: str) -> bool:
    """True if the file ends with `\\n`, doesn't exist, or is empty."""
    try:
        size = os.path.getsize(csv_path)
    except OSError:
        return True
    if size <= 0:
        return True
    try:
        with open(csv_path, "rb") as f:
            f.seek(-1, os.SEEK_END)
            return f.read(1) == b"\n"
    except OSError:
        return True


def _parse_data_row_bytes(line: bytes) -> bool:
    """True if `line` is a well-formed FAST_APPEND_HEADER data row.

    Schema: EXACTLY 6 numeric fields (timestamp, o, c, h, l, v). Header lines
    (starting with "timestamp" case-insensitive) and rows with the wrong
    column count are rejected.
    """
    if not line:
        return False
    # Quick header check
    if line[:1] in (b"t", b"T") and line[:9].lower() == b"timestamp":
        return False
    parts = line.split(b",")
    if len(parts) != 6:  # strict schema: exactly 6 columns
        return False
    try:
        int(parts[0])
        float(parts[1])
        float(parts[2])
        float(parts[3])
        float(parts[4])
        float(parts[5])
    except (ValueError, IndexError):
        return False
    return True


def _read_last_data_line(csv_path: str) -> Optional[bytes]:
    """Read the file's tail and return the last non-empty, non-header line.

    Returns None on missing/empty/header-only files. The returned bytes do
    NOT include the trailing newline.
    """
    try:
        size = os.path.getsize(csv_path)
    except OSError:
        return None
    if size == 0:
        return None
    read_back = min(8192, size)
    try:
        with open(csv_path, "rb") as f:
            f.seek(-read_back, os.SEEK_END)
            data = f.read()
    except OSError:
        return None
    if not data:
        return None
    lines = [ln for ln in data.split(b"\n") if ln]
    # Walk back; skip header
    for ln in reversed(lines):
        if ln[:1] in (b"t", b"T") and ln[:9].lower() == b"timestamp":
            continue
        return ln
    return None


def _repair_incomplete_tail(csv_path: str) -> int:
    """Remove a trailing partial / malformed data row from `csv_path`.

    Called under the partition lock before read/decide/write. Idempotent.
    Logs a warning when bytes are actually truncated.

    Returns the number of bytes truncated (0 if no repair needed).
    """
    try:
        size = os.path.getsize(csv_path)
    except OSError:
        return 0
    if size == 0:
        return 0

    # Repair step 1: ensure file ends with `\n`. If not, truncate to the
    # previous `\n` (or to 0 if none exists).
    if not _file_ends_with_newline(csv_path):
        truncated = _truncate_to_prev_newline(csv_path)
        if truncated > 0:
            print(
                f"{WARNING} partition_writer: repaired incomplete tail "
                f"(no trailing newline) in {csv_path}: -{truncated} bytes"
                f"{Style.RESET_ALL}"
            )

    # Repair step 2: if the last data line doesn't parse as a valid candle
    # row, truncate it. Repeat until the last line parses or only the header
    # remains.
    while True:
        last = _read_last_data_line(csv_path)
        if last is None or _parse_data_row_bytes(last):
            break
        truncated = _truncate_to_prev_newline(csv_path)
        if truncated == 0:
            break  # nothing more to remove
        print(
            f"{WARNING} partition_writer: repaired malformed last row "
            f"in {csv_path}: -{truncated} bytes{Style.RESET_ALL}"
        )

    final_size = os.path.getsize(csv_path)
    return size - final_size


def _truncate_to_prev_newline(csv_path: str) -> int:
    """Walk backwards from EOF; truncate everything from the trailing
    incomplete content up to (and INCLUDING) the previous `\\n`.

    Returns bytes removed. If the file has no newline at all, truncates to 0.
    """
    try:
        with open(csv_path, "rb+") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            if end == 0:
                return 0
            # If last byte is newline, we want to remove the line up to the
            # PREVIOUS newline. Step back over the trailing newline first.
            f.seek(-1, os.SEEK_END)
            last = f.read(1)
            pos = end - 1
            if last == b"\n":
                pos -= 1
            while pos >= 0:
                f.seek(pos)
                if f.read(1) == b"\n":
                    break
                pos -= 1
            new_size = pos + 1  # keep this newline as the new EOF
            if new_size < 0:
                new_size = 0
            f.truncate(new_size)
            return end - new_size
    except OSError:
        return 0


def _read_last_row_timestamp_bytes(last_line: Optional[bytes]) -> Optional[int]:
    if last_line is None:
        return None
    parts = last_line.split(b",")
    try:
        return int(parts[0])
    except (ValueError, IndexError):
        return None


def _strip_last_data_row(content: bytes) -> bytes:
    """Return `content` with the last data row removed; header preserved.

    Walks backwards from EOF to find the boundary. Assumes the schema (no
    quoted newlines). If the file has only a header (or fewer rows), the
    header is preserved.
    """
    if not content:
        return content

    end = len(content)
    # Skip trailing newline if present
    if content[end - 1: end] == b"\n":
        end -= 1
    if end <= 0:
        return content[:0]
    # Walk back for the previous newline (the boundary between row N-1 and N)
    pos = end - 1
    while pos >= 0 and content[pos: pos + 1] != b"\n":
        pos -= 1
    # Truncate up to and including the newline (pos)
    if pos < 0:
        # No previous newline at all: only one line in the whole file.
        # Return empty (header would have to be on that single line otherwise).
        return content[:0]
    return content[: pos + 1]


def _serialize_rows(canonical_df: Any, include_header: bool) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    if include_header:
        w.writerow(list(FAST_APPEND_HEADER))
    for row in canonical_df.itertuples(index=False, name=None):
        w.writerow(row)
    return buf.getvalue().encode("utf-8")


# --------------------------------------------------------------------------- #
# Lock context manager
# --------------------------------------------------------------------------- #


class _PartitionLock:
    """Exclusive flock on a partition's lock-file."""

    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.lock_path = _lock_path_for(csv_path)
        self._fd: Optional[int] = None

    def __enter__(self) -> "_PartitionLock":
        _ensure_parent_dir(self.lock_path)
        self._fd = os.open(self.lock_path, os.O_CREAT | os.O_WRONLY, 0o644)
        fcntl.flock(self._fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


def write_batch(
    csv_path: str,
    canonical_df: Any,
    *,
    fallback_fn: Optional[Callable[..., Any]] = None,
    fallback_args: Tuple[Any, ...] = (),
    fallback_kwargs: Optional[Dict[str, Any]] = None,
    durability_mode: DurabilityMode = "fsync",
) -> WriteResult:
    """Write `canonical_df` to `csv_path` under a partition lock.

    See module docstring for the contract.
    """
    if durability_mode not in _VALID_DURABILITY_MODES:
        raise ValueError(
            f"Invalid durability_mode {durability_mode!r}; "
            f"expected one of {_VALID_DURABILITY_MODES}"
        )
    if fallback_kwargs is None:
        fallback_kwargs = {}

    _ensure_parent_dir(csv_path)

    with _PartitionLock(csv_path):
        # 1. Tail-repair (idempotent; no-op for clean files)
        if os.path.exists(csv_path):
            _repair_incomplete_tail(csv_path)

        # 2. Inspect: existence, size, last_ts
        exists = os.path.exists(csv_path)
        size = os.path.getsize(csv_path) if exists else 0
        last_ts: Optional[int] = None
        if exists and size > 0:
            last_line = _read_last_data_line(csv_path)
            last_ts = _read_last_row_timestamp_bytes(last_line)

        first_incoming_ts = int(canonical_df["timestamp"].iloc[0])

        # 3. Branch
        if not exists or size == 0 or last_ts is None:
            # New or header-only file: write header + rows
            _write_created(csv_path, canonical_df, durability_mode)
            return "created"

        if first_incoming_ts > last_ts:
            _write_appended(csv_path, canonical_df, durability_mode)
            return "appended"

        if first_incoming_ts == last_ts:
            _write_replaced_last(csv_path, canonical_df, durability_mode)
            return "replaced_last"

        # Overlap / backfill: delegate to fallback under the lock
        if fallback_fn is None:
            raise ValueError(
                f"Overlap detected (first_incoming_ts={first_incoming_ts} "
                f"< last_on_disk={last_ts}) but no fallback_fn provided"
            )
        fallback_fn(*fallback_args, **fallback_kwargs)
        return "delegated"


def _write_created(csv_path: str, canonical_df: Any, durability_mode: DurabilityMode) -> None:
    data = _serialize_rows(canonical_df, include_header=True)
    fd = os.open(csv_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        _write_all(fd, data)
        if durability_mode == "fsync":
            _fsync_file(fd)
    finally:
        os.close(fd)
    if durability_mode == "fsync":
        _fsync_dir(csv_path)


def _write_appended(csv_path: str, canonical_df: Any, durability_mode: DurabilityMode) -> None:
    # Ensure trailing newline on existing file before we append
    if not _file_ends_with_newline(csv_path):
        # Shouldn't happen post-repair, but be defensive
        fd = os.open(csv_path, os.O_WRONLY | os.O_APPEND)
        try:
            _write_all(fd, b"\n")
        finally:
            os.close(fd)
    data = _serialize_rows(canonical_df, include_header=False)
    fd = os.open(csv_path, os.O_WRONLY | os.O_APPEND)
    try:
        _write_all(fd, data)
        if durability_mode == "fsync":
            _fsync_file(fd)
    finally:
        os.close(fd)


def _write_replaced_last(csv_path: str, canonical_df: Any, durability_mode: DurabilityMode) -> None:
    """Atomic: write tmp(existing minus last row + new rows), then os.replace."""
    with open(csv_path, "rb") as f:
        existing = f.read()
    trimmed = _strip_last_data_row(existing)
    new_rows = _serialize_rows(canonical_df, include_header=False)
    new_content = trimmed + new_rows

    tmp = _tmp_path_for(csv_path)
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        try:
            _write_all(fd, new_content)
            if durability_mode == "fsync":
                _fsync_file(fd)
        finally:
            os.close(fd)
        os.replace(tmp, csv_path)
        if durability_mode == "fsync":
            _fsync_dir(csv_path)
    except Exception:
        # Cleanup stale tmp on any error
        try:
            if os.path.exists(tmp):
                os.unlink(tmp)
        except OSError:
            pass
        raise
