"""Tests for the atomic partition_writer module.

Phase 1 of the file-robustness fix plan: validates that the new module
closes P0-1..P0-5 (truncate-then-append crash window, no inter-process lock,
partial-write-plus-fallback corruption, duplicate-header race, non-atomic
append).
"""
from __future__ import annotations

import errno
import os
import threading
import time
from typing import Any, List

import pandas as pd
import pytest

from candle_iterator import candle_iterator as ci_mod
from candle_iterator import partition_writer as pw
from candle_iterator.candle_iterator import FAST_APPEND_HEADER


def _mk_df(rows):
    return pd.DataFrame(rows, columns=["timestamp", "open", "close", "high", "low", "volume"])


def _read(p):
    return p.read_text()


# --------------------------------------------------------------------------- #
# API: 4 branches return the correct WriteResult
# --------------------------------------------------------------------------- #


class TestApiBranches:
    def test_created_returns_label_and_writes_header_plus_rows(self, tmp_path):
        p = tmp_path / "new.csv"
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        result = pw.write_batch(str(p), df)
        assert result == "created"
        text = _read(p)
        assert text.count("timestamp,open,close,high,low,volume") == 1
        assert "1000,1.0,1.5,2.0,0.5,10.0" in text

    def test_appended_returns_label_and_no_duplicate_header(self, tmp_path):
        p = tmp_path / "x.csv"
        p.write_text("timestamp,open,close,high,low,volume\n1000,1.0,1.5,2.0,0.5,10.0\n")
        df = _mk_df([(2000, 2.0, 2.5, 3.0, 1.5, 20.0)])
        result = pw.write_batch(str(p), df)
        assert result == "appended"
        text = _read(p)
        assert text.count("timestamp,open,close,high,low,volume") == 1
        assert "1000," in text and "2000," in text

    def test_replaced_last_returns_label_and_replaces(self, tmp_path):
        p = tmp_path / "x.csv"
        p.write_text("timestamp,open,close,high,low,volume\n1000,1.0,1.5,2.0,0.5,10.0\n")
        df = _mk_df([(1000, 99.0, 99.5, 99.9, 99.1, 999.0)])
        result = pw.write_batch(str(p), df)
        assert result == "replaced_last"
        text = _read(p)
        assert "1000,1.0,1.5,2.0,0.5,10.0" not in text
        assert "1000,99.0,99.5,99.9,99.1,999.0" in text
        assert text.count("timestamp,open,close,high,low,volume") == 1

    def test_replaced_last_plus_future_rows(self, tmp_path):
        p = tmp_path / "x.csv"
        p.write_text("timestamp,open,close,high,low,volume\n1000,1.0,1.5,2.0,0.5,10.0\n")
        df = _mk_df([
            (1000, 99.0, 99.5, 99.9, 99.1, 999.0),
            (2000, 2.0, 2.5, 3.0, 1.5, 20.0),
        ])
        result = pw.write_batch(str(p), df)
        assert result == "replaced_last"
        text = _read(p)
        assert "1000,1.0,1.5,2.0,0.5,10.0" not in text
        assert "1000,99.0,99.5,99.9,99.1,999.0" in text
        assert "2000,2.0,2.5,3.0,1.5,20.0" in text

    def test_delegated_returns_label_and_calls_fallback_in_lock(self, tmp_path):
        p = tmp_path / "x.csv"
        p.write_text("timestamp,open,close,high,low,volume\n2000,2.0,2.5,3.0,1.5,20.0\n")
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        calls = []

        def fallback(*args, **kwargs):
            calls.append((args, kwargs))
            return "irrelevant"

        result = pw.write_batch(
            str(p), df, fallback_fn=fallback,
            fallback_args=("positional",), fallback_kwargs={"key": "val"},
        )
        assert result == "delegated"
        assert calls == [(("positional",), {"key": "val"})]

    def test_overlap_without_fallback_raises(self, tmp_path):
        p = tmp_path / "x.csv"
        p.write_text("timestamp,open,close,high,low,volume\n2000,2.0,2.5,3.0,1.5,20.0\n")
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        with pytest.raises(ValueError, match="Overlap detected"):
            pw.write_batch(str(p), df)  # no fallback_fn


# --------------------------------------------------------------------------- #
# Locking — concurrent writers
# --------------------------------------------------------------------------- #


class TestLocking:
    def test_lock_blocks_second_writer_until_first_releases(self, tmp_path):
        """Acquire the partition lock manually in the test thread, then spawn
        a writer thread. Verify the writer cannot make progress until we
        release the lock.
        """
        import fcntl
        p = tmp_path / "x.csv"
        # Pre-create the file so write_batch takes the append path
        p.write_text("timestamp,open,close,high,low,volume\n1000,1.0,1.5,2.0,0.5,10.0\n")

        lock_path = pw._lock_path_for(str(p))
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_WRONLY, 0o644)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)

        completed = threading.Event()

        def writer():
            df = _mk_df([(2000, 2.0, 2.5, 3.0, 1.5, 20.0)])
            pw.write_batch(str(p), df)
            completed.set()

        t = threading.Thread(target=writer)
        t.start()
        # Give the writer thread a chance to attempt the lock
        time.sleep(0.05)
        # Writer should NOT have completed (blocked on lock)
        assert not completed.is_set(), "Writer should be blocked on partition lock"

        # Release the lock
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)

        # Writer should now complete
        assert completed.wait(timeout=2.0), "Writer didn't complete after lock release"
        t.join()

        # File now contains both rows
        text = _read(p)
        assert "1000," in text
        assert "2000," in text

    def test_concurrent_new_file_creation_no_duplicate_header(self, tmp_path):
        """P0-4 prevention: two threads racing on a non-existent path produce
        ONE header regardless of which thread wins. Use a barrier to maximize
        race likelihood. Both writers use the SAME ts so the second one (after
        the first creates the file) takes the same-ts-replace path — avoiding
        the overlap branch and any test-thread-order flakiness.
        """
        p = tmp_path / "race.csv"
        barrier = threading.Barrier(2)
        errors: List[BaseException] = []

        def racer(open_val):
            df = _mk_df([(1000, open_val, 1.5, 2.0, 0.5, 10.0)])
            barrier.wait()
            try:
                pw.write_batch(str(p), df)
            except BaseException as e:
                errors.append(e)

        # Same ts → second writer hits replace_last path under the lock
        t1 = threading.Thread(target=racer, args=(1.0,))
        t2 = threading.Thread(target=racer, args=(2.0,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert errors == [], f"unexpected errors: {errors}"
        text = _read(p)
        # P0-4 fix: exactly one header regardless of which writer ran first
        assert text.count("timestamp,open,close,high,low,volume") == 1
        # File contains exactly one data row at ts=1000 (one writer's row,
        # whichever ran second wins via replace_last)
        data_rows = [
            ln for ln in text.splitlines()
            if ln and not ln.startswith("timestamp,")
        ]
        assert len(data_rows) == 1
        assert data_rows[0].startswith("1000,")


# --------------------------------------------------------------------------- #
# Atomicity (same-ts replace)
# --------------------------------------------------------------------------- #


class TestAtomicityReplaceLast:
    def test_os_replace_failure_leaves_file_unchanged(self, tmp_path, monkeypatch):
        """If `os.replace` raises mid-rename, the partition file is unchanged
        and tmp is cleaned up.
        """
        p = tmp_path / "x.csv"
        original_text = "timestamp,open,close,high,low,volume\n1000,1.0,1.5,2.0,0.5,10.0\n"
        p.write_text(original_text)

        def bad_replace(src, dst):
            raise OSError(errno.EIO, "simulated EIO")

        monkeypatch.setattr(pw.os, "replace", bad_replace)

        df = _mk_df([(1000, 99.0, 99.5, 99.9, 99.1, 999.0)])
        with pytest.raises(OSError):
            pw.write_batch(str(p), df)
        # File unchanged
        assert _read(p) == original_text
        # Tmp cleaned up (no `.tmp` files in directory)
        leftovers = list(tmp_path.glob("*.tmp"))
        assert leftovers == []

    def test_tmp_path_is_unique_per_call(self, tmp_path):
        """Repeated same-ts replaces on the same file produce unique tmp paths
        (no collision between rapid successive calls)."""
        p = tmp_path / "x.csv"
        p.write_text("timestamp,open,close,high,low,volume\n1000,1.0,1.5,2.0,0.5,10.0\n")
        seen = set()
        for i in range(5):
            seen.add(pw._tmp_path_for(str(p)))
        # All five paths unique
        assert len(seen) == 5


# --------------------------------------------------------------------------- #
# Robust append + tail-repair
# --------------------------------------------------------------------------- #


class TestTailRepair:
    def test_incomplete_trailing_line_no_newline_repaired(self, tmp_path, capsys):
        """File ends mid-row (no trailing newline). Repair truncates the
        partial line before the new append.
        """
        p = tmp_path / "x.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5,2.0,0.5,10.0\n"
            "2000,2.0,2.5,3."  # partial; no newline
        )
        df = _mk_df([(3000, 3.0, 3.5, 4.0, 2.5, 30.0)])
        pw.write_batch(str(p), df)
        text = _read(p)
        # Partial 2000 row gone
        assert "2000,2.0,2.5,3." not in text
        # 1000 row preserved
        assert "1000,1.0,1.5,2.0,0.5,10.0" in text
        # New row appended
        assert "3000,3.0,3.5,4.0,2.5,30.0" in text
        # Warning logged
        captured = capsys.readouterr()
        assert "repaired incomplete tail" in captured.out

    def test_malformed_last_row_with_newline_repaired(self, tmp_path, capsys):
        """Last row has a trailing newline but only 5 columns instead of 6 →
        repair removes the malformed row.
        """
        p = tmp_path / "x.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5,2.0,0.5,10.0\n"
            "2000,bad,row,here,5cols\n"
        )
        df = _mk_df([(3000, 3.0, 3.5, 4.0, 2.5, 30.0)])
        pw.write_batch(str(p), df)
        text = _read(p)
        assert "2000,bad,row,here,5cols" not in text
        assert "3000,3.0,3.5,4.0,2.5,30.0" in text
        captured = capsys.readouterr()
        assert "repaired malformed last row" in captured.out

    def test_repair_back_to_header_only_then_new_batch_clean(self, tmp_path):
        """File: header + 1 malformed row. Repair removes the row, leaving
        header-only. Next batch writes cleanly without duplicating header.
        """
        p = tmp_path / "x.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "bad,row,no,numbers,yet,empty\n"
        )
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        pw.write_batch(str(p), df)
        text = _read(p)
        # Single header, single data row (1000)
        assert text.count("timestamp,open,close,high,low,volume") == 1
        assert "1000," in text
        assert "bad,row" not in text

    def test_no_repair_when_file_is_clean(self, tmp_path, capsys):
        p = tmp_path / "x.csv"
        p.write_text("timestamp,open,close,high,low,volume\n1000,1.0,1.5,2.0,0.5,10.0\n")
        df = _mk_df([(2000, 2.0, 2.5, 3.0, 1.5, 20.0)])
        pw.write_batch(str(p), df)
        captured = capsys.readouterr()
        # No repair warning
        assert "repaired" not in captured.out


# --------------------------------------------------------------------------- #
# Durability mode
# --------------------------------------------------------------------------- #


class TestDurabilityMode:
    def test_fsync_default_calls_os_fsync(self, tmp_path, monkeypatch):
        fsync_calls: List[int] = []
        orig_fsync = pw.os.fsync
        def spy(fd):
            fsync_calls.append(fd)
            return orig_fsync(fd)
        monkeypatch.setattr(pw.os, "fsync", spy)
        p = tmp_path / "new.csv"
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        pw.write_batch(str(p), df)  # default durability_mode="fsync"
        # At least one fsync (file + maybe parent dir)
        assert len(fsync_calls) >= 1

    def test_no_fsync_mode_skips_fsync(self, tmp_path, monkeypatch):
        fsync_calls: List[int] = []
        def spy(fd):
            fsync_calls.append(fd)
        monkeypatch.setattr(pw.os, "fsync", spy)
        p = tmp_path / "new.csv"
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        pw.write_batch(str(p), df, durability_mode="none")
        assert fsync_calls == []

    def test_invalid_durability_mode_raises(self, tmp_path):
        p = tmp_path / "x.csv"
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        with pytest.raises(ValueError, match="durability_mode"):
            pw.write_batch(str(p), df, durability_mode="garbage")


# --------------------------------------------------------------------------- #
# No-fallback-on-partial-write (P0-3 contract)
# --------------------------------------------------------------------------- #


class TestNoFallbackOnPartialWrite:
    def test_write_failure_in_append_raises_not_falls_back(self, tmp_path, monkeypatch):
        """If `_write_all` raises during the append path, the error is
        surfaced to the caller — fallback_fn is NOT invoked. This pins P0-3.
        """
        p = tmp_path / "x.csv"
        p.write_text("timestamp,open,close,high,low,volume\n1000,1.0,1.5,2.0,0.5,10.0\n")

        def raising_write_all(fd, data):
            raise OSError(errno.ENOSPC, "simulated disk full")

        monkeypatch.setattr(pw, "_write_all", raising_write_all)

        fallback_calls: List[Any] = []
        def fallback(*args, **kwargs):
            fallback_calls.append((args, kwargs))

        df = _mk_df([(2000, 2.0, 2.5, 3.0, 1.5, 20.0)])
        with pytest.raises(OSError):
            pw.write_batch(str(p), df, fallback_fn=fallback)
        # fallback NEVER called (no silent corruption via double-write)
        assert fallback_calls == []

    def test_write_failure_in_replace_path_cleans_tmp(self, tmp_path, monkeypatch):
        """In the same-ts-replace path, a write failure during tmp creation
        leaves no stale tmp behind.
        """
        p = tmp_path / "x.csv"
        p.write_text("timestamp,open,close,high,low,volume\n1000,1.0,1.5,2.0,0.5,10.0\n")
        attempt_count = [0]
        orig_write_all = pw._write_all
        def conditional_raise(fd, data):
            attempt_count[0] += 1
            # Fail on the FIRST call (tmp write); not on subsequent writes
            if attempt_count[0] == 1:
                raise OSError(errno.ENOSPC, "simulated ENOSPC")
            return orig_write_all(fd, data)
        monkeypatch.setattr(pw, "_write_all", conditional_raise)
        df = _mk_df([(1000, 99.0, 99.5, 99.9, 99.1, 999.0)])
        with pytest.raises(OSError):
            pw.write_batch(str(p), df)
        # No .tmp files left in directory
        leftovers = list(tmp_path.glob("*.tmp"))
        assert leftovers == []


# --------------------------------------------------------------------------- #
# Short writes via _write_all
# --------------------------------------------------------------------------- #


class TestShortWriteHandling:
    def test_short_write_retries_until_complete(self, tmp_path, monkeypatch):
        """A monkeypatched os.write that returns a partial count forces
        _write_all to loop until done. Final file contains all bytes.
        """
        p = tmp_path / "x.csv"
        # Patch os.write inside the pw module to truncate writes to 5 bytes
        orig_write = pw.os.write
        def short_write(fd, data):
            return orig_write(fd, data[:5])
        monkeypatch.setattr(pw.os, "write", short_write)

        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        pw.write_batch(str(p), df)
        # Full content (header + row) present despite short writes
        text = _read(p)
        assert "timestamp,open,close,high,low,volume" in text
        assert "1000,1.0,1.5,2.0,0.5,10.0" in text

    def test_write_all_handles_EINTR(self, monkeypatch):
        """_write_all retries on InterruptedError (EINTR)."""
        attempts = [0]
        def flaky_write(fd, buf):
            attempts[0] += 1
            if attempts[0] == 1:
                raise InterruptedError
            return len(buf)
        monkeypatch.setattr(pw.os, "write", flaky_write)
        # Use a real fd (devnull)
        fd = os.open("/dev/null", os.O_WRONLY)
        try:
            pw._write_all(fd, b"hello")
        finally:
            os.close(fd)
        assert attempts[0] == 2  # retried after EINTR


# --------------------------------------------------------------------------- #
# Tmp / lock file paths
# --------------------------------------------------------------------------- #


class TestPaths:
    def test_lock_path_is_hidden_sibling(self, tmp_path):
        p = tmp_path / "sub" / "x.csv"
        assert pw._lock_path_for(str(p)) == str(tmp_path / "sub" / ".x.csv.lock")

    def test_tmp_path_in_same_directory(self, tmp_path):
        p = tmp_path / "sub" / "x.csv"
        t = pw._tmp_path_for(str(p))
        assert os.path.dirname(t) == os.path.abspath(str(tmp_path / "sub"))
        assert t.endswith(".tmp")
        assert "x.csv" in t  # basename retained for debuggability


# --------------------------------------------------------------------------- #
# Parent directory creation
# --------------------------------------------------------------------------- #


class TestParentDirCreation:
    def test_creates_nested_parent_directories(self, tmp_path):
        p = tmp_path / "a" / "b" / "c" / "new.csv"
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        pw.write_batch(str(p), df)
        assert p.exists()
        assert (tmp_path / "a" / "b" / "c" / ".new.csv.lock").exists()
