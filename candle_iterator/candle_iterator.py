#!/usr/bin/env python3 

import argparse
import csv
import os
import re
import sys
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Callable, List, Tuple, Dict, Iterator, Optional, Any
from enum import Enum  # CandleClosureSource enum

# ----------------------------------------------------------------------
# 1) COLOR & LOGGING SETUP
# ----------------------------------------------------------------------
try:
    import colorama
    from colorama import Fore, Style
    colorama.init(autoreset=True)
except ImportError:
    class Fore:
        CYAN = ""
        GREEN = ""
        YELLOW = ""
        RED = ""
        MAGENTA = ""
        WHITE = ""
    class Style:
        RESET_ALL = ""

INFO = Fore.GREEN + "[INFO]" + Style.RESET_ALL
WARNING = Fore.YELLOW + "[WARNING]" + Style.RESET_ALL
ERROR = Fore.RED + "[ERROR]" + Style.RESET_ALL
SUCCESS = Fore.GREEN + "[SUCCESS]" + Style.RESET_ALL

COLOR_DIR = Fore.CYAN
COLOR_FILE = Fore.YELLOW
COLOR_TIMESTAMPS = Fore.MAGENTA
COLOR_ROWS = Fore.RED
COLOR_NEW = Fore.WHITE
COLOR_VAR = Fore.CYAN
COLOR_TYPE = Fore.YELLOW
COLOR_DESC = Fore.MAGENTA
COLOR_REQ = Fore.RED + "[REQUIRED]" + Style.RESET_ALL

# Environment key for external poll interval configuration
ENV_POLL_INTERVAL_KEY: str = "CANDLES_SYNC_POLL_INTERVAL_SECS"
MIN_POLL_INTERVAL_SECONDS: int = 1  # sanity lower bound for reported interval
DEFAULT_BACKOFF_BARS: int = 200     # number of bars to back off when no start date is supplied

# Tail-follow constants
TAIL_BACKTRACK_BYTES: int = 4096  # re-parse this many trailing bytes every poll

# Fast-append patch toggles
FAST_APPEND_DISABLE_ENV: str = "CANDLES_SYNC_DISABLE_FAST_APPEND"
FAST_APPEND_HEADER: Tuple[str, ...] = ("timestamp", "open", "close", "high", "low", "volume")

# Pre-compiled regex for relational timeframe expressions (e.g., ">=1h", "<4h")
_RELATION_RE = re.compile(r"^(<=|>=|<|>)([0-9]+[mhDWhd]+)$")

# ----------------------------------------------------------------------
# 2) LOCAL IMPORT: SYNCHRONIZE FUNCTION
# ----------------------------------------------------------------------
from candles_sync import synchronize_candle_data  # noqa: E402

# ----------------------------------------------------------------------
# 2.1) APPEND-ONLY FAST PATH FOR candles_sync.write_partition (monkey patch)
# ----------------------------------------------------------------------
def _truthy_env(val: Optional[str]) -> bool:
    """Module-level truthy check for environment variable values (strict whitelist)."""
    if val is None:
        return False
    return val.strip().lower() in ("1", "true", "yes", "on", "y", "t")


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)


def _file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except Exception:
        return 0


def _file_ends_with_newline(path: str) -> bool:
    try:
        size = os.path.getsize(path)
        if size <= 0:
            return True
        with open(path, "rb") as f:
            f.seek(-1, os.SEEK_END)
            return f.read(1) == b"\n"
    except Exception:
        return True


def _ensure_trailing_newline(path: str) -> None:
    try:
        if os.path.exists(path) and os.path.getsize(path) > 0 and not _file_ends_with_newline(path):
            with open(path, "ab") as f:
                f.write(b"\n")
    except Exception:
        # Non-fatal; worst case is rows are appended right after last byte.
        pass


def _read_last_row_timestamp(csv_path: str) -> Optional[int]:
    """
    Read the last non-empty, non-header line's timestamp quickly without
    loading the whole file. Returns None if file does not exist or has no rows.
    """
    try:
        if not os.path.exists(csv_path):
            return None
        size = os.path.getsize(csv_path)
        if size == 0:
            return None

        # Read a small tail window; expand if needed.
        read_back = min(8192, size)
        with open(csv_path, "rb") as f:
            f.seek(-read_back, os.SEEK_END)
            data = f.read()

        if not data:
            return None

        # Normalize to text lines
        text = data.decode("utf-8", errors="ignore")
        if not text:
            return None
        lines = [ln for ln in text.splitlines() if ln.strip()]

        # If the file ends with a newline, last lines[-1] is the final complete row (or empty).
        # Walk backwards to find a data line (skip header if encountered).
        for ln in reversed(lines):
            parts = ln.split(",")
            if not parts:
                continue
            t0 = parts[0].strip().lower()
            if t0 == "timestamp":
                # header; keep searching earlier line
                continue
            try:
                return int(parts[0])
            except Exception:
                continue
        return None
    except Exception as exc:
        print(f"{WARNING} _read_last_row_timestamp failed for {csv_path}: {exc}{Style.RESET_ALL}")
        return None


def _truncate_last_line(csv_path: str) -> bool:
    """
    Truncate the file to remove the last line (including its newline if present).
    This is used for a tiny tail de-dup when the incoming chunk's first timestamp
    equals the last on-disk timestamp.
    """
    try:
        if not os.path.exists(csv_path):
            return False
        with open(csv_path, "rb+") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            if end == 0:
                return True  # empty file already
            # If last byte is newline, step one byte back so loop lands on the previous character.
            f.seek(-1, os.SEEK_END)
            last = f.read(1)
            pos = end - 1
            if last == b"\n":
                pos -= 1
            # Walk backwards to the previous newline or BOF
            while pos >= 0:
                f.seek(pos)
                ch = f.read(1)
                if ch == b"\n":
                    break
                pos -= 1
            # pos is at newline (or -1). Truncate to pos+1 (leave that newline as the new EOF).
            new_size = pos + 1
            if new_size < 0:
                new_size = 0
            f.truncate(new_size)
        return True
    except Exception:
        return False


def _canonicalize_input_df(df: Any) -> Optional[Any]:
    """
    Canonicalize, self-deduplicate, and sort the incoming pandas DataFrame.
    The resulting frame has columns: timestamp, open, close, high, low, volume
    and proper dtypes. Returns None on failure.
    """
    try:
        import pandas as pd  # local import; only needed if candles_sync passes a DataFrame
    except ImportError:
        return None

    if not hasattr(df, "columns") or not hasattr(df, "rename") or not hasattr(df, "sort_values"):
        return None

    # Build a flexible column mapping
    col_map: Dict[str, List[str]] = {
        "timestamp": ["timestamp", "ts", "time", "t"],
        "open": ["open", "o"],
        "close": ["close", "c"],
        "high": ["high", "h"],
        "low": ["low", "l"],
        "volume": ["volume", "vol", "v"],
    }

    rename: Dict[str, str] = {}
    lower_cols = {str(c).lower(): c for c in list(df.columns)}
    for canon, candidates in col_map.items():
        found = None
        for cand in candidates:
            if cand in lower_cols:
                found = lower_cols[cand]
                break
        if found is None:
            # Missing a required column -> give up; let original path handle it.
            return None
        rename[found] = canon

    try:
        work = df.rename(columns=rename)
        work = work[list(FAST_APPEND_HEADER)]  # enforce order
        # Dtypes
        work["timestamp"] = work["timestamp"].astype("int64")
        for col in ("open", "close", "high", "low", "volume"):
            work[col] = work[col].astype("float64")
        # Self-dedup on timestamp (keep last), then sort ascending
        work = work.dropna(subset=["timestamp"])
        work = work.drop_duplicates(subset=["timestamp"], keep="last")
        work = work.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
        # Ensure monotonic non-decreasing timestamps
        if work.empty:
            return None
        return work
    except Exception as exc:
        print(f"{WARNING} _canonicalize_input_df failed: {exc}{Style.RESET_ALL}")
        return None


def _append_rows(csv_path: str, df: Any, *, include_header: bool) -> None:
    """
    Append rows from a canonicalized DataFrame to CSV, without touching existing rows.
    This path avoids reading the existing CSV.
    """
    _ensure_parent_dir(csv_path)
    # If appending into non-empty file and header is not requested, ensure a trailing newline exists.
    if not include_header:
        _ensure_trailing_newline(csv_path)

    mode = "a" if os.path.exists(csv_path) else "w"
    with open(csv_path, mode, encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if include_header:
            writer.writerow(list(FAST_APPEND_HEADER))
        # df is canonicalized; iterate without index
        for row in df.itertuples(index=False, name=None):
            writer.writerow(row)


def _extract_path_and_df(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Tuple[Optional[str], Optional[Any]]:
    """
    Try to locate (csv_path, df) in positional/keyword arguments of candles_sync.write_partition.
    This is defensive to tolerate slightly different signatures.
    """
    # Heuristic for path
    path_candidates = (
        kwargs.get("path") or kwargs.get("csv_path") or kwargs.get("file_path") or
        kwargs.get("filepath") or kwargs.get("out_path") or kwargs.get("dst_path") or kwargs.get("dst")
    )
    csv_path: Optional[str] = None
    if isinstance(path_candidates, str):
        csv_path = path_candidates
    else:
        # search positional args for a plausible path string
        for a in args:
            if isinstance(a, str) and a.lower().endswith(".csv"):
                csv_path = a
                break

    # Heuristic for DataFrame-like
    df: Optional[Any] = None
    for k in ("df", "frame", "data", "rows", "chunk"):
        if k in kwargs:
            df = kwargs[k]
            break
    if df is None:
        for a in args:
            if hasattr(a, "to_csv") and hasattr(a, "columns"):
                df = a
                break

    return csv_path, df


def _fast_write_partition_wrapper_factory(original_write_fn: Any):
    """
    Produce a wrapper that routes writes through the atomic partition_writer
    module when possible, falling back to the original (slow merge) writer
    for pre-write setup failures and for overlap/backfill writes.

    Pre-write errors (extract / canonicalize) still fall back to the original
    writer (safe — no on-disk mutation yet). Errors INSIDE the partition
    writer (during the actual write, under the lock) are surfaced to the
    caller per the P0-3 contract — DO NOT delegate after a partial write.
    """
    def _wrapped_write(*args: Any, **kwargs: Any) -> Any:
        # Allow users to disable the optimization via ENV for any reason.
        if _truthy_env(os.environ.get(FAST_APPEND_DISABLE_ENV)):
            return original_write_fn(*args, **kwargs)

        # ---- Pre-write setup (may safely fall back on error) ----
        try:
            csv_path, df = _extract_path_and_df(args, kwargs)
            if not csv_path or df is None:
                return original_write_fn(*args, **kwargs)

            canon_df = _canonicalize_input_df(df)
            if canon_df is None or len(canon_df) == 0:
                return original_write_fn(*args, **kwargs)
        except Exception as exc:
            print(
                f"{WARNING} fast-append wrapper failed ({exc}), falling back "
                f"to original writer{Style.RESET_ALL}"
            )
            return original_write_fn(*args, **kwargs)

        # ---- Atomic write via partition_writer (no fallback on partial write) ----
        # Import here to avoid circular import at module load
        from . import partition_writer as _pw
        _pw.write_batch(
            csv_path, canon_df,
            fallback_fn=original_write_fn,
            fallback_args=args, fallback_kwargs=kwargs,
        )
        return None

    setattr(_wrapped_write, "_fast_append_installed", True)
    return _wrapped_write


def _install_fast_append_writer() -> None:
    """
    Monkey-patch candles_sync.write_partition with an append-only optimized wrapper.
    This avoids pandas read/concat/sort/rewrite in steady-state polling.
    """
    try:
        import candles_sync as _cs  # import the module itself to patch attribute
    except Exception:
        return
    try:
        # Idempotent
        if getattr(_cs, "_fast_append_writer_installed", False):
            return
        if not hasattr(_cs, "write_partition"):
            return
        original = getattr(_cs, "write_partition")
        # Avoid double wrapping if someone already installed a wrapper
        if getattr(original, "_fast_append_installed", False):
            setattr(_cs, "_fast_append_writer_installed", True)
            return
        wrapped = _fast_write_partition_wrapper_factory(original)
        setattr(_cs, "write_partition", wrapped)
        setattr(_cs, "_fast_append_writer_installed", True)
    except Exception as exc:
        # Never fail the import path due to patching.
        print(f"{WARNING} fast-append writer install failed: {exc}{Style.RESET_ALL}")


# Attempt to install at import time (safe, idempotent). Also called again in factory for certainty.
_install_fast_append_writer()

# ----------------------------------------------------------------------
# 3) TIMEFRAME DEFINITIONS
# ----------------------------------------------------------------------
TIMEFRAMES = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "3h": 10_800_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1D": 86_400_000,
    "2D": 172_800_000,
    "3D": 259_200_000,
    "4D": 345_600_000,
    "1W": 604_800_000,
    "14D": 1_209_600_000
}


def parse_aggregation_timeframes(tokens):
    """
    Parse multiple tokens that may be direct timeframes (e.g. "5m")
    or relational operators (e.g. ">=1h", "<=4h"), returning a sorted list.
    """
    final_set = set()
    for token in tokens:
        if '&' in token:
            parts = token.split('&')
            sets = [parse_single_relation_or_exact(x) for x in parts]
            intersection = sets[0]
            for s2 in sets[1:]:
                intersection = intersection.intersection(s2)
            final_set = final_set.union(intersection)
        else:
            final_set = final_set.union(parse_single_relation_or_exact(token))
    return sorted(final_set, key=lambda tf: TIMEFRAMES[tf])


def parse_single_relation_or_exact(expr):
    expr = expr.strip()
    if expr in TIMEFRAMES:
        return {expr}

    match = _RELATION_RE.match(expr)
    if match:
        op = match.group(1)
        tf_str = match.group(2)
        if tf_str not in TIMEFRAMES:
            print(f"{WARNING} Unknown timeframe in expression: {tf_str}. Skipping.")
            return set()
        ref_ms = TIMEFRAMES[tf_str]
        possible = set(TIMEFRAMES.keys())
        out = set()
        if op == "<":
            out = {k for k in possible if TIMEFRAMES[k] < ref_ms}
        elif op == "<=":
            out = {k for k in possible if TIMEFRAMES[k] <= ref_ms}
        elif op == ">":
            out = {k for k in possible if TIMEFRAMES[k] > ref_ms}
        elif op == ">=":
            out = {k for k in possible if TIMEFRAMES[k] >= ref_ms}
        return out

    print(f"{WARNING} Could not parse aggregation-tf token: '{expr}'. Skipping.")
    return set()


# ----------------------------------------------------------------------
# CandleClosureSource enum
# ----------------------------------------------------------------------
class CandleClosureSource(str, Enum):
    """
    Denotes the origin of a CandleClosure:
      - HISTORICAL: produced while replaying historical files.
      - LIVE: produced after entering polling mode (including live snapshots).
    """
    HISTORICAL = "Historical"
    LIVE = "Live"


@dataclass(slots=True)
class Candle:
    """
    Holds a single candle's OHLCV data.
    """
    timeframe: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp / 1000, tz=timezone.utc)

    def __str__(self) -> str:
        dt_str = self.datetime.strftime("%Y-%m-%d %H:%M")
        return (f"{self.timeframe} :: {self.timestamp} {dt_str} :: "
                f"o={self.open},h={self.high},l={self.low},c={self.close},v={self.volume}")


@dataclass(slots=True)
class CandleClosure:
    """
    A closure event from AggregatorManager, containing:
      - closed_candles: the last fully closed candle(s) for each timeframe
      - open_candles: partial candle(s) still open
      - source: whether this closure originates from historical playback or live polling
    """
    timestamp: int
    closed_candles: Dict[str, Candle] = field(default_factory=dict)
    open_candles: Dict[str, Candle] = field(default_factory=dict)
    last_closed: Optional['CandleClosure'] = None
    is_final: bool = False
    source: CandleClosureSource = CandleClosureSource.HISTORICAL  # default; overridden by manager

    @property
    def datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp / 1000, tz=timezone.utc)

    @property
    def timeframes(self) -> List[str]:
        tfs = set(self.closed_candles.keys()).union(self.open_candles.keys())
        return sorted(tfs, key=lambda tf: TIMEFRAMES[tf])

    def get_candle(self, timeframe: str, closed=True) -> Optional[Candle]:
        if closed:
            return self.closed_candles.get(timeframe)
        else:
            return self.open_candles.get(timeframe)

    def printsmall(self):
        dt_str = self.datetime.strftime("%Y-%m-%d %H:%M")
        closed_tfs = ','.join(sorted(self.closed_candles.keys()))
        open_tfs = ','.join(sorted(self.open_candles.keys()))
        return (f"[Closure] T={self.timestamp} {dt_str} | "
                f"Closed: [{closed_tfs}] | Open: [{open_tfs}] | Final: {self.is_final}")

    def print(self):
        dt_str = self.datetime.strftime("%Y-%m-%d %H:%M")
        now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        print(f"{Fore.YELLOW}[Closure - {now_utc_str}]{Style.RESET_ALL} "
              f"T={self.timestamp} {dt_str} => Timeframes in snapshot: {', '.join(self.timeframes)}")
        print(f"{Fore.GREEN}  Closed Candles:{Style.RESET_ALL}")
        for tf in sorted(self.closed_candles.keys(), key=lambda x: TIMEFRAMES[x]):
            print(f"    - {self.closed_candles[tf]}")

        if self.open_candles:
            print(f"{Fore.RED}  Open Candles (partial):{Style.RESET_ALL}")
            for tf in sorted(self.open_candles.keys(), key=lambda x: TIMEFRAMES[x]):
                print(f"    - {self.open_candles[tf]}")


# ----------------------------------------------------------------------
# CandleAggregator
# ----------------------------------------------------------------------
@dataclass(slots=True)
class Partial:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float


class CandleAggregator:
    """
    Aggregator for one timeframe.

    Contract (strict close):
    - A base timeframe candle is finalized **only** when the next interval's
      base candle is actually observed (i.e., we ingest a row whose timestamp
      is >= current_boundary + tf_ms). We never finalize just because wall-clock
      time has advanced.

    Behaviors:
    - "Seed next open": immediately after finalizing a candle, create a synthetic
      open at the next boundary (OHLC=last close, volume=0). This appears only
      after a legitimate close.
    - Higher-TF preview: accept real-time base partial via on_base_partial()
      and overlay it non-destructively in get_current_open_candle().
    - Guard against double-count in preview by ignoring a "partial" whose
      timestamp equals last finalized sub-timestamp.
    """
    def __init__(self, timeframe: str, manager: 'AggregatorManager', is_base: bool = False, sub_factor: int = 1, anchor_offset_ms: int = 0):
        self.manager = manager
        self.tf = timeframe
        self.tf_ms = TIMEFRAMES[timeframe]
        self.is_base = is_base
        self.sub_factor = sub_factor

        # Anchor offset for boundary alignment. Default 0 = epoch-aligned (legacy
        # behavior: weeks fall on Thursdays because 1970-01-01 was Thursday).
        # Non-zero values shift the boundary; e.g. -259_200_000 (3 days) aligns
        # weekly boundaries to Monday 00:00 UTC for ISO-week semantics.
        # Same unit as tf_ms; effectively reduced modulo tf_ms.
        self.anchor_offset_ms = anchor_offset_ms

        # Rolling boundary state
        self.current_boundary: Optional[int] = None

        # Seeded "next open" (preview after a legitimate close)
        self._seed_next_open_ts: Optional[int] = None
        self._seed_next_open_price: Optional[float] = None

        # Live base 1m partial snapshot for higher-TF previews (is_base == False)
        self._live_partial: Optional[Partial] = None  # ts,o,h,l,c,v

        # State of the currently forming *committed* aggregator candle
        self.open_ts: Optional[int] = None
        self.open_px: Optional[float] = None
        self.high_px: Optional[float] = None
        self.low_px: Optional[float] = None
        self.close_px: Optional[float] = None
        self.volume: float = 0.0
        self.sub_count: int = 0
        self.last_sub_ts: Optional[int] = None

    # -----------------------------
    # Boundary alignment
    # -----------------------------
    def _align_to_boundary(self, ts: int) -> int:
        """Floor `ts` to this aggregator's boundary, respecting anchor_offset_ms.

        With anchor_offset_ms == 0 (default), this is equivalent to the legacy
        epoch-aligned floor `(ts // self.tf_ms) * self.tf_ms`.
        With anchor_offset_ms != 0, the result is shifted so that boundaries
        fall on multiples of tf_ms PLUS the offset — e.g. with tf_ms=604_800_000
        and anchor_offset_ms=-259_200_000, boundaries are Monday 00:00 UTC.
        """
        return ((ts - self.anchor_offset_ms) // self.tf_ms) * self.tf_ms + self.anchor_offset_ms

    # -----------------------------
    # Base aggregator ingestion
    # -----------------------------
    def on_base_csv_row(self, ts: int, o: float, h: float, l: float, c: float, v: float) -> None:
        """
        Called only for the base aggregator. Each row is the latest snapshot for its minute.
        We finalize the previous minute **only when** a subsequent minute (or later) row arrives.
        """
        if not self.is_base:
            raise RuntimeError("Called on_base_csv_row() on a non-base aggregator? (bug)")
        self._consume_subcandle(ts, o, h, l, c, v, volume_delta=False)

    # -----------------------------
    # Higher timeframe ingestion (from closed base candles)
    # -----------------------------
    def on_base_candle_closed(self, base_ts: int, o: float, h: float, l: float, c: float, v: float) -> None:
        """
        For higher timeframes (is_base=False). We accumulate each *closed* 1m subcandle.
        Once we have sub_factor subcandles, we finalize. Also handle multi-interval jumps.
        """
        if self.is_base:
            raise RuntimeError("Base aggregator called on_base_candle_closed? (bug)")
        self._consume_subcandle(base_ts, o, h, l, c, v, volume_delta=True)

    # -----------------------------
    # Higher timeframe live preview (from *open* base candles)
    # -----------------------------
    def on_base_partial(self, base_ts: int, o: float, h: float, l: float, c: float, v: float) -> None:
        if self.is_base:
            return  # base aggregator does not need this

        if self.last_sub_ts is not None and int(base_ts) == int(self.last_sub_ts):
            return  # avoid double-count preview

        boundary = self._align_to_boundary(base_ts)
        if self.current_boundary is None:
            self.current_boundary = boundary

        self._live_partial = Partial(ts=int(base_ts), o=float(o), h=float(h), l=float(l), c=float(c), v=float(v))

    # -----------------------------
    # Unified ingestion core
    # -----------------------------
    def _consume_subcandle(self, base_ts: int, o: float, h: float, l: float, c: float, v: float, *, volume_delta: bool) -> None:
        """
        Common ingestion path for both base rows (volume_delta=False) and higher-TF
        accumulation of closed base candles (volume_delta=True).
        """
        if self.current_boundary is None:
            self.current_boundary = self._align_to_boundary(base_ts)

        # Handle jumps across boundaries:
        while base_ts >= self.current_boundary + self.tf_ms:
            if self.sub_count > 0:
                closing_price = self.close_px if self.close_px is not None else self.open_px
                real_event_ts = self.last_sub_ts if self.last_sub_ts is not None else self.current_boundary
                self._finalize_aggregator_candle(real_event_ts=real_event_ts)
                if closing_price is not None:
                    self._seed_next_open(self.current_boundary + self.tf_ms, closing_price)
            else:
                # No data occurred in this boundary; emit a zero candle
                zero_event_ts = (self.last_sub_ts if (self.last_sub_ts is not None and not self.is_base) else self.current_boundary)
                self._record_zero_candle(real_event_ts=zero_event_ts)

            self.current_boundary += self.tf_ms
            self._clear_open_state()

        # Initialize forming candle if needed
        if self.open_ts is None:
            self.open_ts = self.current_boundary
            self.open_px = o
            self.high_px = o
            self.low_px = o
            self.close_px = o
            self.volume = 0.0
            self.sub_count = 0

        # Update forming candle
        self.high_px = max(self.high_px, h) if self.high_px is not None else h
        self.low_px = min(self.low_px, l) if self.low_px is not None else l
        self.close_px = c
        if volume_delta:
            self.volume += v
        else:
            self.volume = float(v)
        self.sub_count += 1
        self.last_sub_ts = base_ts

        # Drop preview if it was exactly this now-closed minute (for higher TFs)
        if not self.is_base and self._live_partial is not None:
            if int(self._live_partial.ts) == base_ts:
                self._live_partial = None

        # Finalize in higher TF when we've reached required subcandles
        if not self.is_base and self.sub_count >= self.sub_factor:
            closing_price = self.close_px if self.close_px is not None else self.open_px
            self._finalize_aggregator_candle(real_event_ts=base_ts)
            if closing_price is not None:
                self._seed_next_open(self.current_boundary + self.tf_ms, closing_price)
            self.current_boundary += self.tf_ms
            self._clear_open_state()

    # -----------------------------
    # Helpers: record/finalize/seed
    # -----------------------------
    def _clear_open_state(self) -> None:
        """Clear the open-candle state for the next boundary."""
        self.open_ts = None
        self.open_px = None
        self.high_px = None
        self.low_px = None
        self.close_px = None
        self.volume = 0.0
        self.sub_count = 0
        self.last_sub_ts = None
        if not self.is_base:
            self._live_partial = None

    def _seed_next_open(self, next_ts: int, price: float) -> None:
        self._seed_next_open_ts = int(next_ts)
        self._seed_next_open_price = float(price)

    def _record_zero_candle(self, real_event_ts: int) -> None:
        self.manager.record_closure(
            aggregator_closure_ts=real_event_ts,
            tf=self.tf,
            label_ts=self.current_boundary,
            o=0.0, h=0.0, l=0.0, c=0.0,
            vol=0.0
        )

    def _finalize_aggregator_candle(self, real_event_ts: int) -> None:
        if self.open_ts is None:
            return
        if self.manager and getattr(self.manager, "verbose", False):
            print(
                f"{Fore.MAGENTA}[DEBUG]{Style.RESET_ALL} aggregator {self.tf} FINALIZING => "
                f"boundary={self.current_boundary}, "
                f"o={self.open_px},h={self.high_px},l={self.low_px},c={self.close_px},vol={self.volume}"
            )
        self.manager.record_closure(
            aggregator_closure_ts=real_event_ts,
            tf=self.tf,
            label_ts=self.open_ts,
            o=self.open_px,
            h=self.high_px,
            l=self.low_px,
            c=self.close_px,
            vol=self.volume
        )

    def get_current_open_candle(self) -> Optional[Candle]:
        """
        Returns a preview of the *currently open* candle.

        - If a committed open candle exists, return it.
        - Else, if a seeded next open exists (only after a legitimate close), show that.
        - For higher TFs, overlay the latest base partial on the committed state.
        """
        boundary_ts = None
        o = h = l = c = None
        committed_vol = 0.0

        if self.open_ts is not None:
            boundary_ts = self.open_ts
            o = self.open_px
            h = self.high_px
            l = self.low_px
            c = self.close_px
            committed_vol = float(self.volume)
        elif self._seed_next_open_ts is not None and self._seed_next_open_price is not None:
            boundary_ts = self._seed_next_open_ts
            o = h = l = c = self._seed_next_open_price
            committed_vol = 0.0

        preview_vol = committed_vol
        if not self.is_base and self._live_partial is not None:
            lp_ts = int(self._live_partial.ts)
            if self.last_sub_ts is None or lp_ts != int(self.last_sub_ts):
                lp_boundary = self._align_to_boundary(lp_ts)
                if boundary_ts is None:
                    boundary_ts = lp_boundary
                    o = h = l = c = self._live_partial.o
                    committed_vol = 0.0
                if boundary_ts == lp_boundary:
                    if self.sub_count == 0:
                        o = self._live_partial.o
                    h = max(h, self._live_partial.h) if h is not None else self._live_partial.h
                    l = min(l, self._live_partial.l) if l is not None else self._live_partial.l
                    c = self._live_partial.c
                    preview_vol = committed_vol + self._live_partial.v

        if boundary_ts is None or o is None or h is None or l is None or c is None:
            return None

        return Candle(
            timeframe=self.tf,
            timestamp=int(boundary_ts),
            open=o,
            high=h,
            low=l,
            close=c,
            volume=preview_vol,
        )


# ----------------------------------------------------------------------
# AggregatorManager
# ----------------------------------------------------------------------
class AggregatorManager:
    """
    Manages a base aggregator plus any higher aggregators.

    Notes for strict base close:
    - We never force-close the base TF from wall-clock time. Closures are emitted
      only when the next base interval row is observed.
    - Heartbeat snapshots are available at any time to show current open previews.
    """
    def __init__(self, base_tf: str, higher_tfs: List[str], verbose: bool = False,
                 anchor_offsets: Optional[Dict[str, int]] = None,
                 force_close_final: bool = False):
        """
        anchor_offsets (optional): per-TF boundary offset in ms. Default None preserves
        the legacy epoch-aligned behavior (weeks fall on Thursdays). Example for
        ISO-week semantics (Monday-start): {"1W": -259_200_000}.
        Same unit as tf_ms; effectively reduced modulo tf_ms per-TF.
        Offset for the base TF and any TF not present in the dict defaults to 0.

        force_close_final (optional): when True, flush() will force-close the BASE
        timeframe's currently-open candle as if a subsequent bar had arrived. This
        lets end-of-data runs (e.g. a daily-equity scan after market close) fire
        alerts on the latest bar instead of waiting for the next trading day's
        bar to arrive. Higher TFs are NOT force-closed — they follow normal
        sub_factor / boundary-jump rules. Default False preserves the legacy
        "no force-close on wall-clock" behavior.
        """
        self.verbose = verbose
        self.base_tf = base_tf
        self.anchor_offsets: Dict[str, int] = dict(anchor_offsets or {})
        self.force_close_final = force_close_final

        base_offset = self.anchor_offsets.get(base_tf, 0)
        self.base_agg = CandleAggregator(base_tf, manager=self, is_base=True,
                                          anchor_offset_ms=base_offset)
        self.higher_aggs: List[CandleAggregator] = []

        base_ms = TIMEFRAMES[base_tf]
        for tf in higher_tfs:
            factor = TIMEFRAMES[tf] // base_ms
            tf_offset = self.anchor_offsets.get(tf, 0)
            agg = CandleAggregator(tf, manager=self, is_base=False, sub_factor=factor,
                                    anchor_offset_ms=tf_offset)
            self.higher_aggs.append(agg)

        # Snapshots of latest closed by TF (stored as Candle objects directly)
        self.latest_closed_candles: Dict[str, Candle] = {}

        # Pending closures since the last emission:
        # A list of (event_ts, {tf: Candle})
        self._pending: List[Tuple[int, Dict[str, Candle]]] = []
        self._pending_tf_count: int = 0  # O(1) counter for total TFs across pending buckets

        # Rolling "as-of-last-emission" closed state (Candle objects)
        self._last_emitted_closed_state: Dict[str, Candle] = {}

        self._all_timeframes = [self.base_tf] + higher_tfs
        self._final_partial_emitted = False  # used only for finite-range mode

        # Source for CandleClosure objects (Historical by default)
        self.closure_source: CandleClosureSource = CandleClosureSource.HISTORICAL

        # Timing diagnostics toggle
        self.debug_timing: bool = _truthy_env(os.environ.get("CANDLES_SYNC_DEBUG_TIMING")) or bool(self.verbose)
        self._timing_prefix = Fore.CYAN + "[TIMING]" + Style.RESET_ALL

    # ---------- Public snapshot utilities ----------
    def preferred_snapshot_ts(self) -> Optional[int]:
        """Pick a monotonic event timestamp for a heartbeat snapshot."""
        best: Optional[int] = None
        base_open = self.base_agg.get_current_open_candle()
        if base_open:
            best = base_open.timestamp
        for agg in self.higher_aggs:
            oc = agg.get_current_open_candle()
            if oc and (best is None or oc.timestamp > best):
                best = oc.timestamp
        for candle in self.latest_closed_candles.values():
            if best is None or candle.timestamp > best:
                best = candle.timestamp
        return best

    def set_closure_source(self, source: CandleClosureSource) -> None:
        """Update the current source used when constructing CandleClosure objects."""
        self.closure_source = source

    def _current_open_candles(self) -> Dict[str, Candle]:
        """Collect current open candles from base + higher aggregators."""
        open_candles: Dict[str, Candle] = {}
        if (c := self.base_agg.get_current_open_candle()):
            open_candles[self.base_tf] = c
        for agg in self.higher_aggs:
            if (c := agg.get_current_open_candle()):
                open_candles[agg.tf] = c
        return open_candles

    def build_snapshot_closure(self, event_ts: Optional[int] = None) -> Optional[CandleClosure]:
        """
        Build a single snapshot closure (no pending finalized closures are consumed).
        Preserves original external behavior and API.
        """
        ts = int(event_ts) if event_ts is not None else self.preferred_snapshot_ts()
        if ts is None:
            return None

        # Closed = latest closed per TF where label_ts <= snapshot ts
        closed_candles: Dict[str, Candle] = {
            tf: candle for tf, candle in self.latest_closed_candles.items()
            if candle.timestamp <= ts
        }

        open_candles = self._current_open_candles()

        if not closed_candles and not open_candles:
            return None

        return CandleClosure(
            timestamp=ts,
            closed_candles=closed_candles,
            open_candles=open_candles,
            last_closed=None,
            is_final=False,
            source=self.closure_source
        )

    # ---------- Ingestion ----------
    def record_closure(self, aggregator_closure_ts: int, tf: str, label_ts: int,
                       o: float, h: float, l: float, c: float, vol: float) -> None:
        """
        Called by CandleAggregator when a timeframe closes (or a zero candle recorded).
        Timing note: this is the earliest moment a closure becomes 'pending'.
        """
        t0 = time.perf_counter()

        # Propagate base closures upward
        if tf == self.base_tf:
            for agg in self.higher_aggs:
                agg.on_base_candle_closed(aggregator_closure_ts, o, h, l, c, vol)

        # Update "latest closed" and collect pending closure for this event_ts
        candle = Candle(tf, label_ts, o, h, l, c, vol)
        self.latest_closed_candles[tf] = candle

        if self._pending and self._pending[-1][0] == aggregator_closure_ts:
            # Merge into existing last bucket (only increment if tf is new to this bucket)
            if tf not in self._pending[-1][1]:
                self._pending_tf_count += 1
            self._pending[-1][1][tf] = candle
        else:
            # Append new bucket (we deliberately keep simple ordered list)
            self._pending.append((aggregator_closure_ts, {tf: candle}))
            self._pending_tf_count += 1

        if self.debug_timing:
            dt_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"{self._timing_prefix} {dt_str} record_closure tf={tf} label_ts={label_ts} "
                f"event_ts={aggregator_closure_ts} pending_tf_count={self._pending_tf_count} "
                f"handler_s={(time.perf_counter()-t0):.6f}"
            )

    def on_subcandle(self, ts: int, o: float, h: float, l: float, c: float, v: float) -> List[CandleClosure]:
        # Feed into base
        self.base_agg.on_base_csv_row(ts, o, h, l, c, v)

        # Forward genuine open-minute partials to higher TFs
        if self.base_agg.open_ts is not None and int(ts) == int(self.base_agg.open_ts):
            for agg in self.higher_aggs:
                agg.on_base_partial(ts, o, h, l, c, v)

        # Only return real finalized closures here; snapshots are requested explicitly by the iterator.
        return self._make_closure(event_ts=None, include_pending=True)

    # ---------- Internal: build closures (pending and snapshot) ----------
    def _make_closure(self, event_ts: Optional[int], *, include_pending: bool) -> List[CandleClosure]:
        """
        Core builder:
          - If include_pending and there are pending finalized closures, emit one
            CandleClosure per unique event_ts (sorted ascending), using a *rolling*
            closed-state to preserve correct "as-of-ts" snapshots.
          - Else, build a single snapshot at event_ts (or preferred snapshot ts if None).
        """
        t0 = time.perf_counter()
        out: List[CandleClosure] = []

        if include_pending and self._pending:
            # Consume pending in ascending event_ts order (they are already recorded in order)
            open_candles = self._current_open_candles()

            # Start from the last emitted closed snapshot (ensures correct as-of semantics)
            rolling_closed: Dict[str, Candle] = dict(self._last_emitted_closed_state)

            for closure_ts, delta in self._pending:
                # Apply the closures that happened at this event_ts
                rolling_closed.update(delta)

                # Snapshot: copy dict of references (Candle objects are immutable-by-convention)
                closed_candles: Dict[str, Candle] = dict(rolling_closed)

                cc = CandleClosure(
                    timestamp=closure_ts,
                    closed_candles=closed_candles,
                    open_candles=open_candles,
                    last_closed=None,
                    is_final=False,
                    source=self.closure_source
                )
                out.append(cc)

            # Update rolling baseline and clear pending
            self._last_emitted_closed_state = rolling_closed
            self._pending.clear()
            self._pending_tf_count = 0

            if self.debug_timing:
                dt_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                print(
                    f"{self._timing_prefix} {dt_str} make_closure (pending) "
                    f"count={len(out)} elapsed_s={(time.perf_counter()-t0):.6f}"
                )
            return out

        # Snapshot path (no pending finalized closures)
        ts = int(event_ts) if event_ts is not None else self.preferred_snapshot_ts()
        if ts is None:
            return []

        closed_candles: Dict[str, Candle] = {
            tf: candle for tf, candle in self.latest_closed_candles.items()
            if candle.timestamp <= ts
        }
        open_candles = self._current_open_candles()

        if not closed_candles and not open_candles:
            return []

        cc = CandleClosure(
            timestamp=ts,
            closed_candles=closed_candles,
            open_candles=open_candles,
            last_closed=None,
            is_final=False,
            source=self.closure_source
        )

        if self.debug_timing:
            dt_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"{self._timing_prefix} {dt_str} make_closure (snapshot) "
                f"ts={ts} elapsed_s={(time.perf_counter()-t0):.6f}"
            )
        return [cc]

    # ---------- Flush (finite-range only) ----------
    def flush(self) -> List[CandleClosure]:
        """
        Emit any pending finalized closures and one last partial snapshot.

        IMPORTANT: By default we do **not** force-close the base TF on wall-clock
        time. When `force_close_final=True` was passed to the manager, this
        method DOES force-close the base TF's currently-open candle as if a
        subsequent bar had arrived. This is opt-in for end-of-data scans where
        the operator knows the latest bar is genuinely complete (e.g. cron
        running 30 min after US market close).
        """
        out: List[CandleClosure] = []

        # Force-close the BASE TF's open candle when explicitly enabled.
        # This must run BEFORE the higher-TF block below so that any propagated
        # sub_factor completions are handled by the normal record_closure path.
        # Higher TFs are intentionally NOT force-closed here — they follow the
        # standard sub_factor / boundary-jump rules.
        if self.force_close_final and self.base_agg.open_ts is not None:
            # event_ts: "when did we detect the close". Prefer last_sub_ts
            # (the last real input that contributed to this bar), then
            # current_boundary, then open_ts as a last-resort identity value.
            # We avoid 0 because it would inject a meaningless epoch timestamp
            # into downstream consumers that key on closure event time.
            event_ts = (
                self.base_agg.last_sub_ts
                if self.base_agg.last_sub_ts is not None
                else self.base_agg.current_boundary
                if self.base_agg.current_boundary is not None
                else self.base_agg.open_ts
            )
            # _finalize_aggregator_candle is idempotent (early-returns if
            # open_ts is None) and uses self.open_ts as label_ts inside
            # record_closure. record_closure propagates to higher TFs via
            # on_base_candle_closed, so a force-closed base bar that completes
            # a higher TF's sub_factor will correctly cascade.
            self.base_agg._finalize_aggregator_candle(real_event_ts=event_ts)
            # Clear the base aggregator's open state. We deliberately do NOT
            # call _seed_next_open — there is no next bar coming.
            self.base_agg._clear_open_state()
            # ALSO clear any pre-existing seeded next-open. During normal bar
            # ingestion, _consume_subcandle calls _seed_next_open after every
            # bar transition (line ~670), so a "next bar opens here" hint is
            # set on the base aggregator after every real bar. Without this
            # cleanup, get_current_open_candle would still return a seeded
            # preview candle, putting the base TF back into open_candles
            # snapshots — defeating the contract that the force-closed bar
            # has been MOVED to closed_candles.
            self.base_agg._seed_next_open_ts = None
            self.base_agg._seed_next_open_price = None

        # If higher TFs already have enough subs, allow them to finalize (logical completion)
        for agg in self.higher_aggs:
            # Skip aggregators with no data (avoids nonsensical epoch-0 timestamps)
            if agg.last_sub_ts is None and agg.current_boundary is None:
                continue
            if agg.open_ts is not None and agg.sub_count >= agg.sub_factor:
                if self.verbose:
                    print(f"{Fore.MAGENTA}[DEBUG]{Style.RESET_ALL} forcibly finalizing aggregator {agg.tf}")
                closing_price = agg.close_px if agg.close_px is not None else agg.open_px
                agg._finalize_aggregator_candle(real_event_ts=agg.last_sub_ts if agg.last_sub_ts is not None else (agg.current_boundary or 0))
                if closing_price is not None and agg.current_boundary is not None:
                    agg._seed_next_open(agg.current_boundary + agg.tf_ms, closing_price)
                agg.current_boundary = (agg.current_boundary or 0) + agg.tf_ms
                agg._clear_open_state()

        # Emit any newly finalized closures first
        if self._pending:
            closures = self._make_closure(event_ts=None, include_pending=True)
            out.extend(closures)

        # Final partial snapshot (once)
        if not self._final_partial_emitted:
            partial_open_candles: Dict[str, Candle] = self._current_open_candles()
            latest_open_ts: Optional[int] = None
            for oc in partial_open_candles.values():
                if latest_open_ts is None or oc.timestamp > latest_open_ts:
                    latest_open_ts = oc.timestamp
            if partial_open_candles and latest_open_ts is not None:
                final_closed_candles: Dict[str, Candle] = dict(self.latest_closed_candles)
                cc = CandleClosure(
                    timestamp=latest_open_ts,
                    closed_candles=final_closed_candles,
                    open_candles=partial_open_candles,
                    last_closed=None,
                    is_final=True,
                    source=self.closure_source
                )
                out.append(cc)
            self._final_partial_emitted = True

        return out



def parse_timestamp(date_str, is_start=True):
    if not date_str:
        return None
    try:
        if len(date_str) == 10:
            if is_start:
                dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            else:
                # End-of-day: next day midnight minus 1ms to cover 23:59:59.999
                dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                dt = dt + timedelta(days=1) - timedelta(milliseconds=1)
            return int(dt.timestamp() * 1000)
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}")


class Config:
    """
    Configuration parameters for iteration.
    """
    def __init__(
        self, *,
        exchange,
        ticker,
        base_timeframe,
        start_ts,
        end_ts,
        data_dir,
        verbose=False,
        poll_interval_seconds: Optional[int] = None
    ):
        self.exchange = exchange.upper()
        self.ticker = ticker
        self.base_timeframe = base_timeframe
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.data_dir = data_dir
        self.verbose = verbose
        self.poll_interval_seconds = poll_interval_seconds


# ----------------------------------------------------------------------
# CandleIterator
# ----------------------------------------------------------------------
class CandleIterator:
    """
    Iterates over base timeframe candles, returning CandleClosure objects.

    Polling semantics:
    - Polling is **disabled by default** (i.e., when `poll_interval_seconds` is None).
      In that case, the iterator behaves as a finite stream: once historical files
      are exhausted, it flushes pending closures/partials and ends.
    - If polling is enabled (via env `CANDLES_SYNC_POLL_INTERVAL_SECS` or a positive
      `poll_interval_seconds`), on transition to polling mode we perform the **first
      poll immediately** (no initial sleep) and only then begin waiting per poll interval.

    Live snapshot ingestion:
    - We re-ingest rows whose timestamp equals the last-seen timestamp (ts == last_ts)
      to capture evolving snapshots for the current candle. We still skip strictly
      older rows (ts < last_ts).

    Low-latency emission:
    - After each poll sync we **break as soon as a real closure is enqueued**,
      i.e., only when the triggering row's timestamp strictly advances beyond
      the starting `last_ts`. Same-ts *snapshots* do not cause an early break.

    Tail-follow polling (this implementation):
    - In steady state only the current partition file changes. We cache its path and tail
      only new bytes, always re-parsing the last ~4KB to catch in-place last-line rewrites.
      This eliminates directory re-listing and full-file re-reading in steady state.
    """

    def __init__(self, cfg: Config, manager: AggregatorManager, *,
                 on_before_initial_resync: Optional[Callable] = None):
        self.cfg = cfg
        self.manager = manager
        self._on_before_initial_resync = on_before_initial_resync
        self.data_path = os.path.expanduser(
            f"{cfg.data_dir}/{cfg.exchange}/candles/{cfg.ticker}/{cfg.base_timeframe}"
        )
        if not os.path.isdir(self.data_path):
            raise ValueError(f"Data directory not found: {self.data_path}")

        self.csv_file_paths = self._list_csv_files()
        if self.cfg.verbose:
            print(f"{INFO} Found {len(self.csv_file_paths)} data files for base timeframe: {self.cfg.base_timeframe}")

        self._candles_gen = self._load_candles_from_disk()
        self._closed_buffer: deque[CandleClosure] = deque()
        self._last_ts: Optional[int] = None
        self._last_close: Optional[float] = None

        self.base_ms = TIMEFRAMES[self.cfg.base_timeframe]

        # Polling-interval and announcement toggle
        self.poll_interval_seconds: Optional[int] = self._resolve_poll_interval_seconds()
        self.poll_interval_ms: Optional[int] = (
            max(MIN_POLL_INTERVAL_SECONDS, int(self.poll_interval_seconds)) * 1000
            if self.poll_interval_seconds is not None else
            None
        )
        self._polling_notice_emitted: bool = False

        # Used only for finite end ranges (including "polling disabled" mode)
        self._final_flush_done: bool = False

        # Hint to immediately re-drain (no sleep, no resync) if we advanced
        self._redrain_after_first_emission: bool = False

        # Catch-up phase: after initial resync, closures stay HISTORICAL until
        # the buffer drains.  The LIVE flip happens on the next __next__() call
        # after the catch-up buffer is empty.
        self._catchup_drain_pending: bool = False
        self._poll_print_suppressed: bool = False

        # Timing diagnostics
        self.debug_timing: bool = _truthy_env(os.environ.get("CANDLES_SYNC_DEBUG_TIMING")) or bool(self.cfg.verbose)
        self._timing_prefix = Fore.CYAN + "[TIMING]" + Style.RESET_ALL

        # ---- Tail-follow cache (steady-state) ----
        self._tail_file_path: Optional[str] = None
        self._tail_inode: Optional[int] = None
        self._tail_dev: Optional[int] = None
        self._tail_size: int = 0
        self._tail_offset: int = 0  # byte offset where the next read will start from
        self._tail_backtrack_bytes: int = TAIL_BACKTRACK_BYTES
        self._tail_last_line_len: int = 0  # length of last parsed line, for dynamic backtrack

    def __iter__(self) -> Iterator[CandleClosure]:
        return self

    def __next__(self) -> CandleClosure:
        # Return buffered closures first (lowest latency)
        if self._closed_buffer:
            return self._closed_buffer.popleft()

        # Catch-up buffer fully drained — transition to LIVE for all subsequent closures
        if self._catchup_drain_pending:
            self._catchup_drain_pending = False
            self.manager.set_closure_source(CandleClosureSource.LIVE)

        while True:
            try:
                row_ts, o, h, l, c, v = next(self._candles_gen)
            except StopIteration:
                # If an explicit end_ts was set OR polling is disabled, we truly end.
                if self.cfg.end_ts is not None or self.poll_interval_seconds is None:
                    if not self._final_flush_done:
                        all_closures = self.manager.flush()
                        self._closed_buffer.extend(all_closures)
                        self._final_flush_done = True
                        if self._closed_buffer:
                            return self._closed_buffer.popleft()
                    raise StopIteration

                # Live mode (no end_ts) with polling enabled: switch to polling
                if not self._polling_notice_emitted:
                    self._polling_notice_emitted = True
                    # Fire the one-shot callback so the consumer can flush buffered
                    # output before synchronize_candle_data() prints to stdout.
                    if self._on_before_initial_resync is not None:
                        self._on_before_initial_resync()
                    # Suppress [candles-sync] output during catch-up resync —
                    # consumer will print SCMR + announcement first.
                    self._suppress_poll_print()
                    # DON'T announce or flip to LIVE yet — catch-up closures stay HISTORICAL.
                    # Consumer is responsible for printing the transition announcement.
                    # FIRST POLL: drain ALL resynced rows (no early break) so the entire
                    # catch-up backlog is tagged HISTORICAL.  Using stop_after_first_emission=False
                    # avoids the redrain path which would run after the LIVE flip.
                    self._poll_for_new_data_and_buffer(
                        sleep_first=False, resync=True, stop_after_first_emission=False
                    )
                    # Mark catch-up drain pending — LIVE flip happens when buffer empties
                    self._catchup_drain_pending = True
                else:
                    # If prior cycle produced a strictly-new row, re-drain immediately (no sleep/resync)
                    if self._redrain_after_first_emission:
                        self._poll_for_new_data_and_buffer(
                            sleep_first=False, resync=False, stop_after_first_emission=True
                        )
                    else:
                        # Normal cadence
                        self._poll_for_new_data_and_buffer(
                            sleep_first=True, resync=True, stop_after_first_emission=True
                        )

                # If the poll produced closures (from fresh rows), emit the first now.
                if self._closed_buffer:
                    return self._closed_buffer.popleft()

                # Otherwise, emit a single heartbeat snapshot immediately
                snap_ts = self.manager.preferred_snapshot_ts()
                snapshot = self.manager.build_snapshot_closure(event_ts=snap_ts)
                if snapshot is not None:
                    self._closed_buffer.append(snapshot)
                    return self._closed_buffer.popleft()

                # Nothing at all to emit; loop to next poll
                continue

            # Normal ingestion from file: process row and emit as soon as we have a closure
            self._ingest_row(row_ts, o, h, l, c, v)
            if self._closed_buffer:
                return self._closed_buffer.popleft()

    # -----------------------------
    # Ingestion helpers
    # -----------------------------
    def _ingest_row(self, row_ts: int, o: float, h: float, l: float, c: float, v: float) -> None:
        """Common ingestion for a single base row (used for file + poll drains)."""

        # Initialize last seen timestamp (we still ingest this row)
        if self._last_ts is None:
            self._last_ts = row_ts
            self._last_close = o

        # Fill any full-minute gaps before this row
        while self._last_ts is not None and row_ts > self._last_ts + self.base_ms:
            missing_ts = self._last_ts + self.base_ms
            if self.cfg.end_ts and missing_ts > self.cfg.end_ts:
                pending = self.manager.flush()
                if pending:
                    self._closed_buffer.extend(pending)
                return
            if self.cfg.verbose:
                dt_str = datetime.fromtimestamp(missing_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                print(f"{Fore.RED}[MISSING]{Style.RESET_ALL} Candle missing at {missing_ts} ({dt_str}) => generating fake candle (0 volume)")
            dummy = self._last_close if self._last_close is not None else o
            new_closures = self.manager.on_subcandle(missing_ts, dummy, dummy, dummy, dummy, 0.0)
            self._closed_buffer.extend(new_closures)
            self._last_ts = missing_ts
            self._last_close = dummy

        # Enforce end date bound (mostly relevant if end_ts was given)
        if self.cfg.end_ts and row_ts > self.cfg.end_ts:
            pending = self.manager.flush()
            if pending:
                self._closed_buffer.extend(pending)
            return

        # Skip strictly older timestamps; allow equal timestamps as snapshot updates.
        if self._last_ts is not None and row_ts < self._last_ts:
            if self.cfg.verbose:
                print(f"{WARNING} Skipping out-of-order/older timestamp: {row_ts}")
            return

        # Normal or snapshot-update ingestion:
        closures = self.manager.on_subcandle(row_ts, o, h, l, c, v)
        self._closed_buffer.extend(closures)
        self._last_ts = row_ts
        self._last_close = c

    # -----------------------------
    # Polling helpers (TAIL-FOLLOW)
    # -----------------------------
    def _resolve_poll_interval_seconds(self) -> Optional[int]:
        """
        Determine the polling interval to use when entering polling mode.
        Returns:
            Optional[int]: the interval in seconds, or None to **disable polling**.
        Priority (first valid wins):
            1) ENV: CANDLES_SYNC_POLL_INTERVAL_SECS (>= MIN_POLL_INTERVAL_SECONDS)
            2) cfg.poll_interval_seconds (>= MIN_POLL_INTERVAL_SECONDS)
            3) default: None  => polling disabled by default
        """
        env_val = os.environ.get(ENV_POLL_INTERVAL_KEY)
        if env_val:
            try:
                val = int(float(env_val))
                if val >= MIN_POLL_INTERVAL_SECONDS:
                    return val
            except (ValueError, TypeError):
                pass  # fall back

        if self.cfg.poll_interval_seconds is not None:
            try:
                val = int(self.cfg.poll_interval_seconds)
                if val >= MIN_POLL_INTERVAL_SECONDS:
                    return val
            except (ValueError, TypeError):
                pass  # fall back

        # Default: polling disabled unless explicitly enabled
        return None

    def restore_sync_logging(self) -> None:
        """Public: consumer calls this after printing its transition announcement."""
        self._restore_poll_print()

    def _suppress_poll_print(self) -> None:
        """Suppress candles_sync.poll_print during the catch-up transition window."""
        try:
            import candles_sync.candles_sync as _cs_mod
            self._orig_poll_print = getattr(_cs_mod, 'poll_print', None)
            _cs_mod.poll_print = lambda msg: None
            self._poll_print_suppressed = True
        except Exception:
            pass

    def _restore_poll_print(self) -> None:
        """Restore candles_sync.poll_print after catch-up transition."""
        if not self._poll_print_suppressed:
            return
        try:
            import candles_sync.candles_sync as _cs_mod
            if self._orig_poll_print is not None:
                _cs_mod.poll_print = self._orig_poll_print
            self._poll_print_suppressed = False
        except Exception:
            pass

    def _poll_for_new_data_and_buffer(
        self,
        *,
        sleep_first: bool = True,
        resync: bool = True,
        stop_after_first_emission: bool = False
    ) -> None:
        """
        Tail-follow + short-circuit polling:

        - Optionally sleep.
        - Optionally resync from remote (writes fresh rows to the current CSV).
        - Tail only the current partition file:
            * Seek to max(0, last_offset - BACKTRACK)
            * Read to EOF
            * If start > 0, drop the first partial line (align on newline)
            * Parse only those lines; inclusive semantics preserved by BACKTRACK
        - Break as soon as a *real* closure is enqueued from a row strictly advancing last_ts.
        """
        import time as _time
        t_cycle_start = _time.perf_counter()

        # Diagnostics: inputs & starting state
        initial_last_ts = self._last_ts if self._last_ts is not None else -1
        if self.debug_timing:
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(f"{self._timing_prefix} {now_str} poll_start "
                  f"(sleep_first={sleep_first}, resync={resync}, initial_last_ts={initial_last_ts})")

        # Default: assume we won't need a no-sleep re-drain unless proven otherwise
        self._redrain_after_first_emission = False

        # 1) Sleep (if requested)
        sleep_s = 0.0
        if sleep_first:
            t0_sleep = _time.perf_counter()
            _time.sleep(self.poll_interval_seconds)  # type: ignore[arg-type]
            sleep_s = _time.perf_counter() - t0_sleep

        # 2) Sync (optional)
        sync_s = 0.0
        t_sync_end_perf = None
        if resync:
            t0_sync = _time.perf_counter()
            # Ensure fast-append writer is installed before any synchronize call
            _install_fast_append_writer()
            ok = synchronize_candle_data(
                exchange=self.cfg.exchange,
                ticker=self.cfg.ticker,
                timeframe=self.cfg.base_timeframe,
                end_date_str=None,  # always pull up to latest
                polling=1,
                verbose=self.cfg.verbose  # honor CLI -v
            )
            sync_s = _time.perf_counter() - t0_sync
            if not ok and self.cfg.verbose:
                print(f"{WARNING} Poll sync failed; will retry on next interval.{Style.RESET_ALL}")
            t_sync_end_perf = _time.perf_counter()

        # 3) Tail the current partition file
        #    (no directory re-listing in steady state)
        tail_seek_s = 0.0
        tail_read_s = 0.0
        tail_decode_s = 0.0
        tail_parse_s = 0.0
        bytes_read = 0
        drained_any = False
        rows_scanned = 0
        first_row_ts: Optional[int] = None
        emitted_row_ts: Optional[int] = None
        closures_before = len(self._closed_buffer)
        ingest_time_s = 0.0

        t0_tail_total = _time.perf_counter()
        path = self._ensure_tail_file_current()
        if path is not None and os.path.exists(path):
            try:
                # Note: there is a benign TOCTOU race between os.stat() and open()/read() below —
                # the file could rotate between these calls. This is handled by inode-based rotation
                # detection on the next poll cycle (_ensure_tail_file_current checks inode/dev).
                st = os.stat(path)
                # Dynamic backtrack: use smaller window in steady state based on last line length,
                # falling back to full TAIL_BACKTRACK_BYTES on first poll or when no data seen yet.
                if self._tail_last_line_len > 0:
                    backtrack = min(self._tail_backtrack_bytes, self._tail_last_line_len * 2 + 256)
                else:
                    backtrack = self._tail_backtrack_bytes
                start_offset = max(0, self._tail_offset - backtrack)
                end_size = st.st_size

                # Read bytes
                t0_seek = _time.perf_counter()
                with open(path, "rb") as f:
                    f.seek(start_offset, os.SEEK_SET)
                    tail_seek_s = _time.perf_counter() - t0_seek

                    t0_read = _time.perf_counter()
                    data = f.read()
                    tail_read_s = _time.perf_counter() - t0_read
                    bytes_read = len(data)

                # Decode + align to newline after backtrack
                t0_decode = _time.perf_counter()
                text = data.decode("utf-8", errors="ignore")
                tail_decode_s = _time.perf_counter() - t0_decode

                if start_offset > 0:
                    # Drop the first partial line to align on a full record boundary.
                    # Assumption: OHLCV CSV lines are ~60 bytes, well within TAIL_BACKTRACK_BYTES (4096),
                    # so a complete line will always be found within the backtrack window.
                    nl = text.find("\n")
                    if nl >= 0:
                        text = text[nl + 1:]
                    else:
                        text = ""  # all partial; wait for next poll

                # Parse lines
                t0_parse = _time.perf_counter()
                if text:
                    lines = text.splitlines()
                    for line in lines:
                        if not line:
                            continue
                        # Skip header if encountered (e.g., on rotation)
                        # Fast pre-check on first char avoids lowercasing entire line
                        if (line[0] == 't' or line[0] == 'T') and line[:9].lower() == "timestamp":
                            continue
                        parts = line.split(",")
                        if len(parts) < 6:
                            continue
                        try:
                            ts = int(parts[0])
                            o = float(parts[1])
                            c = float(parts[2])
                            h = float(parts[3])
                            l = float(parts[4])
                            v = float(parts[5])
                        except (ValueError, IndexError):
                            if self.cfg.verbose:
                                print(f"{WARNING} Skipping malformed CSV row in tail: {line[:80]}{Style.RESET_ALL}")
                            continue

                        drained_any = True
                        rows_scanned += 1
                        self._tail_last_line_len = len(line)
                        if first_row_ts is None:
                            first_row_ts = ts

                        t0_ingest = _time.perf_counter()
                        before = len(self._closed_buffer)
                        self._ingest_row(ts, o, h, l, c, v)
                        t1_ingest = _time.perf_counter()
                        ingest_time_s += (t1_ingest - t0_ingest)

                        # Early break only when a real closure is enqueued by a strictly-new ts
                        if stop_after_first_emission and len(self._closed_buffer) > before and ts > initial_last_ts:
                            emitted_row_ts = ts
                            if t_sync_end_perf is not None:
                                # keep for timing summary; will compute later
                                pass
                            self._redrain_after_first_emission = True
                            break
                tail_parse_s = _time.perf_counter() - t0_parse

                # Advance offset to EOF (we consumed up to current size)
                self._tail_offset = end_size
                self._tail_size = end_size

            except Exception as exc:
                # If anything goes wrong tailing, do not crash the iterator; next poll will retry
                print(f"{WARNING} tail-follow error: {exc}{Style.RESET_ALL}")

        tail_total_s = _time.perf_counter() - t0_tail_total
        closures_added = len(self._closed_buffer) - closures_before
        first_emit_after_sync_s = -1.0
        if t_sync_end_perf is not None and emitted_row_ts is not None:
            # We measured ingest end earlier; best proxy is tail_total end relative to sync end
            first_emit_after_sync_s = max(0.0, (_time.perf_counter() - t_sync_end_perf))

        # 4) Timing summary
        if self.debug_timing:
            advanced = (emitted_row_ts is not None and emitted_row_ts > initial_last_ts)
            now_str_end = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"{self._timing_prefix} {now_str_end} poll_summary "
                f"sleep_s={sleep_s:.3f} sync_s={sync_s:.3f} "
                f"tail_total_s={tail_total_s:.3f} seek_s={tail_seek_s:.3f} read_s={tail_read_s:.3f} "
                f"decode_s={tail_decode_s:.3f} parse_s={tail_parse_s:.3f} ingest_s={ingest_time_s:.3f} "
                f"bytes_read={bytes_read} rows_scanned={rows_scanned} first_row_ts={first_row_ts} emitted_row_ts={emitted_row_ts} "
                f"first_emit_after_sync_s={first_emit_after_sync_s:.3f} advanced={advanced} closures_added={closures_added} "
                f"redrain={self._redrain_after_first_emission}"
            )

        if self.cfg.verbose and resync and not drained_any:
            print(f"{INFO} Poll returned no new rows; emitting snapshot.")

    # -----------------------------
    # File/CSV helpers (historical listing only)
    # -----------------------------
    def _file_day_range(self, filename: str) -> Tuple[int, int]:
        """
        Determine the [start, end] millis range a CSV file covers based on filename.
        Mirrors your original logic.
        """
        base_tf = self.cfg.base_timeframe
        without_ext = filename.replace(".csv", "")

        if base_tf == "1m":
            y, m, d = without_ext.split("-")
            dt_start = datetime(int(y), int(m), int(d), 0, 0, tzinfo=timezone.utc)
            dt_end = dt_start + timedelta(days=1) - timedelta(milliseconds=1)
            return int(dt_start.timestamp() * 1000), int(dt_end.timestamp() * 1000)

        elif base_tf == "1h":
            y, m = without_ext.split("-")
            dt_start = datetime(int(y), int(m), 1, 0, 0, tzinfo=timezone.utc)
            # Jump to next month
            if int(m) == 12:
                dt_next = datetime(int(y) + 1, 1, 1, 0, 0, tzinfo=timezone.utc)
            else:
                dt_next = datetime(int(y), int(m) + 1, 1, 0, 0, tzinfo=timezone.utc)
            dt_end = dt_next - timedelta(milliseconds=1)
            return int(dt_start.timestamp() * 1000), int(dt_end.timestamp() * 1000)

        elif base_tf == "1D":
            y = without_ext
            dt_start = datetime(int(y), 1, 1, 0, 0, tzinfo=timezone.utc)
            dt_next = datetime(int(y) + 1, 1, 1, 0, 0, tzinfo=timezone.utc)
            dt_end = dt_next - timedelta(milliseconds=1)
            return int(dt_start.timestamp() * 1000), int(dt_end.timestamp() * 1000)

        else:
            # Default to day-based filenames
            y, m, d = without_ext.split("-")
            dt_start = datetime(int(y), int(m), int(d), 0, 0, tzinfo=timezone.utc)
            dt_end = dt_start + timedelta(days=1) - timedelta(milliseconds=1)
            return int(dt_start.timestamp() * 1000), int(dt_end.timestamp() * 1000)

    def _list_csv_files(self) -> List[str]:
        """
        List initial CSV files intersecting the requested [start_ts, end_ts] window.
        (Used only for the historical pass; steady-state polling no longer re-lists.)
        """
        base_tf = self.cfg.base_timeframe
        pattern_map = {
            "1m":  r"^\d{4}-\d{2}-\d{2}\.csv$",
            "1h":  r"^\d{4}-\d{2}\.csv$",
            "1D":  r"^\d{4}\.csv$",
        }
        regex_pat = pattern_map.get(base_tf, r"^\d{4}-\d{2}-\d{2}\.csv$")
        regex_obj = re.compile(regex_pat)

        raw_files = [f for f in os.listdir(self.data_path) if regex_obj.match(f)]

        def sort_key(filename: str):
            without_ext = filename.replace(".csv", "")
            parts = without_ext.split("-")
            if base_tf == "1m":
                y, m, d = parts
                return (int(y), int(m), int(d))
            elif base_tf == "1h":
                y, m = parts
                return (int(y), int(m))
            elif base_tf == "1D":
                return (int(without_ext), 0, 0)
            else:
                return without_ext

        raw_files.sort(key=sort_key)

        selected_files: List[str] = []
        for f in raw_files:
            fullpath = os.path.join(self.data_path, f)
            f_start, f_end = self._file_day_range(f)
            if self.cfg.start_ts and f_end < self.cfg.start_ts:
                continue
            if self.cfg.end_ts and f_start > self.cfg.end_ts:
                break
            selected_files.append(fullpath)

        return selected_files

    def _current_partition_basename(self, dt: datetime) -> str:
        """Return the expected partition filename for the current base timeframe at given UTC dt."""
        tf = self.cfg.base_timeframe
        if tf == "1m":
            return dt.strftime("%Y-%m-%d") + ".csv"
        if tf == "1h":
            return dt.strftime("%Y-%m") + ".csv"
        if tf == "1D":
            return dt.strftime("%Y") + ".csv"
        # Default to daily naming
        return dt.strftime("%Y-%m-%d") + ".csv"

    def _ensure_tail_file_current(self) -> Optional[str]:
        """
        Ensure the tail cache points at the current partition file.
        Rotation rules:
          - 1m: daily file (YYYY-MM-DD.csv)
          - 1h: monthly file (YYYY-MM.csv)
          - 1D: yearly file (YYYY.csv)
        Returns the path if it exists, else None.
        """
        now_dt = datetime.now(timezone.utc)
        candidate = os.path.join(self.data_path, self._current_partition_basename(now_dt))

        # If the candidate doesn't exist yet (very early after rollover), try the most recent existing file once.
        if not os.path.exists(candidate):
            # Fallback: choose the last file from the initial listing (safe, rare)
            try:
                files = self._list_csv_files()
                if files:
                    candidate = files[-1]
            except Exception as exc:
                if self.cfg.verbose:
                    print(f"{WARNING} _ensure_tail_file_current fallback failed: {exc}{Style.RESET_ALL}")
                return None

        if not os.path.exists(candidate):
            return None

        st = os.stat(candidate)
        inode = getattr(st, "st_ino", None)
        dev = getattr(st, "st_dev", None)
        size = st.st_size

        # First initialization of tailing
        if self._tail_file_path is None:
            self._tail_file_path = candidate
            self._tail_inode = inode
            self._tail_dev = dev
            self._tail_size = size
            # Start at EOF but we'll backtrack a small window to re-parse last line
            self._tail_offset = size
            return candidate

        # Rotation or replacement (path changed or (inode/dev) changed)
        path_changed = (os.path.abspath(candidate) != os.path.abspath(self._tail_file_path))
        sig_changed = (inode != self._tail_inode) or (dev != self._tail_dev)
        if path_changed or sig_changed:
            self._tail_file_path = candidate
            self._tail_inode = inode
            self._tail_dev = dev
            self._tail_size = size
            # On rotation, start from 0 to capture any rows in the new file (still cheap at day start)
            self._tail_offset = 0
            self._tail_last_line_len = 0  # reset dynamic backtrack on rotation
            return candidate

        # Same file; detect truncation
        if size < self._tail_offset:
            # File shrank (rewrite) → restart from 0
            self._tail_size = size
            self._tail_offset = 0
            self._tail_last_line_len = 0  # reset dynamic backtrack on truncation
            return candidate

        # Normal steady state
        self._tail_size = size
        return candidate

    def _load_candles_from_disk(self) -> Iterator[Tuple[int, float, float, float, float, float]]:
        """
        Load candles from the initial file list.
        Yields (ts, o, h, l, c, v).
        """
        reached_end = False
        for csv_path in self.csv_file_paths:
            if reached_end:
                break
            if self.cfg.verbose:
                print(f"{INFO} Reading file: {csv_path}")
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                for row in reader:
                    if reached_end:
                        break
                    if not row or len(row) < 6:
                        continue
                    if row[0].lower() == "timestamp":
                        continue
                    try:
                        ts = int(row[0])
                        o = float(row[1])
                        c = float(row[2])
                        h = float(row[3])
                        l = float(row[4])
                        v = float(row[5])
                    except ValueError:
                        continue

                    if self.cfg.start_ts and ts < self.cfg.start_ts:
                        continue
                    if self.cfg.end_ts and ts > self.cfg.end_ts:
                        reached_end = True
                        break

                    yield (ts, o, h, l, c, v)

    def _load_candles_since(self, start_inclusive_ts: int) -> Iterator[Tuple[int, float, float, float, float, float]]:
        """
        Historical helper (not used during steady-state tail polling).
        Yields (ts, o, h, l, c, v) for rows with ts >= start_inclusive_ts.
        """
        file_list = self._list_csv_files()
        for csv_path in file_list:
            if self.cfg.verbose:
                print(f"{INFO} (hist) Reading file: {csv_path}")
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row or len(row) < 6:
                        continue
                    if row[0].lower() == "timestamp":
                        continue
                    try:
                        ts = int(row[0])
                        o = float(row[1])
                        c = float(row[2])
                        h = float(row[3])
                        l = float(row[4])
                        v = float(row[5])
                    except ValueError:
                        continue

                    if ts < start_inclusive_ts:
                        continue
                    if self.cfg.end_ts and ts > self.cfg.end_ts:
                        return

                    yield (ts, o, h, l, c, v)



# ----------------------------------------------------------------------
# create_candle_iterator
# ----------------------------------------------------------------------
def create_candle_iterator(
    exchange: str,
    ticker: str,
    base_timeframe: str,
    aggregation_timeframes: List[str],
    start_date: str = None,
    end_date: str = None,
    data_dir: str = "~/.corky",
    verbose: bool = False,
    poll_interval_seconds: Optional[int] = None,
    on_before_initial_resync: Optional[Callable] = None,
    anchor_offsets: Optional[Dict[str, int]] = None,
    force_close_final: bool = False,
) -> Iterator[CandleClosure]:
    """
    Factory for CandleIterator. This version also injects a wrapper around
    `synchronize_candle_data` so we emit a TRACE line *exactly* when that
    function returns (in addition to your existing outer timing).

    Polling default:
    - Passing `poll_interval_seconds=None` (the default) **disables polling**.
      To enable live polling after the historical drain, pass a positive value
      or set ENV `CANDLES_SYNC_POLL_INTERVAL_SECS`.

    The wrapper is enabled when:
      - env CANDLES_SYNC_TRACE is truthy (1/true/yes/on), OR
      - verbose=True
    """

    # --- Validate paths and timeframes ---
    exchange = exchange.upper()

    if base_timeframe not in TIMEFRAMES:
        raise ValueError(f"Invalid base timeframe: {base_timeframe}")

    parsed_tfs = parse_aggregation_timeframes(aggregation_timeframes)
    if not parsed_tfs:
        raise ValueError(f"No valid aggregation timeframes found from {aggregation_timeframes}")

    base_ms = TIMEFRAMES[base_timeframe]
    for tf in parsed_tfs:
        if TIMEFRAMES[tf] < base_ms:
            raise ValueError(
                f"Aggregation timeframe '{tf}' is smaller than base timeframe '{base_timeframe}'"
            )

    # Ensure the base TF is included
    if base_timeframe not in parsed_tfs:
        parsed_tfs.insert(0, base_timeframe)

    start_ts = parse_timestamp(start_date, True)
    end_ts = parse_timestamp(end_date, False)

    # Resolve whether polling is enabled (env takes precedence, then parameter)
    resolved_poll_interval: Optional[int] = None
    env_val = os.environ.get(ENV_POLL_INTERVAL_KEY)
    if env_val:
        try:
            _val = int(float(env_val))
            if _val >= MIN_POLL_INTERVAL_SECONDS:
                resolved_poll_interval = _val
        except (ValueError, TypeError):
            resolved_poll_interval = None
    if resolved_poll_interval is None and poll_interval_seconds is not None:
        try:
            _val = int(poll_interval_seconds)
            if _val >= MIN_POLL_INTERVAL_SECONDS:
                resolved_poll_interval = _val
        except (ValueError, TypeError):
            resolved_poll_interval = None

    # If no explicit start, back off DEFAULT_BACKOFF_BARS of the largest TF and align.
    # IMPORTANT FIX:
    # - If polling will be enabled and the user did NOT explicitly provide an end date,
    #   we DO NOT set end_ts here. Leaving end_ts=None allows CandleIterator to enter
    #   polling after the historical drain (previously end_ts was set to "now" which
    #   made the iterator finite and prevented polling).
    if start_ts is None:
        largest_tf = max(parsed_tfs, key=lambda x: TIMEFRAMES[x])
        largest_tf_ms = TIMEFRAMES[largest_tf]

        # Use "now" only as an anchor to compute the starting window
        anchor_now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        effective_end_for_alignment = end_ts if end_ts is not None else anchor_now_ms

        raw_start_ts = effective_end_for_alignment - (DEFAULT_BACKOFF_BARS * largest_tf_ms)
        start_ts = (raw_start_ts // largest_tf_ms) * largest_tf_ms

        if verbose:
            print(
                f"{INFO} No start date supplied. Using highest timeframe '{largest_tf}' "
                f"({largest_tf_ms} ms) and {DEFAULT_BACKOFF_BARS} bars => raw start = {raw_start_ts}, aligned to {start_ts}."
            )

        # Only set an implicit end when polling is disabled OR user provided end_date.
        if end_ts is None and resolved_poll_interval is None:
            end_ts = anchor_now_ms
            if verbose:
                print(f"{INFO} No end date supplied and polling disabled. Using 'now' => {end_ts}.")
        elif end_ts is None and resolved_poll_interval is not None:
            # Polling enabled with no explicit end: leave end_ts unset so iterator will enter polling.
            if verbose:
                print(
                    f"{INFO} Polling enabled and no end date supplied — leaving end_ts unset to allow polling "
                    f"after historical drain."
                )

    # --- Build manager and config ---
    higher_tfs = [t for t in parsed_tfs if t != base_timeframe]
    manager = AggregatorManager(base_timeframe, higher_tfs, verbose=verbose,
                                 anchor_offsets=anchor_offsets,
                                 force_close_final=force_close_final)

    cfg = Config(
        exchange=exchange,
        ticker=ticker,
        base_timeframe=base_timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        data_dir=data_dir,
        verbose=verbose,
        poll_interval_seconds=poll_interval_seconds,
    )

    # Ensure the fast-append writer patch is installed before any sync activity
    _install_fast_append_writer()

    # --- Enable "trace-at-return" for synchronize_candle_data by rebinding the imported symbol ---
    # This guarantees you see a TRACE line at the *exact* moment the function returns,
    # regardless of what the inner candles-sync logs measure.
    should_wrap = _truthy_env(os.environ.get("CANDLES_SYNC_TRACE")) or verbose
    if should_wrap:
        # Rebind the *imported* symbol `synchronize_candle_data` in this module’s globals
        # so all subsequent calls (including inside CandleIterator polling) go through the wrapper.
        global synchronize_candle_data
        _orig_sync = synchronize_candle_data  # keep original

        # Avoid double-wrapping
        if not getattr(_orig_sync, "_return_traced", False):
            def _utc_now_ms() -> str:
                return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"

            def _sync_wrapper(*args, **kwargs):
                # Entry marker (optional but useful)
                print(f"{Fore.MAGENTA}[TRACE]{Style.RESET_ALL} {_utc_now_ms()} sync:call")
                t0 = time.perf_counter()
                ok = False
                try:
                    res = _orig_sync(*args, **kwargs)
                    ok = bool(res)
                    return res
                finally:
                    dt_ms = (time.perf_counter() - t0) * 1000.0
                    print(f"{Fore.MAGENTA}[TRACE]{Style.RESET_ALL} {_utc_now_ms()} sync:return ok={ok} duration_ms={dt_ms:.1f}")

            # Mark wrapper to prevent re-wrapping and rebind
            setattr(_sync_wrapper, "_return_traced", True)
            synchronize_candle_data = _sync_wrapper  # type: ignore[assignment]

    # --- Initial backfill sync (now wrapped if tracing enabled) ---
    end_date_str = None
    if end_ts:
        end_dt = datetime.fromtimestamp(end_ts / 1000, timezone.utc)
        end_date_str = end_dt.strftime("%Y-%m-%d %H:%M")

    ok = synchronize_candle_data(
        exchange=exchange,
        ticker=ticker,
        timeframe=base_timeframe,
        end_date_str=end_date_str,
        verbose=True
    )
    if not ok:
        print(f"\n{ERROR} Synchronization failed for {base_timeframe}.\n")
        sys.exit(1)

    return CandleIterator(cfg, manager, on_before_initial_resync=on_before_initial_resync)


if __name__ == "__main__":
    print(f"{ERROR} This module should not be run directly. Use example.py or your main script.")
    sys.exit(1)
