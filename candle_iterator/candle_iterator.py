#!/usr/bin/env python3

import argparse
import csv
import json
import os
import re
import sys
import numpy as np
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Iterator, Optional
from pathlib import Path
from pprint import pprint

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
UPDATE = Fore.MAGENTA + "[UPDATE]" + Style.RESET_ALL

COLOR_DIR = Fore.CYAN
COLOR_FILE = Fore.YELLOW
COLOR_TIMESTAMPS = Fore.MAGENTA
COLOR_ROWS = Fore.RED
COLOR_NEW = Fore.WHITE
COLOR_VAR = Fore.CYAN
COLOR_TYPE = Fore.YELLOW
COLOR_DESC = Fore.MAGENTA
COLOR_REQ = Fore.RED + "[REQUIRED]" + Style.RESET_ALL

# ----------------------------------------------------------------------
# 2) LOCAL IMPORT: SYNCHRONIZE FUNCTION
# ----------------------------------------------------------------------
from candles_sync import synchronize_candle_data

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

    pattern = r"^(<=|>=|<|>)([0-9]+[mhDWhd]+)$"
    match = re.match(pattern, expr)
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


@dataclass
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


@dataclass
class CandleClosure:
    """
    A closure event from aggregator_manager, containing:
      - closed_candles: the last fully closed candle(s) for each timeframe
      - open_candles: partial candle(s) still open
    """
    timestamp: int
    closed_candles: Dict[str, Candle] = field(default_factory=dict)
    open_candles: Dict[str, Candle] = field(default_factory=dict)
    last_closed: Optional['CandleClosure'] = None
    is_final: bool = False

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

    def print(self):
        dt_str = self.datetime.strftime("%Y-%m-%d %H:%M")
        print(f"{Fore.YELLOW}[Closure]{Style.RESET_ALL} T={self.timestamp} {dt_str} => "
              f"Timeframes in snapshot: {', '.join(self.timeframes)}")
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
class CandleAggregator:
    """
    Aggregator for one timeframe.
    - If is_base=True, we treat incoming subcandles as partial real-time data.
      We finalize them only if they are definitely behind us or the next row jumps forward.
    - If is_base=False, we accumulate a certain number of subcandles (sub_factor),
      or fill zero-volume for missing intervals, etc.
    """
    def __init__(self, timeframe: str, is_base=False, sub_factor=1):
        self.tf = timeframe
        self.tf_ms = TIMEFRAMES[timeframe]
        self.is_base = is_base
        self.sub_factor = sub_factor

        self.current_boundary: Optional[int] = None
        self.reset()

    def reset(self):
        self.open_ts: Optional[int] = None
        self.open_px = None
        self.high_px = None
        self.low_px = None
        self.close_px = None
        self.volume = 0.0
        self.sub_count = 0
        self.last_sub_ts = None

    def on_base_csv_row(self, ts, o, h, l, c, v):
        """
        Called only for the base aggregator. We accept each 1m row as partial real‐time data,
        finalizing if the row's ts jumps beyond the aggregator boundary.
        """
        if not self.is_base:
            raise RuntimeError("Called on_base_csv_row() on a non-base aggregator? (bug)")

        self._accumulate_base_subcandle(ts, o, h, l, c, v)

    def _accumulate_base_subcandle(self, base_ts, o, h, l, c, v):
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        if self.current_boundary is None:
            self.current_boundary = (base_ts // self.tf_ms) * self.tf_ms

        # Possibly finalize older candles if we've jumped one or more intervals
        while self.current_boundary + self.tf_ms <= base_ts:
            if self.sub_count > 0:
                self._finalize_aggregator_candle(real_event_ts=self.last_sub_ts or self.current_boundary)
            else:
                self._record_zero_candle(real_event_ts=self.current_boundary)
            self.current_boundary += self.tf_ms
            self.reset()

        # Initialize aggregator candle if empty
        if self.open_ts is None:
            self.open_ts = self.current_boundary
            self.open_px = o
            self.high_px = o
            self.low_px = o
            self.close_px = o
            self.volume = 0.0
            self.sub_count = 0

        # Update OHLC
        self.high_px = max(self.high_px, h)
        self.low_px = min(self.low_px, l)
        self.close_px = c
        self.volume += v
        self.sub_count += 1
        self.last_sub_ts = base_ts

        # If we are behind the boundary => finalize
        # (Case: If this row is clearly in the past relative to aggregator boundary.)
        fully_behind_now = (base_ts + self.tf_ms <= now_ms)
        if (base_ts > (self.current_boundary + self.tf_ms - 1)) or fully_behind_now:
            self._finalize_aggregator_candle(real_event_ts=base_ts)
            self.current_boundary += self.tf_ms
            self.reset()

    def on_base_candle_closed(self, base_ts, o, h, l, c, v):
        """
        For higher timeframes (is_base=False). We accumulate each 'closed' 1m candle as a subcandle.
        Once we have sub_factor subcandles, we finalize.
        Also handle multi-interval jumps (filling zero if needed).
        """
        if self.is_base:
            raise RuntimeError("Base aggregator called on_base_candle_closed? (bug)")

        # If our boundary is None, align to the base_ts
        if self.current_boundary is None:
            self.current_boundary = (base_ts // self.tf_ms) * self.tf_ms

        # If we jumped multiple intervals:
        while base_ts >= self.current_boundary + self.tf_ms:
            # finalize or record zero
            if self.sub_count > 0:
                # finalize what we had
                self._finalize_aggregator_candle(real_event_ts=self.last_sub_ts or self.current_boundary)
            else:
                # record zero
                self._record_zero_candle(real_event_ts=self.last_sub_ts or self.current_boundary)
            self.current_boundary += self.tf_ms
            self.reset()

        # Initialize aggregator candle if none open
        if self.open_ts is None:
            self.open_ts = self.current_boundary
            self.open_px = o
            self.high_px = o
            self.low_px = o
            self.close_px = o
            self.volume = 0.0
            self.sub_count = 0

        # Update OHLC data
        self.high_px = max(self.high_px, h)
        self.low_px = min(self.low_px, l)
        self.close_px = c
        self.volume += v
        self.sub_count += 1
        self.last_sub_ts = base_ts

        # If we've now reached the sub_factor (e.g. 5 subcandles => 5m), finalize immediately
        if self.sub_count >= self.sub_factor:
            self._finalize_aggregator_candle(real_event_ts=base_ts)
            self.current_boundary += self.tf_ms
            self.reset()

    def _record_zero_candle(self, real_event_ts):
        aggregator_manager.record_closure(
            aggregator_closure_ts=real_event_ts,
            tf=self.tf,
            label_ts=self.current_boundary,
            o=0.0, h=0.0, l=0.0, c=0.0,
            vol=0.0
        )

    def _finalize_aggregator_candle(self, real_event_ts):
        if self.open_ts is None:
            return
        if aggregator_manager and aggregator_manager.verbose:
            print(
                f"{Fore.MAGENTA}[DEBUG]{Style.RESET_ALL} aggregator {self.tf} FINALIZING => "
                f"boundary={self.current_boundary}, "
                f"o={self.open_px},h={self.high_px},l={self.low_px},c={self.close_px},vol={self.volume}"
            )
        aggregator_manager.record_closure(
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
        Returns the partial candle if still open; None if aggregator is not tracking any open candle.
        """
        if self.open_ts is None:
            return None
        return Candle(
            timeframe=self.tf,
            timestamp=self.open_ts,
            open=self.open_px,
            high=self.high_px,
            low=self.low_px,
            close=self.close_px,
            volume=self.volume
        )


# ----------------------------------------------------------------------
# AggregatorManager
# ----------------------------------------------------------------------
class AggregatorManager:
    """
    Manages a base aggregator plus any higher aggregators.
    """
    def __init__(self, base_tf: str, higher_tfs: List[str], verbose: bool = False):
        self.verbose = verbose
        self.base_tf = base_tf
        self.base_agg = CandleAggregator(base_tf, is_base=True)
        self.higher_aggs: List[CandleAggregator] = []

        base_ms = TIMEFRAMES[base_tf]
        for tf in higher_tfs:
            factor = TIMEFRAMES[tf] // base_ms
            agg = CandleAggregator(tf, is_base=False, sub_factor=factor)
            self.higher_aggs.append(agg)

        self.latest_closed_candles: Dict[str, Tuple[str,int,float,float,float,float,float]] = {}
        self.pending_closures: Dict[int, set] = {}
        self._all_timeframes = [self.base_tf] + higher_tfs

        # Ensure we only produce final partial closure once
        self._final_partial_emitted = False

    def record_closure(self, aggregator_closure_ts, tf, label_ts, o, h, l, c, vol):
        """
        Called by aggregator to record a newly finalized candle.
        If tf == base_tf, that candle is also fed to each higher aggregator.
        """
        if tf == self.base_tf:
            for agg in self.higher_aggs:
                agg.on_base_candle_closed(aggregator_closure_ts, o, h, l, c, vol)

        self.latest_closed_candles[tf] = (tf, label_ts, o, h, l, c, vol)
        if aggregator_closure_ts not in self.pending_closures:
            self.pending_closures[aggregator_closure_ts] = set()
        self.pending_closures[aggregator_closure_ts].add(tf)

    def on_subcandle(self, ts, o, h, l, c, v) -> List[CandleClosure]:
        """
        Called for each row of base timeframe data.
        """
        # Clear old closures from the prior subcandle
        self.pending_closures.clear()

        # Feed it into the base aggregator
        self.base_agg.on_base_csv_row(ts, o, h, l, c, v)

        # Build closures for newly finalized candles
        return self._build_and_return_closures()

    def _build_and_return_closures(self) -> List[CandleClosure]:
        out = []
        if not self.pending_closures:
            return out

        for closure_ts in sorted(self.pending_closures.keys()):
            closed_candles: Dict[str, Candle] = {}
            for tf in self._all_timeframes:
                if tf in self.latest_closed_candles:
                    (tfid, lbl_ts, oo, hh, ll, cc, vv) = self.latest_closed_candles[tf]
                    closed_candles[tf] = Candle(tfid, lbl_ts, oo, hh, ll, cc, vv)

            open_candles: Dict[str, Candle] = {}
            # Gather current open from each aggregator
            for agg in self.higher_aggs:
                oc = agg.get_current_open_candle()
                if oc:
                    open_candles[agg.tf] = oc

            base_open = self.base_agg.get_current_open_candle()
            if base_open:
                open_candles[self.base_tf] = base_open

            cc = CandleClosure(
                timestamp=closure_ts,
                closed_candles=closed_candles,
                open_candles=open_candles,
                last_closed=None,
                is_final=False
            )
            out.append(cc)

        return out

    def flush(self) -> List[CandleClosure]:
        # Finalize base if behind
        if self.base_agg.open_ts is not None:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            if self.base_agg.open_ts + self.base_agg.tf_ms <= now_ms:
                if self.verbose:
                    print(f"{Fore.MAGENTA}[DEBUG]{Style.RESET_ALL} forcibly finalizing base aggregator partial.")
                self.base_agg._finalize_aggregator_candle(real_event_ts=self.base_agg.last_sub_ts)
                self.base_agg.current_boundary += self.base_agg.tf_ms
                self.base_agg.reset()
        # Finalize higher if enough subs
        for agg in self.higher_aggs:
            if agg.open_ts is not None and agg.sub_count >= agg.sub_factor:
                if self.verbose:
                    print(f"{Fore.MAGENTA}[DEBUG]{Style.RESET_ALL} forcibly finalizing aggregator {agg.tf}")
                agg._finalize_aggregator_candle(real_event_ts=agg.last_sub_ts)
                agg.current_boundary += agg.tf_ms
                agg.reset()
        out = []
        # Emit any pending closures
        if self.pending_closures:
            closures = self._build_and_return_closures()
            self.pending_closures.clear()
            out.extend(closures)
        # FINAL PARTIAL CLOSURE: use the latest open‐candle timestamp
        if not self._final_partial_emitted:
            partial_open_candles = {}
            latest_open_ts = None
            all_aggs = [self.base_agg] + self.higher_aggs
            for aggregator in all_aggs:
                oc = aggregator.get_current_open_candle()
                if oc:
                    partial_open_candles[aggregator.tf] = oc
                    if latest_open_ts is None or oc.timestamp > latest_open_ts:
                        latest_open_ts = oc.timestamp
            if partial_open_candles and latest_open_ts is not None:
                # include last-known closed
                final_closed_candles = {}
                for tf in self._all_timeframes:
                    if tf in self.latest_closed_candles:
                        (tfid, lbl_ts, oo, hh, ll, cc, vv) = self.latest_closed_candles[tf]
                        final_closed_candles[tf] = Candle(tfid, lbl_ts, oo, hh, ll, cc, vv)
                cc = CandleClosure(
                    timestamp=latest_open_ts,
                    closed_candles=final_closed_candles,
                    open_candles=partial_open_candles,
                    last_closed=None,
                    is_final=True
                )
                out.append(cc)
                for aggregator in all_aggs:
                    aggregator.reset()
            self._final_partial_emitted = True
        return out

aggregator_manager = None


def parse_timestamp(date_str, is_start=True):
    if not date_str:
        return None
    try:
        if len(date_str) == 10:
            date_str += " 00:00" if is_start else " 23:59"
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}")


class Config:
    """
    Configuration parameters for iteration.
    """
    def __init__(self, *, exchange, ticker, base_timeframe, start_ts, end_ts, data_dir, verbose=False):
        self.exchange = exchange
        self.ticker = ticker
        self.base_timeframe = base_timeframe
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.data_dir = data_dir
        self.verbose = verbose


# ----------------------------------------------------------------------
# CandleIterator
# ----------------------------------------------------------------------
class CandleIterator:
    """
    Iterates over base timeframe candles, returning CandleClosure objects.
    We fill missing steps with zero-volume candles, then call `flush()` at the end
    to produce any final partial closure exactly once.
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.data_path = os.path.expanduser(
            f"{cfg.data_dir}/{cfg.exchange}/candles/{cfg.ticker}/{cfg.base_timeframe}"
        )
        if not os.path.isdir(self.data_path):
            raise ValueError(f"Data directory not found: {self.data_path}")

        self.csv_file_paths = self._list_csv_files()  # Possibly shortened via date skip
        if self.cfg.verbose:
            print(f"{INFO} Found {len(self.csv_file_paths)} data files for base timeframe: {self.cfg.base_timeframe}")

        self._candles_gen = self._load_candles_from_disk()
        self._closed_buffer: List[CandleClosure] = []
        self._current_row = None
        self._last_ts = None
        self._last_close = None

        self.base_ms = TIMEFRAMES[self.cfg.base_timeframe]

    def __iter__(self) -> Iterator[CandleClosure]:
        return self

    def __next__(self) -> CandleClosure:
        global aggregator_manager

        # If we have buffered closures, return from that buffer
        if self._closed_buffer:
            return self._closed_buffer.pop(0)

        while True:
            if self._current_row:
                row_ts, o, h, l, c, v = self._current_row
                self._current_row = None
            else:
                # Fetch next candle row from CSV
                try:
                    row_ts, o, h, l, c, v = next(self._candles_gen)
                except StopIteration:
                    # No more CSV rows => do final flush
                    flush_closures = []
                    if self._last_ts is not None:
                        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                        current_minute_boundary = (now_ms // self.base_ms) * self.base_ms

                        if current_minute_boundary > self._last_ts:
                            # Feed a zero-volume candle for the "current minute" if we moved into a new minute
                            aggregator_manager.on_subcandle(
                                current_minute_boundary,
                                self._last_close,
                                self._last_close,
                                self._last_close,
                                self._last_close,
                                0.0
                            )
                            flush_closures.extend(aggregator_manager._build_and_return_closures())

                    flush_closures_final = aggregator_manager.flush()
                    flush_closures.extend(flush_closures_final)

                    if flush_closures:
                        self._closed_buffer.extend(flush_closures)
                        return self._closed_buffer.pop(0)
                    else:
                        raise StopIteration

            # If first row, set _last_ts
            if self._last_ts is None:
                self._last_ts = row_ts
                self._last_close = o
                if self.cfg.end_ts and row_ts > self.cfg.end_ts:
                    raise StopIteration

            # If there's a gap > one base step => fill missing candle
            if row_ts > self._last_ts + self.base_ms:
                missing_ts = self._last_ts + self.base_ms
                if self.cfg.end_ts and missing_ts > self.cfg.end_ts:
                    raise StopIteration

                if self.cfg.verbose:
                    dt_str = datetime.fromtimestamp(missing_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                    print(f"{Fore.RED}[MISSING]{Style.RESET_ALL} Candle missing at {missing_ts} ({dt_str}) => generating fake candle (0 volume)")

                dummy_o = dummy_h = dummy_l = dummy_c = self._last_close
                dummy_v = 0.0

                new_closures = aggregator_manager.on_subcandle(missing_ts, dummy_o, dummy_h, dummy_l, dummy_c, dummy_v)
                self._closed_buffer.extend(new_closures)

                self._last_ts = missing_ts
                self._last_close = dummy_c
                self._current_row = (row_ts, o, h, l, c, v)

                if self._closed_buffer:
                    return self._closed_buffer.pop(0)
                else:
                    continue

            # If row_ts is beyond end_ts, flush aggregator and stop
            if self.cfg.end_ts and row_ts > self.cfg.end_ts:
                flush_closures = aggregator_manager.flush()
                if flush_closures:
                    self._closed_buffer.extend(flush_closures)
                    return self._closed_buffer.pop(0)
                else:
                    raise StopIteration

            if row_ts <= self._last_ts:
                # Skip duplicates or non-increasing
                if self.cfg.verbose:
                    print(f"{WARNING} Skipping non-increasing timestamp: {row_ts}")
                continue

            # Normal row => feed aggregator
            new_closures = aggregator_manager.on_subcandle(row_ts, o, h, l, c, v)
            self._closed_buffer.extend(new_closures)

            self._last_ts = row_ts
            self._last_close = c

            if self._closed_buffer:
                return self._closed_buffer.pop(0)

    # ----------------------------------------------------------------------
    # HELPER FUNCTION to parse day-based filenames into start/end timestamps
    # ----------------------------------------------------------------------
    def _file_day_range(self, filename: str) -> Tuple[int, int]:
        """
        For 1m data (YYYY-MM-DD.csv), returns (start_ts, end_ts) for that day.
        For 1h data (YYYY-MM.csv), returns (start_ts, end_ts) for that month.
        For 1D data (YYYY.csv), returns (start_ts, end_ts) for that year.
        """
        base_tf = self.cfg.base_timeframe
        without_ext = filename.replace(".csv", "")

        if base_tf == "1m":
            # filename like 2025-05-12.csv
            y, m, d = without_ext.split("-")
            dt_start = datetime(int(y), int(m), int(d), 0, 0, tzinfo=timezone.utc)
            dt_end = dt_start + timedelta(days=1) - timedelta(milliseconds=1)
            return int(dt_start.timestamp() * 1000), int(dt_end.timestamp() * 1000)

        elif base_tf == "1h":
            # filename like 2025-05.csv
            y, m = without_ext.split("-")
            dt_start = datetime(int(y), int(m), 1, 0, 0, tzinfo=timezone.utc)
            # Jump to next month:
            if int(m) == 12:
                dt_next = datetime(int(y) + 1, 1, 1, 0, 0, tzinfo=timezone.utc)
            else:
                dt_next = datetime(int(y), int(m) + 1, 1, 0, 0, tzinfo=timezone.utc)
            dt_end = dt_next - timedelta(milliseconds=1)
            return int(dt_start.timestamp() * 1000), int(dt_end.timestamp() * 1000)

        elif base_tf == "1D":
            # filename like 2025.csv
            y = without_ext
            dt_start = datetime(int(y), 1, 1, 0, 0, tzinfo=timezone.utc)
            dt_next = datetime(int(y) + 1, 1, 1, 0, 0, tzinfo=timezone.utc)
            dt_end = dt_next - timedelta(milliseconds=1)
            return int(dt_start.timestamp() * 1000), int(dt_end.timestamp() * 1000)

        else:
            # Default to day-based for safety
            y, m, d = without_ext.split("-")
            dt_start = datetime(int(y), int(m), int(d), 0, 0, tzinfo=timezone.utc)
            dt_end = dt_start + timedelta(days=1) - timedelta(milliseconds=1)
            return int(dt_start.timestamp() * 1000), int(dt_end.timestamp() * 1000)

    def _list_csv_files(self) -> List[str]:
        files = []
        base_tf = self.cfg.base_timeframe

        pattern_map = {
            "1m":  r"^\d{4}-\d{2}-\d{2}\.csv$",
            "1h":  r"^\d{4}-\d{2}\.csv$",
            "1D":  r"^\d{4}\.csv$",
        }
        regex_pat = pattern_map.get(base_tf, r"^\d{4}-\d{2}-\d{2}\.csv$")
        regex_obj = re.compile(regex_pat)

        raw_files = [f for f in os.listdir(self.data_path) if regex_obj.match(f)]

        def sort_key(filename):
            without_ext = filename.replace(".csv", "")
            parts = without_ext.split("-")
            if base_tf == "1m":
                # yyyy-mm-dd
                y, m, d = parts
                return (int(y), int(m), int(d))
            elif base_tf == "1h":
                # yyyy-mm
                y, m = parts
                return (int(y), int(m))
            elif base_tf == "1D":
                # yyyy
                return (int(without_ext), 0, 0)
            else:
                return without_ext

        raw_files.sort(key=sort_key)

        # Skip files outside [start_ts, end_ts]
        selected_files = []
        for f in raw_files:
            fullpath = os.path.join(self.data_path, f)
            f_start, f_end = self._file_day_range(f)

            # If entire file is before our start_ts, skip
            if self.cfg.start_ts and f_end < self.cfg.start_ts:
                continue

            # If entire file is beyond our end_ts, skip
            if self.cfg.end_ts and f_start > self.cfg.end_ts:
                break

            selected_files.append(fullpath)

        return selected_files

    def _load_candles_from_disk(self) -> Iterator[Tuple[int, float, float, float, float, float]]:
        """
        Reads CSV files in chronological order, yields (ts, open, high, low, close, volume).
        Skips lines outside [start_ts, end_ts].
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
                        h = float(row[2])
                        l = float(row[3])
                        c = float(row[4])
                        v = float(row[5])
                    except ValueError:
                        continue

                    if self.cfg.start_ts and ts < self.cfg.start_ts:
                        continue
                    if self.cfg.end_ts and ts > self.cfg.end_ts:
                        reached_end = True
                        break

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
    verbose: bool = False
) -> Iterator[CandleClosure]:
    data_path = os.path.expanduser(f"{data_dir}/{exchange}/candles/{ticker}/{base_timeframe}")
    if not os.path.exists(data_path):
        raise ValueError(f"No data directory found: {data_path}")

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

    # Ensure the base TF is in final list
    if base_timeframe not in parsed_tfs:
        parsed_tfs.insert(0, base_timeframe)

    start_ts = parse_timestamp(start_date, True)
    end_ts = parse_timestamp(end_date, False)

    # If no start date, pick enough base candles to have ~200 bars at the largest TF
    if start_ts is None:
        largest_tf = max(parsed_tfs, key=lambda x: TIMEFRAMES[x])
        factor = TIMEFRAMES[largest_tf] // TIMEFRAMES[base_timeframe]
        required_base_bars = 200 * factor

        if verbose:
            print(f"{INFO} No start date supplied. Using {required_base_bars} base-bars "
                  f"({base_timeframe}) to cover ~200 bars at highest TF: {largest_tf}")

        if end_ts is None:
            end_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
            if verbose:
                print(f"{INFO} No end date supplied. Using 'now' => {end_ts}.")

        start_ts = end_ts - (required_base_bars * base_ms)

    global aggregator_manager
    higher_tfs = [t for t in parsed_tfs if t != base_timeframe]
    aggregator_manager = AggregatorManager(base_timeframe, higher_tfs, verbose=verbose)

    cfg = Config(
        exchange=exchange,
        ticker=ticker,
        base_timeframe=base_timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        data_dir=data_dir,
        verbose=verbose
    )

    # Possibly sync data first
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

    return CandleIterator(cfg)


if __name__ == "__main__":
    print(f"{ERROR} This module should not be run directly. Use example.py or your main script.")
    sys.exit(1)

