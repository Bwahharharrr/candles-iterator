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
      - closed_candles: the last fully closed candle for each timeframe
      - open_candles: the partial candle for each higher timeframe still open
    """
    timestamp: int
    closed_candles: Dict[str, Candle] = field(default_factory=dict)
    open_candles: Dict[str, Candle] = field(default_factory=dict)
    last_closed: Optional['CandleClosure'] = None

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
        print(f"{Fore.YELLOW}[Closure]{Style.RESET_ALL} T={self.timestamp} {dt_str} "
              f"=> Timeframes in snapshot: {', '.join(self.timeframes)}")
        # Closed Candles title in green
        print(f"{Fore.GREEN}  Closed Candles:{Style.RESET_ALL}")
        for tf in sorted(self.closed_candles.keys(), key=lambda x: TIMEFRAMES[x]):
            print(f"    - {self.closed_candles[tf]}")

        if self.open_candles:
            # Open Candles title in red
            print(f"{Fore.RED}  Open Candles (partial):{Style.RESET_ALL}")
            for tf in sorted(self.open_candles.keys(), key=lambda x: TIMEFRAMES[x]):
                print(f"    - {self.open_candles[tf]}")


# ----------------------------------------------------------------------
# CandleAggregator
# ----------------------------------------------------------------------
class CandleAggregator:
    """
    Aggregator for a single timeframe.

    - If is_base=True, each CSV row is immediately treated as a closed candle (no accumulation).
    - If is_base=False, we accumulate sub_count base candles, or we fill zero-volume candles for missing periods.
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
        For base aggregator, finalize the row as a closed candle with the row's real OHLCV.
        """
        if not self.is_base:
            raise RuntimeError("Called on_base_csv_row() on a non-base aggregator?")

        aggregator_manager.record_closure(
            aggregator_closure_ts=ts,
            tf=self.tf,
            label_ts=ts,
            o=o, h=h, l=l, c=c, vol=v
        )
        self.last_sub_ts = ts

    def on_base_candle_closed(self, base_ts, o, h, l, c, v):
        """
        For higher aggregator. Accumulate subcandles. If we jump multiple aggregator boundaries => produce zero candles.
        """
        if self.is_base:
            raise RuntimeError("Base aggregator called on_base_candle_closed?")

        if self.current_boundary is None:
            aligned = (base_ts // self.tf_ms) * self.tf_ms
            self.current_boundary = aligned

        # If we jumped multiple boundaries
        while self.current_boundary + self.tf_ms <= base_ts:
            if self.sub_count > 0:
                self._finalize_aggregator_candle(real_event_ts=self.last_sub_ts or self.current_boundary)
            else:
                self._record_zero_candle(real_event_ts=self.last_sub_ts or self.current_boundary)

            self.current_boundary += self.tf_ms
            self.reset()

        # If not already open, initialize aggregator candle
        if self.open_ts is None:
            self.open_ts = self.current_boundary
            self.open_px = o
            self.high_px = o
            self.low_px = o
            self.close_px = o
            self.volume = 0.0
            self.sub_count = 0

        # Accumulate
        self.high_px = max(self.high_px, h)
        self.low_px = min(self.low_px, l)
        self.close_px = c
        self.volume += v
        self.sub_count += 1
        self.last_sub_ts = base_ts

        # finalize if sub_count >= sub_factor
        if self.sub_count >= self.sub_factor:
            self._finalize_aggregator_candle(real_event_ts=base_ts)
            self.current_boundary += self.tf_ms
            self.reset()

    def _record_zero_candle(self, real_event_ts):
        """
        Creates a zero-volume candle at the aggregator boundary.
        """
        aggregator_manager.record_closure(
            aggregator_closure_ts=real_event_ts,
            tf=self.tf,
            label_ts=self.current_boundary,
            o=0.0, h=0.0, l=0.0, c=0.0,
            vol=0.0
        )

    def _finalize_aggregator_candle(self, real_event_ts):
        """
        Finalize aggregator candle from current open state.
        """
        if aggregator_manager.verbose:
            print(f"{Fore.MAGENTA}[DEBUG]{Style.RESET_ALL} aggregator {self.tf} FINALIZING => boundary={self.current_boundary}, o={self.open_px},h={self.high_px},l={self.low_px},c={self.close_px},vol={self.volume}")
        aggregator_manager.record_closure(
            aggregator_closure_ts=real_event_ts,
            tf=self.tf,
            label_ts=self.current_boundary,
            o=self.open_px,
            h=self.high_px,
            l=self.low_px,
            c=self.close_px,
            vol=self.volume
        )

    def get_current_open_candle(self) -> Optional[Candle]:
        """Returns the partial aggregator candle, if not finalized."""
        if self.is_base or self.open_ts is None:
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
    Manages the base aggregator plus higher aggregator(s).
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

    def record_closure(self, aggregator_closure_ts, tf, label_ts, o, h, l, c, vol):
        """Called by aggregator whenever it finalizes a candle."""
        if tf == self.base_tf:
            # forward to higher aggregator
            for agg in self.higher_aggs:
                agg.on_base_candle_closed(aggregator_closure_ts, o, h, l, c, vol)

        self.latest_closed_candles[tf] = (tf, label_ts, o, h, l, c, vol)

        if aggregator_closure_ts not in self.pending_closures:
            self.pending_closures[aggregator_closure_ts] = set()
        self.pending_closures[aggregator_closure_ts].add(tf)

    def on_subcandle(self, ts, o, h, l, c, v) -> List[CandleClosure]:
        self.pending_closures.clear()
        closures = self.base_agg.on_base_csv_row(ts, o, h, l, c, v)
        # 'on_base_csv_row' returns None, so 'closures' is always None here.
        # The real closures are recorded in self.pending_closures, so we build them below.
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
            for agg in self.higher_aggs:
                oc = agg.get_current_open_candle()
                if oc:
                    open_candles[agg.tf] = oc

            cc = CandleClosure(
                timestamp=closure_ts,
                closed_candles=closed_candles,
                open_candles=open_candles,
                last_closed=None
            )
            out.append(cc)

        return out

    def flush(self) -> List[CandleClosure]:
        """
        Called at iteration end => forcibly finalize or skip partial aggregator candles.
        """
        for agg in self.higher_aggs:
            if agg.open_ts is not None:
                if agg.sub_count >= agg.sub_factor:
                    real_ts = agg.last_sub_ts or agg.current_boundary
                    if self.verbose:
                        print(f"{Fore.MAGENTA}[DEBUG]{Style.RESET_ALL} aggregator {agg.tf} forcibly finalizing partial => sub_count={agg.sub_count}")
                    agg._finalize_aggregator_candle(real_event_ts=real_ts)
                else:
                    if self.verbose:
                        print(
                            f"{Fore.YELLOW}[DEBUG]{Style.RESET_ALL} aggregator {agg.tf} skipping partial flush => "
                            f"sub_count={agg.sub_count}/{agg.sub_factor}"
                        )
                if agg.current_boundary is not None:
                    agg.current_boundary += agg.tf_ms
                agg.reset()

        if self.pending_closures:
            closures = self._build_and_return_closures()
            self.pending_closures.clear()
            return closures
        return []


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
    Reads CSV data day-by-day. For each real row => aggregator_manager.on_subcandle(...).
    If a gap is found, produce exactly one gap candle, yield aggregator closures, then repeat.
    Adds debug so you can see if the row is being used or skipped.
    """

    def __init__(self, config: Config):
        self.config = config
        self.current_day = datetime.fromtimestamp(config.start_ts / 1000, tz=timezone.utc)
        self.buffer: List[CandleClosure] = []
        self.current_rows = None
        self.current_row_index = 0

        self._last_closure: Optional[CandleClosure] = None
        self.last_ts: Optional[int] = None
        self.last_close_val: Optional[float] = None

        if self.config.verbose:
            print(f"Iterator initialized: start={self.current_day}, "
                  f"end={'No end date' if config.end_ts is None else datetime.fromtimestamp(config.end_ts / 1000, tz=timezone.utc)}")

    def process_row(self, row):
        try:
            raw_ts = int(row["timestamp"])
            if self.config.verbose:
                print(f"{Fore.CYAN}[DEBUG]{Style.RESET_ALL} Checking row: timestamp={raw_ts}, row={row}")
            if raw_ts < self.config.start_ts:
                if self.config.verbose:
                    print(f"{Fore.CYAN}[DEBUG]{Style.RESET_ALL} => row timestamp < start_ts, skipping.")
                return None
            elif self.config.end_ts is not None and raw_ts > self.config.end_ts:
                if self.config.verbose:
                    print(f"{Fore.CYAN}[DEBUG]{Style.RESET_ALL} => row timestamp > end_ts, skipping.")
                return None
            else:
                if self.config.verbose:
                    print(f"--- {Fore.CYAN}[DEBUG]{Style.RESET_ALL} Checking row: timestamp={raw_ts}, row={row}")

            o = float(row["open"])
            h = float(row["high"])
            l = float(row["low"])
            c = float(row["close"])
            v = float(row["volume"])
            return (raw_ts, o, h, l, c, v)
        except (ValueError, KeyError) as e:
            print(f"Error processing row: {row}, Error: {e}")
            return None

    def load_next_file(self) -> bool:
        if self.config.end_ts is not None:
            end_date = datetime.fromtimestamp(self.config.end_ts/1000, tz=timezone.utc).date()
        else:
            end_date = datetime.now(timezone.utc).date()

        if self.current_day.date() > end_date:
            if self.config.verbose:
                print(f"Past end date: {self.current_day.date()} > {end_date}")
            return False

        while self.current_day.date() <= end_date:
            csv_path = os.path.join(
                os.path.expanduser(f"{self.config.data_dir}/{self.config.exchange}/candles/{self.config.ticker}/{self.config.base_timeframe}"),
                f"{self.current_day.date()}.csv"
            )
            cd = self.current_day.date()
            self.current_day += timedelta(days=1)

            if os.path.isfile(csv_path):
                with open(csv_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    self.current_rows = sorted(reader, key=lambda r: int(r["timestamp"]))
                    self.current_row_index = 0
                    if self.config.verbose:
                        print(f"Loaded file: {csv_path} with {len(self.current_rows)} rows")
                return True
            else:
                if self.config.verbose:
                    print(f"File not found: {csv_path} (date={cd})")

        if self.config.verbose:
            print(f"No more files to process. Current day {self.current_day.date()} > end date {end_date}")
        return False

    def _emit_base_candle(self, ts, o, h, l, c, v, is_gap=False):
        """
        Feed one base candle => aggregator => store closures in buffer.
        If is_gap=True, we log additional debug info.
        """
        if self.config.verbose:
            if is_gap:
                print(f"{Fore.GREEN}[GAP-FILL]{Style.RESET_ALL} Emitting gap candle at ts={ts} with close={c} volume=0.0")
            else:
                print(f"{Fore.GREEN}[REAL]{Style.RESET_ALL} Emitting real candle at ts={ts} o={o},h={h},l={l},c={c},v={v}")

        new_closures = aggregator_manager.on_subcandle(ts, o, h, l, c, v)
        if new_closures:
            self.buffer.extend(new_closures)

    def __iter__(self):
        return self

    def __next__(self) -> CandleClosure:
        global aggregator_manager

        # If we already have closures buffered, return them first
        if self.buffer:
            cc = self.buffer.pop(0)
            cc.last_closed = self._last_closure
            self._last_closure = cc
            return cc

        while True:
            if self.current_rows is None or self.current_row_index >= len(self.current_rows):
                if not self.load_next_file():
                    # final flush
                    flushes = aggregator_manager.flush()
                    if flushes:
                        self.buffer.extend(flushes)
                        cc = self.buffer.pop(0)
                        cc.last_closed = self._last_closure
                        self._last_closure = cc
                        return cc
                    raise StopIteration

            # process next row
            if self.current_row_index < len(self.current_rows):
                row = self.current_rows[self.current_row_index]
                self.current_row_index += 1

                parsed = self.process_row(row)
                if parsed is None:
                    continue

                ts, o, h, l, c, v = parsed
                base_ms = TIMEFRAMES[self.config.base_timeframe]

                # check for gap
                if self.last_ts is not None:
                    next_expected = self.last_ts + base_ms
                    if next_expected < ts:
                        # produce single gap candle
                        fill_price = self.last_close_val if self.last_close_val is not None else c
                        self._emit_base_candle(next_expected, fill_price, fill_price, fill_price, fill_price, 0.0, is_gap=True)
                        self.last_ts = next_expected
                        self.current_row_index -= 1
                        if self.buffer:
                            cc = self.buffer.pop(0)
                            cc.last_closed = self._last_closure
                            self._last_closure = cc
                            return cc
                        # re-check same row on next iteration
                        continue

                # no gap => real candle
                self._emit_base_candle(ts, o, h, l, c, v, is_gap=False)
                self.last_ts = ts
                self.last_close_val = c

                if self.buffer:
                    cc = self.buffer.pop(0)
                    cc.last_closed = self._last_closure
                    self._last_closure = cc
                    return cc
            else:
                # done with the file
                continue


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

    if base_timeframe not in parsed_tfs:
        parsed_tfs.insert(0, base_timeframe)

    start_ts = parse_timestamp(start_date, True)
    end_ts = parse_timestamp(end_date, False)
    if start_ts is None:
        dt = datetime.now(timezone.utc) - timedelta(days=300)
        dt_midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        start_ts = int(dt_midnight.timestamp() * 1000)

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

    # Possibly sync data
    end_date_str = None
    if end_ts:
        end_dt = datetime.fromtimestamp(end_ts / 1000, timezone.utc)
        end_date_str = end_dt.strftime("%Y-%m-%d %H:%M")

    synchronize_candle_data(
        exchange=exchange,
        ticker=ticker,
        timeframe=base_timeframe,
        end_date_str=end_date_str,
        verbose=False
    )

    return CandleIterator(cfg)


if __name__ == "__main__":
    print(f"{ERROR} This module should not be run directly. Use example.py or your main script.")
    sys.exit(1)
