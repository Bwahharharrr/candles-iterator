#!/usr/bin/env python3

import argparse
import csv
import json
import os
import re
import sys
import numpy as np
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import List, Tuple, Dict, Iterator
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

# Additional color definitions for CLI output
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
    Each token can be:
      - exact timeframe e.g. "1h", "4h"
      - single operator e.g. "<1h", "<=1h", ">1h", ">=1h"
      - compound with '&' e.g. ">=1h&<=4h"
    We interpret each token, produce a set of matching TFs, then union them all together.
    """
    final_set = set()
    for token in tokens:
        # If there's an '&', we split and do intersection
        if '&' in token:
            subparts = token.split('&')
            sub_sets = [parse_single_relation_or_exact(tp) for tp in subparts]
            # intersection
            s_inter = sub_sets[0]
            for ss in sub_sets[1:]:
                s_inter = s_inter.intersection(ss)
            final_set = final_set.union(s_inter)
        else:
            # single token
            single_set = parse_single_relation_or_exact(token)
            final_set = final_set.union(single_set)

    return sorted(final_set, key=lambda tf: TIMEFRAMES[tf])

def parse_single_relation_or_exact(expr):
    """
    Parse a single expression that might be:
      - exact tf e.g. "1h"
      - relational e.g. "<1h", "<=1h", ">1h", ">=1h"
    Returns a set of timeframe strings that match.
    """
    expr = expr.strip()
    # If it matches an exact known timeframe:
    if expr in TIMEFRAMES:
        return {expr}

    # Otherwise, match patterns
    pattern = r"^(<=|>=|<|>)([0-9]+[mhDWhd]+)$"
    match = re.match(pattern, expr)
    if match:
        op = match.group(1)  # e.g. "<=", ">=", "<", ">"
        tf_str = match.group(2)  # e.g. "1h"

        if tf_str not in TIMEFRAMES:
            print(f"{WARNING} Unknown timeframe in expression: {tf_str}. Skipping.")
            return set()

        ref_ms = TIMEFRAMES[tf_str]
        possible_tfs = set(TIMEFRAMES.keys())
        out_set = set()

        if op == "<":
            out_set = {tf for tf in possible_tfs if TIMEFRAMES[tf] < ref_ms}
        elif op == "<=":
            out_set = {tf for tf in possible_tfs if TIMEFRAMES[tf] <= ref_ms}
        elif op == ">":
            out_set = {tf for tf in possible_tfs if TIMEFRAMES[tf] > ref_ms}
        elif op == ">=":
            out_set = {tf for tf in possible_tfs if TIMEFRAMES[tf] >= ref_ms}
        return out_set

    # If we get here, we cannot parse => skip
    print(f"{WARNING} Could not parse aggregation-tf token: '{expr}'. Skipping.")
    return set()

# ----------------------------------------------------------------------
# CandleAggregator base for 1 timeframe
# ----------------------------------------------------------------------
class CandleAggregator:
    """
    Maintains an aggregator for exactly one timeframe. For the base aggregator:
    - We close each CSV row immediately (since it's already final).
    For higher aggregators:
    - We accumulate closed base-candles as "sub-candles" until we reach sub_factor => then close.
    """

    def __init__(self, timeframe, is_base=False, sub_factor=1):
        self.tf = timeframe
        self.tf_ms = TIMEFRAMES[timeframe]

        self.is_base = is_base

        # For higher aggregator, sub_factor is how many base candles to accumulate
        self.sub_factor = sub_factor
        self.sub_count = 0

        self.open_ts = None
        self.open_px = None
        self.high_px = None
        self.low_px  = None
        self.close_px= None
        self.volume  = 0.0

        # We add last_sub_ts to fix partial flush logic
        self.last_sub_ts = None

    def reset(self):
        self.open_ts   = None
        self.open_px   = None
        self.high_px   = None
        self.low_px    = None
        self.close_px  = None
        self.volume    = 0.0
        self.sub_count = 0
        self.last_sub_ts = None

    def on_base_candle_closed(self, base_ts, o, h, l, c, v):
        """
        Called by aggregator_manager when a base aggregator closes a candle.
        This method accumulates that candle as a sub-candle for the higher aggregator.
        Once sub_count >= sub_factor => close & reset.
        """
        if self.is_base:
            raise RuntimeError("on_base_candle_closed() called on base aggregator? Should not happen.")

        if self.open_ts is None:
            self.open_ts = base_ts
            self.open_px = o
            self.high_px = o
            self.low_px  = o
            self.close_px= o
            self.volume  = 0.0

        self.high_px  = max(self.high_px, h)
        self.low_px   = min(self.low_px, l)
        self.close_px = c
        self.volume  += v

        self.sub_count += 1
        self.last_sub_ts = base_ts  # track last sub-candle time

        if self.sub_count >= self.sub_factor:
            # We'll label it by open_ts (aligned to timeframe boundary if you want).
            aligned_ts = (self.open_ts // self.tf_ms) * self.tf_ms
            aggregator_manager.record_closure(
                aggregator_closure_ts=base_ts,
                tf=self.tf,
                label_ts=aligned_ts,
                o=self.open_px,
                h=self.high_px,
                l=self.low_px,
                c=self.close_px,
                vol=self.volume
            )
            self.reset()

    def on_base_csv_row(self, ts, o, h, l, c, v):
        """
        Only used by the base aggregator. We close every single candle immediately,
        as each CSV row is final. The label_ts = ts.
        """
        if not self.is_base:
            raise RuntimeError("on_base_csv_row() called on a non-base aggregator?")

        # Because base aggregator closes each row, last_sub_ts is the row's ts
        self.last_sub_ts = ts

        aggregator_manager.record_closure(
            aggregator_closure_ts=ts,
            tf=self.tf,
            label_ts=ts,
            o=o, h=h, l=l, c=c, vol=v
        )

# ----------------------------------------------------------------------
# AggregatorManager
# ----------------------------------------------------------------------
class AggregatorManager:
    """
    Has:
    - base_agg (one aggregator for the base timeframe),
    - higher_aggs (a list of aggregator objects for higher timeframes).
    - a data structure for pending closures => so we can print them sorted in time order.
    """

    def __init__(self, base_tf, higher_tfs):
        self.base_tf = base_tf
        self.base_agg = CandleAggregator(base_tf, is_base=True)

        self.higher_aggs = []
        base_ms = TIMEFRAMES[base_tf]
        for tf in higher_tfs:
            factor = TIMEFRAMES[tf] // base_ms
            agg = CandleAggregator(tf, is_base=False, sub_factor=factor)
            self.higher_aggs.append(agg)

        self.pending_closures = {}

    def record_closure(self, aggregator_closure_ts, tf, label_ts, o, h, l, c, vol):
        """
        Called by CandleAggregator whenever it closes a candle. If it's the
        base aggregator => we pass that closed candle to each higher aggregator.
        Then we store to pending_closures for printing.
        """
        # If it was base aggregator => propagate sub-candle
        if tf == self.base_tf:
            for agg in self.higher_aggs:
                agg.on_base_candle_closed(
                    aggregator_closure_ts, o, h, l, c, vol
                )

        # Group closures by timestamp
        if aggregator_closure_ts not in self.pending_closures:
            self.pending_closures[aggregator_closure_ts] = []
        self.pending_closures[aggregator_closure_ts].append(
            (tf, label_ts, o, h, l, c, vol)
        )

    def on_subcandle(self, ts, o, h, l, c, v):
        """
        The base aggregator gets each CSV row. We close that row immediately.
        """
        self.pending_closures.clear()
        self.base_agg.on_base_csv_row(ts, o, h, l, c, v)
        if self.pending_closures:
            return self.print_closures()
        return []

    def print_closures(self):
        """
        Sort closures by aggregator_closure_ts and timeframe, then print.
        """
        result = []
        for ts in sorted(self.pending_closures.keys()):
            # Sort closures for this timestamp by timeframe
            closures = sorted(self.pending_closures[ts], key=lambda x: TIMEFRAMES[x[0]])
            result.append((ts, closures))

        self.pending_closures.clear()
        return result

    def flush(self):
        """
        We only forcibly close aggregators that actually completed their sub_factor
        (or is base, but base aggregator closes every row anyway).
        This avoids partial bars that cause the out-of-order close.
        """
        for agg in self.higher_aggs:
            if agg.open_ts is not None:
                # if sub_count >= sub_factor => we forcibly close
                # else we skip partial bar
                if agg.sub_count >= agg.sub_factor:
                    aggregator_closure_ts = agg.last_sub_ts
                    if aggregator_closure_ts is None:
                        aggregator_closure_ts = agg.open_ts  # fallback

                    aligned_ts = (agg.open_ts // agg.tf_ms) * agg.tf_ms

                    final_o = agg.open_px
                    final_h = agg.high_px
                    final_l = agg.low_px
                    final_c = agg.close_px
                    final_v = agg.volume

                    print(
                        f"{Fore.YELLOW}[DEBUG]{Style.RESET_ALL} aggregator {agg.tf} forcibly closed partial => "
                        f"closure_ts={aggregator_closure_ts} sub_count={agg.sub_count}/{agg.sub_factor}"
                    )

                    self.record_closure(
                        aggregator_closure_ts,
                        agg.tf,
                        aligned_ts,
                        final_o, final_h, final_l, final_c, final_v
                    )
                else:
                    # skip partial bar
                    print(
                        f"{Fore.YELLOW}[DEBUG]{Style.RESET_ALL} aggregator {agg.tf} skipping partial flush => "
                        f"sub_count={agg.sub_count}/{agg.sub_factor}"
                    )
                agg.reset()

        if self.pending_closures:
            return self.print_closures()
        return []


# Global variable for aggregator manager
aggregator_manager = None

def parse_timestamp(date_str, is_start=True):
    """Convert date string to millisecond timestamp"""
    if not date_str:
        return None
    try:
        if len(date_str) == 10:
            date_str += " 00:00" if is_start else " 23:59"
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        raise ValueError("Invalid date format")

class Config:
    def __init__(self, *, exchange, ticker, base_timeframe, start_ts, end_ts, data_dir):
        """
        Initialize Config with validated parameters
        
        Args:
            exchange: Exchange name
            ticker: Ticker symbol
            base_timeframe: Base timeframe
            start_ts: Start timestamp in milliseconds
            end_ts: End timestamp in milliseconds
            data_dir: Base directory for data
        """
        self.exchange = exchange
        self.ticker = ticker
        self.base_timeframe = base_timeframe
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.data_dir = data_dir

@dataclass
class Candle:
    timeframe: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    
    @property
    def datetime(self) -> datetime:
        """Convert timestamp to datetime object"""
        return datetime.fromtimestamp(self.timestamp/1000, tz=timezone.utc)
    
    def __str__(self) -> str:
        """Format candle for printing"""
        dt_str = self.datetime.strftime("%Y-%m-%d %H:%M")
        return (f"{self.timeframe} :: {self.timestamp} {dt_str} :: "
                f"o={self.open},h={self.high},l={self.low},"
                f"c={self.close},v={self.volume}")

class CandleClosure:
    def __init__(self, timestamp: int, candles: List[Tuple]):
        self.timestamp = timestamp
        self.candles: Dict[str, Candle] = {}
        
        # Convert raw tuples to Candle objects
        for tf, label_ts, o, h, l, c, v in candles:
            self.candles[tf] = Candle(
                timeframe=tf,
                timestamp=label_ts,
                open=o, high=h, low=l,
                close=c, volume=v
            )
    
    @property
    def datetime(self) -> datetime:
        """Convert closure timestamp to datetime"""
        return datetime.fromtimestamp(self.timestamp/1000, tz=timezone.utc)
    
    @property
    def timeframes(self) -> List[str]:
        """Get list of timeframes in this closure, sorted by duration"""
        return sorted(self.candles.keys(), key=lambda tf: TIMEFRAMES[tf])
    
    def get_candle(self, timeframe: str) -> Candle:
        """Get candle data for specific timeframe"""
        return self.candles.get(timeframe)
    
    def print(self) -> None:
        """Print closure in formatted way"""
        dt_str = self.datetime.strftime("%Y-%m-%d %H:%M")
        print(f"{Fore.CYAN}Closed{Style.RESET_ALL} "
              f"({Fore.YELLOW}{self.timestamp}{Style.RESET_ALL}) {dt_str}")
        
        for tf in self.timeframes:
            candle = self.candles[tf]
            print(f"  - {Fore.GREEN}{candle}{Style.RESET_ALL}")
    
    def __str__(self) -> str:
        """String representation of closure"""
        parts = [f"Closure at {self.datetime}:"]
        for tf in self.timeframes:
            parts.append(f"  {self.candles[tf]}")
        return "\n".join(parts)

class CandleIterator:
    def __init__(self, config):
        self.config = config
        self.current_day = datetime.fromtimestamp(config.start_ts / 1000, tz=timezone.utc)
        self.last_ts = None
        self.buffer = []
        self.current_rows = None
        self.current_row_index = 0

        print(f"Iterator initialized: start={self.current_day}, end={'No end date' if not hasattr(config, 'end_ts') or config.end_ts is None else datetime.fromtimestamp(config.end_ts / 1000, tz=timezone.utc)}")

    def process_row(self, row):
        """Process a single CSV row"""
        try:
            ts = int(row["timestamp"])
            # Skip if timestamp is outside our range
            if ts < self.config.start_ts:
                return None
            if hasattr(self.config, 'end_ts') and self.config.end_ts is not None and ts > self.config.end_ts:
                print(f"Reached end timestamp: {ts} > {self.config.end_ts}")
                return None
                
            return (
                ts,
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"])
            )
        except (ValueError, KeyError) as e:
            print(f"Error processing row: {row}, Error: {e}")
            return None

    def load_next_file(self):
        """Load next CSV file"""
        end_date = datetime.fromtimestamp(self.config.end_ts / 1000, tz=timezone.utc).date() if self.config.end_ts is not None else datetime.now(timezone.utc).date()
        
        # First check if we're already past the end date
        if self.current_day.date() > end_date:
            print(f"Past end date: {self.current_day.date()} > {end_date}")
            return False
        
        # Try to load files until we either find one or pass the end date
        while self.current_day.date() <= end_date:
            csv_path = os.path.join(
                os.path.expanduser(f"{self.config.data_dir}/{self.config.exchange}/candles/{self.config.ticker}/{self.config.base_timeframe}"),
                f"{self.current_day.date()}.csv"
            )
            
            # Increment the day BEFORE trying to load the file
            # This ensures we don't try the same day again
            current_date = self.current_day.date()
            self.current_day += timedelta(days=1)
            
            if os.path.isfile(csv_path):
                with open(csv_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    self.current_rows = sorted(reader, key=lambda r: int(r["timestamp"]))
                    self.current_row_index = 0
                    print(f"Loaded file: {csv_path} with {len(self.current_rows)} rows")
                    return True
            else:
                print(f"File not found: {csv_path}")
        
        print(f"No more files to process. Current day {self.current_day.date()} > end date {end_date}")
        return False

    def __iter__(self):
        return self

    def __next__(self):

        while True:
            # Process buffered results first
            if self.buffer:
                ts, candles = self.buffer.pop(0)
                return CandleClosure(ts, candles)

            # Check if we need to load a new file
            if self.current_rows is None or self.current_row_index >= len(self.current_rows):
                if not self.load_next_file():
                    # Do final flush before stopping
                    flush_results = aggregator_manager.flush()
                    if flush_results:
                        self.buffer.extend(flush_results)
                        ts, candles = self.buffer.pop(0)
                        return CandleClosure(ts, candles)
                    raise StopIteration
                continue

            # Process next row
            row = self.current_rows[self.current_row_index]
            self.current_row_index += 1
            
            candle = self.process_row(row)
            if candle is None:
                # If we're past end timestamp, stop iteration
                if self.config.end_ts is not None and int(row["timestamp"]) > self.config.end_ts:
                    flush_results = aggregator_manager.flush()
                    if flush_results:
                        self.buffer.extend(flush_results)
                        ts, candles = self.buffer.pop(0)
                        return CandleClosure(ts, candles)
                    raise StopIteration
                continue
                
            ts, o, h, l, c, v = candle

            # Handle gap for the base timeframe if needed
            if self.config.base_timeframe in TIMEFRAMES and self.last_ts is not None:
                gap_ms = TIMEFRAMES[self.config.base_timeframe]
                expected_ts = self.last_ts + gap_ms
                if expected_ts < ts:
                    self.last_ts = expected_ts

                    # Produce the 'fake' candle
                    results = aggregator_manager.on_subcandle(
                        expected_ts,
                        c, c, c, c,  # or self.last_close if you prefer
                        0.0
                    )
                    self.buffer.extend(results)

                    # If aggregator_manager signals a closure, yield or return it
                    if self.buffer:
                        out_ts, candles = self.buffer.pop(0)
                        self.current_row_index -= 1
                        return CandleClosure(out_ts, candles)

            # Process the real current candle now
            results = aggregator_manager.on_subcandle(ts, o, h, l, c, v)
            self.last_ts = ts

            if results:
                self.buffer.extend(results)
                ts, candles = self.buffer.pop(0)
                return CandleClosure(ts, candles)

def create_candle_iterator(
    exchange: str,
    ticker: str,
    base_timeframe: str,
    aggregation_timeframes: List[str],
    start_date: str = None,
    end_date: str = None,
    data_dir: str = "~/.corky"
) -> Iterator[CandleClosure]:
    """
    Aggregate candle data across multiple timeframes.
    
    Args:
        exchange: Exchange name
        ticker: Ticker symbol
        base_timeframe: Base timeframe
        aggregation_timeframes: List of timeframes to aggregate data for
        start_date: Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM)
        end_date: End date (YYYY-MM-DD or YYYY-MM-DD HH:MM)
        data_dir: Base directory for data
    
    Returns:
        Iterator yielding CandleClosure objects
    
    Raises:
        ValueError: If inputs are invalid or data directory not found
    """
    # 1. Validate data directory
    data_path = os.path.expanduser(f"{data_dir}/{exchange}/candles/{ticker}/{base_timeframe}")
    if not os.path.exists(data_path):
        raise ValueError(f"No data directory found: {data_path}")
    
    # 2. Validate base timeframe
    if base_timeframe not in TIMEFRAMES:
        raise ValueError(f"Invalid base timeframe: {base_timeframe}")
    
    # 3. Parse and validate aggregation timeframes
    parsed_tfs = parse_aggregation_timeframes(aggregation_timeframes)
    if not parsed_tfs:
        raise ValueError(f"No valid aggregation timeframes found from {aggregation_timeframes}")

    # Check that no aggregation-tf is smaller than base
    base_ms = TIMEFRAMES[base_timeframe]
    for tf in parsed_tfs:
        if TIMEFRAMES[tf] < base_ms:
            raise ValueError(f"Aggregation timeframe '{tf}' is smaller than base timeframe '{base_timeframe}'")

    # Ensure base timeframe is included
    if base_timeframe not in parsed_tfs:
        parsed_tfs.insert(0, base_timeframe)
    
    # 4. Convert dates to timestamps
    start_ts = parse_timestamp(start_date, True)
    end_ts = parse_timestamp(end_date, False)
    
    if start_ts is None:
        dt = datetime.now(timezone.utc) - timedelta(days=300)
        dt_midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        start_ts = int(dt_midnight.timestamp() * 1000)
    # if end_ts is None:
    #     end_ts = int(datetime.now(timezone.utc).timestamp() * 1000)

    # 5. Initialize aggregator manager
    global aggregator_manager  # Make it accessible globally
    higher_tfs = [t for t in parsed_tfs if t != base_timeframe]
    aggregator_manager = AggregatorManager(base_timeframe, higher_tfs)

    # 6. Create config with validated parameters
    config = Config(
        exchange=exchange,
        ticker=ticker,
        base_timeframe=base_timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        data_dir=data_dir
    )

    print(config.__dict__)

    # 7. Synchronize candle data
    # Convert end_ts (milliseconds) to end_date_str format (YYYY-MM-DD HH:MM)
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


    # 8. Create and return iterator
    return CandleIterator(config)

# ----------------------------------------------------------------------
# 6) MAIN
# ----------------------------------------------------------------------
def parse_args():
    """
    Parses command-line arguments with a color-coded help output.
    """
    parser = argparse.ArgumentParser(
        description=f"""
{INFO} Aggregate candle data across multiple timeframes {Style.RESET_ALL}

  {COLOR_VAR}--exchange{Style.RESET_ALL}         {COLOR_TYPE}(str){Style.RESET_ALL}    {COLOR_REQ} {COLOR_DESC}Exchange name (e.g., BITFINEX){Style.RESET_ALL}
  {COLOR_VAR}--ticker{Style.RESET_ALL}           {COLOR_TYPE}(str){Style.RESET_ALL}    {COLOR_REQ} {COLOR_DESC}Trading pair (e.g., tBTCUSD){Style.RESET_ALL}
  {COLOR_VAR}--timeframe{Style.RESET_ALL}        {COLOR_TYPE}(str){Style.RESET_ALL}    {COLOR_REQ} {COLOR_DESC}Base timeframe (e.g., 1m, 1h, 1D){Style.RESET_ALL}
  {COLOR_VAR}--aggregation-tfs{Style.RESET_ALL}  {COLOR_TYPE}(str){Style.RESET_ALL}           {COLOR_DESC}Timeframes to aggregate (e.g., 1h 4h >=2h&<=12h){Style.RESET_ALL}
  {COLOR_VAR}--start{Style.RESET_ALL}            {COLOR_TYPE}(str){Style.RESET_ALL}           {COLOR_DESC}Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM){Style.RESET_ALL}
  {COLOR_VAR}--end{Style.RESET_ALL}              {COLOR_TYPE}(str){Style.RESET_ALL}           {COLOR_DESC}End date (YYYY-MM-DD or YYYY-MM-DD HH:MM){Style.RESET_ALL}
  {COLOR_VAR}--data-dir{Style.RESET_ALL}         {COLOR_TYPE}(str){Style.RESET_ALL}           {COLOR_DESC}Base directory for candle data (default: ~/.corky){Style.RESET_ALL}

{INFO} Example Usage: {Style.RESET_ALL}
  {COLOR_FILE}python -m candle_iterator --exchange BITFINEX --ticker tBTCUSD --timeframe 1h{Style.RESET_ALL}
  {COLOR_FILE}python -m candle_iterator --exchange BITFINEX --ticker tBTCUSD --timeframe 1h --aggregation-tfs 1h 4h 1D{Style.RESET_ALL}
  {COLOR_FILE}python -m candle_iterator --exchange BITFINEX --ticker tBTCUSD --timeframe 1h --aggregation-tfs ">=2h&<=12h"{Style.RESET_ALL}
  {COLOR_FILE}python -m candle_iterator --exchange BITFINEX --ticker tBTCUSD --timeframe 1h --start "2024-01-01" --end "2024-02-01"{Style.RESET_ALL}
""",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument("--exchange", required=True, help="Exchange name")
    parser.add_argument("--ticker", required=True, help="Trading pair")
    parser.add_argument("--timeframe", required=True, help="Base timeframe")
    parser.add_argument("--aggregation-tfs", nargs="+", default=None,
                      help="Timeframes to aggregate data for (e.g., 1h 4h >=2h&<=12h). Defaults to base timeframe if not specified.")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM)")
    parser.add_argument("--data-dir", default="~/.corky",
                      help="Base directory for candle data")
    
    if len(sys.argv) == 1:
        print(f"\n{ERROR} No arguments provided! Please specify the required parameters.\n")
        parser.print_help()
        sys.exit(1)
        
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Print configuration summary
    print(f"\n{INFO} Running candle iterator with the following parameters:\n")
    print(f"  {COLOR_VAR}--exchange{Style.RESET_ALL}         {COLOR_TYPE}(str){Style.RESET_ALL}  {args.exchange}")
    print(f"  {COLOR_VAR}--ticker{Style.RESET_ALL}           {COLOR_TYPE}(str){Style.RESET_ALL}  {args.ticker}")
    print(f"  {COLOR_VAR}--timeframe{Style.RESET_ALL}        {COLOR_TYPE}(str){Style.RESET_ALL}  {args.timeframe}")
    
    aggregation_tfs = args.aggregation_tfs if args.aggregation_tfs is not None else [args.timeframe]
    print(f"  {COLOR_VAR}--aggregation-tfs{Style.RESET_ALL}  {COLOR_TYPE}(list){Style.RESET_ALL} {' '.join(aggregation_tfs)}")
    
    if args.start:
        print(f"  {COLOR_VAR}--start{Style.RESET_ALL}            {COLOR_TYPE}(str){Style.RESET_ALL}  {args.start}")
    if args.end:
        print(f"  {COLOR_VAR}--end{Style.RESET_ALL}              {COLOR_TYPE}(str){Style.RESET_ALL}  {args.end}")
    
    print(f"  {COLOR_VAR}--data-dir{Style.RESET_ALL}         {COLOR_TYPE}(str){Style.RESET_ALL}  {args.data_dir}")
    print(f"\n{INFO} Starting candle iterator...\n")

    try:
        for closure in create_candle_iterator(
            exchange=args.exchange,
            ticker=args.ticker,
            base_timeframe=args.timeframe,
            aggregation_timeframes=aggregation_tfs,
            start_date=args.start,
            end_date=args.end,
            data_dir=args.data_dir
        ):
            closure.print()
            # pass

    except ValueError as e:
        print(f"{ERROR} {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
