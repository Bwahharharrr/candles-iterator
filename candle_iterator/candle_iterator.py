#!/usr/bin/env python3

import argparse
import csv
import json
import os
import re
import sys
import time
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

# Environment key for external poll interval configuration
ENV_POLL_INTERVAL_KEY: str = "CANDLES_SYNC_POLL_INTERVAL_SECS"
MIN_POLL_INTERVAL_SECONDS: int = 1  # sanity lower bound for reported interval

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
    def __init__(self, timeframe: str, is_base: bool = False, sub_factor: int = 1):
        self.tf = timeframe
        self.tf_ms = TIMEFRAMES[timeframe]
        self.is_base = is_base
        self.sub_factor = sub_factor

        self.current_boundary: Optional[int] = None

        # Seeded "next open" (preview after a legitimate close)
        self._seed_next_open_ts: Optional[int] = None
        self._seed_next_open_price: Optional[float] = None

        # Live base 1m partial snapshot for higher-TF previews (is_base == False)
        self._live_partial: Optional[Dict[str, float]] = None  # keys: ts,o,h,l,c,v

        self.reset()

    def reset(self) -> None:
        # State of the currently forming *committed* aggregator candle
        self.open_ts: Optional[int] = None
        self.open_px: Optional[float] = None
        self.high_px: Optional[float] = None
        self.low_px: Optional[float] = None
        self.close_px: Optional[float] = None
        self.volume: float = 0.0
        self.sub_count: int = 0
        self.last_sub_ts: Optional[int] = None

        # Reset should clear live partial for this boundary; seeds persist.
        if not self.is_base:
            self._live_partial = None

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
        self._accumulate_base_subcandle(ts, o, h, l, c, v)

    def _accumulate_base_subcandle(self, base_ts: int, o: float, h: float, l: float, c: float, v: float) -> None:
        if self.current_boundary is None:
            self.current_boundary = (base_ts // self.tf_ms) * self.tf_ms

        # If we've advanced into (or past) the next boundary, finalize the prior candle(s).
        # STRICT: finalize only due to **observed** next-interval data (no wall-clock shortcuts).
        while self.current_boundary + self.tf_ms <= base_ts:
            if self.sub_count > 0:
                closing_price = self.close_px if self.close_px is not None else self.open_px
                # Use the last seen sub-timestamp for the event that caused closure.
                self._finalize_aggregator_candle(real_event_ts=self.last_sub_ts or self.current_boundary)
                if closing_price is not None:
                    next_bound = self.current_boundary + self.tf_ms
                    self._seed_next_open(next_bound, closing_price)
            else:
                # No data occurred in this boundary; emit a zero candle if desired by the manager.
                self._record_zero_candle(real_event_ts=self.current_boundary)

            self.current_boundary += self.tf_ms
            self.reset()

        # Initialize or update the *current* boundary state with this row
        if self.open_ts is None:
            self.open_ts = self.current_boundary
            self.open_px = o
            self.high_px = o
            self.low_px = o
            self.close_px = o
            self.volume = 0.0
            self.sub_count = 0

        # Update OHLC with latest snapshot for the minute; volume is snapshot (non-accumulating)
        self.high_px = max(self.high_px, h)
        self.low_px = min(self.low_px, l)
        self.close_px = c
        self.volume = float(v)
        self.sub_count += 1
        self.last_sub_ts = base_ts

        # NOTE: We intentionally do **not** finalize here based on wall-clock time.
        # The candle remains open until we ingest the first row of the next minute.

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

        if self.current_boundary is None:
            self.current_boundary = (base_ts // self.tf_ms) * self.tf_ms

        # Handle jumps across higher-TF boundaries
        while base_ts >= self.current_boundary + self.tf_ms:
            if self.sub_count > 0:
                closing_price = self.close_px if self.close_px is not None else self.open_px
                self._finalize_aggregator_candle(real_event_ts=self.last_sub_ts or self.current_boundary)
                if closing_price is not None:
                    self._seed_next_open(self.current_boundary + self.tf_ms, closing_price)
            else:
                self._record_zero_candle(real_event_ts=self.last_sub_ts or self.current_boundary)

            self.current_boundary += self.tf_ms
            self.reset()

        # Initialize forming higher-TF candle if needed
        if self.open_ts is None:
            self.open_ts = self.current_boundary
            self.open_px = o
            self.high_px = o
            self.low_px = o
            self.close_px = o
            self.volume = 0.0
            self.sub_count = 0

        # Commit this closed 1m into the forming higher-TF bar
        self.high_px = max(self.high_px, h)
        self.low_px = min(self.low_px, l)
        self.close_px = c
        self.volume += v
        self.sub_count += 1
        self.last_sub_ts = base_ts

        # Drop preview if it was exactly this now-closed minute
        if self._live_partial is not None:
            lp_ts = int(self._live_partial["ts"])
            if lp_ts == base_ts:
                self._live_partial = None

        # Finalize when we've reached the required number of subcandles
        if self.sub_count >= self.sub_factor:
            closing_price = self.close_px if self.close_px is not None else self.open_px
            self._finalize_aggregator_candle(real_event_ts=base_ts)
            if closing_price is not None:
                self._seed_next_open(self.current_boundary + self.tf_ms, closing_price)
            self.current_boundary += self.tf_ms
            self.reset()

    # -----------------------------
    # Higher timeframe live preview (from *open* base candles)
    # -----------------------------
    def on_base_partial(self, base_ts: int, o: float, h: float, l: float, c: float, v: float) -> None:
        if self.is_base:
            return  # base aggregator does not need this

        if self.last_sub_ts is not None and int(base_ts) == int(self.last_sub_ts):
            return  # avoid double-count preview

        boundary = (base_ts // self.tf_ms) * self.tf_ms
        if self.current_boundary is None:
            self.current_boundary = boundary

        self._live_partial = {"ts": float(base_ts), "o": float(o), "h": float(h),
                              "l": float(l), "c": float(c), "v": float(v)}

    # -----------------------------
    # Helpers: record/finalize/seed
    # -----------------------------
    def _seed_next_open(self, next_ts: int, price: float) -> None:
        self._seed_next_open_ts = int(next_ts)
        self._seed_next_open_price = float(price)

    def _record_zero_candle(self, real_event_ts: int) -> None:
        aggregator_manager.record_closure(
            aggregator_closure_ts=real_event_ts,
            tf=self.tf,
            label_ts=self.current_boundary,
            o=0.0, h=0.0, l=0.0, c=0.0,
            vol=0.0
        )

    def _finalize_aggregator_candle(self, real_event_ts: int) -> None:
        if self.open_ts is None:
            return
        if aggregator_manager and getattr(aggregator_manager, "verbose", False):
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
            o = h = l = c = float(self._seed_next_open_price)
            committed_vol = 0.0

        preview_vol = committed_vol
        if not self.is_base and self._live_partial is not None:
            lp_ts = int(self._live_partial["ts"])
            if self.last_sub_ts is None or lp_ts != int(self.last_sub_ts):
                lp_boundary = (lp_ts // self.tf_ms) * self.tf_ms
                if boundary_ts is None:
                    boundary_ts = lp_boundary
                    o = h = l = c = float(self._live_partial["o"])
                    committed_vol = 0.0
                if boundary_ts == lp_boundary:
                    if self.sub_count == 0:
                        o = float(self._live_partial["o"])
                    h = max(float(h), float(self._live_partial["h"])) if h is not None else float(self._live_partial["h"])
                    l = min(float(l), float(self._live_partial["l"])) if l is not None else float(self._live_partial["l"])
                    c = float(self._live_partial["c"])
                    preview_vol = committed_vol + float(self._live_partial["v"])

        if boundary_ts is None or o is None or h is None or l is None or c is None:
            return None

        return Candle(
            timeframe=self.tf,
            timestamp=int(boundary_ts),
            open=float(o),
            high=float(h),
            low=float(l),
            close=float(c),
            volume=float(preview_vol),
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

        # Snapshots of latest closed by TF and frozen-per-event history
        self.latest_closed_candles: Dict[str, Tuple[str, int, float, float, float, float, float]] = {}
        self._closed_by_ts: Dict[int, Dict[str, Tuple[str, int, float, float, float, float, float]]] = {}
        self.pending_closures: Dict[int, set] = {}

        self._all_timeframes = [self.base_tf] + higher_tfs
        self._final_partial_emitted = False  # used only for finite-range mode

        # Timing diagnostics toggle
        self.debug_timing: bool = self._truthy_env(os.environ.get("CANDLES_SYNC_DEBUG_TIMING")) or bool(self.verbose)
        self._timing_prefix = Fore.CYAN + "[TIMING]" + Style.RESET_ALL

    # ---------- Public snapshot utilities ----------
    def preferred_snapshot_ts(self) -> Optional[int]:
        """Pick a monotonic event timestamp for a heartbeat snapshot."""
        candidates: List[int] = []
        base_open = self.base_agg.get_current_open_candle()
        if base_open:
            candidates.append(int(base_open.timestamp))
        for agg in self.higher_aggs:
            oc = agg.get_current_open_candle()
            if oc:
                candidates.append(int(oc.timestamp))
        for rec in self.latest_closed_candles.values():
            candidates.append(int(rec[1]))
        return max(candidates) if candidates else None

    def build_snapshot_closure(self, event_ts: Optional[int] = None) -> Optional[CandleClosure]:
        ts = int(event_ts) if event_ts is not None else self.preferred_snapshot_ts()
        if ts is None:
            return None

        closed_candles = self._compose_closed_snapshot_for_ts(ts)

        open_candles: Dict[str, Candle] = {}
        for agg in self.higher_aggs:
            oc = agg.get_current_open_candle()
            if oc:
                open_candles[agg.tf] = oc
        base_open = self.base_agg.get_current_open_candle()
        if base_open:
            open_candles[self.base_tf] = base_open

        if not closed_candles and not open_candles:
            return None

        return CandleClosure(
            timestamp=ts,
            closed_candles=closed_candles,
            open_candles=open_candles,
            last_closed=None,
            is_final=False
        )

    # ---------- Ingestion ----------
    def record_closure(self, aggregator_closure_ts: int, tf: str, label_ts: int,
                       o: float, h: float, l: float, c: float, vol: float) -> None:
        """
        Called by CandleAggregator when a timeframe closes (or a zero candle recorded).
        Timing note: this is the earliest moment a closure becomes 'pending'.
        """
        t0 = time.perf_counter()

        if tf == self.base_tf:
            for agg in self.higher_aggs:
                agg.on_base_candle_closed(aggregator_closure_ts, o, h, l, c, vol)

        self.latest_closed_candles[tf] = (tf, label_ts, o, h, l, c, vol)

        if aggregator_closure_ts not in self._closed_by_ts:
            self._closed_by_ts[aggregator_closure_ts] = {}
        self._closed_by_ts[aggregator_closure_ts][tf] = (tf, label_ts, o, h, l, c, vol)

        if aggregator_closure_ts not in self.pending_closures:
            self.pending_closures[aggregator_closure_ts] = set()
        self.pending_closures[aggregator_closure_ts].add(tf)

        if self.debug_timing:
            dt_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            pend_count = sum(len(s) for s in self.pending_closures.values())
            print(
                f"{self._timing_prefix} {dt_str} record_closure tf={tf} label_ts={label_ts} "
                f"event_ts={aggregator_closure_ts} pending_tf_count={pend_count} "
                f"handler_s={(time.perf_counter()-t0):.6f}"
            )

    def on_subcandle(self, ts: int, o: float, h: float, l: float, c: float, v: float) -> List[CandleClosure]:
        # Feed into base
        self.base_agg.on_base_csv_row(ts, o, h, l, c, v)

        # Forward genuine open-minute partials to higher TFs
        if self.base_agg.open_ts is not None and int(ts) == int(self.base_agg.open_ts):
            for agg in self.higher_aggs:
                agg.on_base_partial(ts, o, h, l, c, v)

        # IMPORTANT FIX: During ingestion we do NOT emit snapshots.
        # Only return real finalized closures here; the iterator will request
        # a heartbeat snapshot explicitly when no closures were produced.
        return self._build_and_return_closures(snapshot_event_ts=None)

    # ---------- Internal: build closures ----------
    def _compose_closed_snapshot_for_ts(self, closure_ts: int) -> Dict[str, Candle]:
        out: Dict[str, Candle] = {}
        frozen = self._closed_by_ts.get(closure_ts, {})

        for tf in self._all_timeframes:
            if tf in frozen:
                (tfid, lbl_ts, oo, hh, ll, cc, vv) = frozen[tf]
                out[tf] = Candle(tfid, lbl_ts, oo, hh, ll, cc, vv)
                continue
            rec = self.latest_closed_candles.get(tf)
            if rec is not None:
                (tfid, lbl_ts, oo, hh, ll, cc, vv) = rec
                if lbl_ts <= closure_ts:
                    out[tf] = Candle(tfid, lbl_ts, oo, hh, ll, cc, vv)
        return out

    def _build_snapshot_closure(self, snapshot_event_ts: Optional[int]) -> List[CandleClosure]:
        if snapshot_event_ts is None:
            return []

        closed_candles = self._compose_closed_snapshot_for_ts(snapshot_event_ts)

        open_candles: Dict[str, Candle] = {}
        for agg in self.higher_aggs:
            oc = agg.get_current_open_candle()
            if oc:
                open_candles[agg.tf] = oc
        base_open = self.base_agg.get_current_open_candle()
        if base_open:
            open_candles[self.base_tf] = base_open

        if not closed_candles and not open_candles:
            return []

        cc = CandleClosure(
            timestamp=int(snapshot_event_ts),
            closed_candles=closed_candles,
            open_candles=open_candles,
            last_closed=None,
            is_final=False
        )
        return [cc]

    def _build_and_return_closures(self, snapshot_event_ts: Optional[int] = None) -> List[CandleClosure]:
        t0 = time.perf_counter()
        out: List[CandleClosure] = []

        if self.pending_closures:
            pending_items = sorted(self.pending_closures.keys())
            for closure_ts in pending_items:
                closed_candles = self._compose_closed_snapshot_for_ts(closure_ts)

                open_candles: Dict[str, Candle] = {}
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

            self.pending_closures.clear()
            self._closed_by_ts.clear()

            if self.debug_timing:
                dt_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                print(
                    f"{self._timing_prefix} {dt_str} build_and_return_closures "
                    f"count={len(out)} elapsed_s={(time.perf_counter()-t0):.6f}"
                )
            return out

        # No finalized closures -> optionally build a snapshot (callers decide).
        out2 = self._build_snapshot_closure(snapshot_event_ts)
        if self.debug_timing and out2:
            dt_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"{self._timing_prefix} {dt_str} build_snapshot_closure "
                f"ts={snapshot_event_ts} elapsed_s={(time.perf_counter()-t0):.6f}"
            )
        return out2

    # ---------- Flush (finite-range only) ----------
    def flush(self) -> List[CandleClosure]:
        """
        Emit any pending finalized closures and one last partial snapshot.
        IMPORTANT: We do **not** force-close the base TF on wall-clock time.
        """
        out: List[CandleClosure] = []

        # If higher TFs already have enough subs, allow them to finalize (this is logical completion)
        for agg in self.higher_aggs:
            if agg.open_ts is not None and agg.sub_count >= agg.sub_factor:
                if self.verbose:
                    print(f"{Fore.MAGENTA}[DEBUG]{Style.RESET_ALL} forcibly finalizing aggregator {agg.tf}")
                closing_price = agg.close_px if agg.close_px is not None else agg.open_px
                agg._finalize_aggregator_candle(real_event_ts=agg.last_sub_ts)
                if closing_price is not None and agg.current_boundary is not None:
                    agg._seed_next_open(agg.current_boundary + agg.tf_ms, closing_price)
                agg.current_boundary = (agg.current_boundary or 0) + agg.tf_ms
                agg.reset()

        # Emit any newly finalized closures first
        if self.pending_closures:
            closures = self._build_and_return_closures()
            out.extend(closures)

        # Final partial snapshot (once)
        if not self._final_partial_emitted:
            partial_open_candles: Dict[str, Candle] = {}
            latest_open_ts: Optional[int] = None
            all_aggs = [self.base_agg] + self.higher_aggs
            for aggregator in all_aggs:
                oc = aggregator.get_current_open_candle()
                if oc:
                    partial_open_candles[aggregator.tf] = oc
                    if latest_open_ts is None or oc.timestamp > latest_open_ts:
                        latest_open_ts = oc.timestamp
            if partial_open_candles and latest_open_ts is not None:
                final_closed_candles: Dict[str, Candle] = {}
                for tf, rec in self.latest_closed_candles.items():
                    (tfid, lbl_ts, oo, hh, ll, cc, vv) = rec
                    final_closed_candles[tf] = Candle(tfid, lbl_ts, oo, hh, ll, cc, vv)
                cc = CandleClosure(
                    timestamp=latest_open_ts,
                    closed_candles=final_closed_candles,
                    open_candles=partial_open_candles,
                    last_closed=None,
                    is_final=True
                )
                out.append(cc)
            self._final_partial_emitted = True

        return out

    # -----------------------------
    # Utilities
    # -----------------------------
    @staticmethod
    def _truthy_env(val: Optional[str]) -> bool:
        if val is None:
            return False
        s = val.strip().lower()
        return s not in ("", "0", "false", "no", "off")




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
        self.exchange = exchange
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

    Timing diagnostics:
    - Enabled with env `CANDLES_SYNC_DEBUG_TIMING=1` or verbose=True.
      Each poll cycle prints:
        * sleep_s, sync_s
        * list_files_s (time to enumerate candidate files since start point)
        * drain_s (time to scan & ingest rows)
        * ingest_s (time spent inside _ingest_row/aggregator)
        * rows_scanned, first_row_ts, emitted_row_ts
        * first_emit_after_sync_s (delay from sync end → first closure)
        * advanced (whether emitted_row_ts > starting last_ts)
        * closures_added (buffer delta)
        * redrain (whether a no-sleep/no-resync continuation will occur)
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.data_path = os.path.expanduser(
            f"{cfg.data_dir}/{cfg.exchange}/candles/{cfg.ticker}/{cfg.base_timeframe}"
        )
        if not os.path.isdir(self.data_path):
            raise ValueError(f"Data directory not found: {self.data_path}")

        self.csv_file_paths = self._list_csv_files()
        if self.cfg.verbose:
            print(f"{INFO} Found {len(self.csv_file_paths)} data files for base timeframe: {self.cfg.base_timeframe}")

        self._candles_gen = self._load_candles_from_disk()
        self._closed_buffer: List[CandleClosure] = []
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

        # Timing diagnostics
        self.debug_timing: bool = self._truthy_env(os.environ.get("CANDLES_SYNC_DEBUG_TIMING")) or bool(self.cfg.verbose)
        self._timing_prefix = Fore.CYAN + "[TIMING]" + Style.RESET_ALL

    def __iter__(self) -> Iterator[CandleClosure]:
        return self

    def __next__(self) -> CandleClosure:
        global aggregator_manager

        # Return buffered closures first (lowest latency)
        if self._closed_buffer:
            return self._closed_buffer.pop(0)

        while True:
            try:
                row_ts, o, h, l, c, v = next(self._candles_gen)
            except StopIteration:
                # If an explicit end_ts was set OR polling is disabled, we truly end.
                if self.cfg.end_ts is not None or self.poll_interval_seconds is None:
                    if not self._final_flush_done:
                        all_closures = aggregator_manager.flush()
                        self._closed_buffer.extend(all_closures)
                        self._final_flush_done = True
                        if self._closed_buffer:
                            return self._closed_buffer.pop(0)
                    raise StopIteration

                # Live mode (no end_ts) with polling enabled: switch to polling
                if not self._polling_notice_emitted:
                    self._announce_polling_mode()
                    # FIRST POLL: run immediately (no sleep), resync from remote,
                    # and stop draining as soon as any real closure is produced.
                    self._poll_for_new_data_and_buffer(
                        sleep_first=False, resync=True, stop_after_first_emission=True
                    )
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
                    return self._closed_buffer.pop(0)

                # Otherwise, emit a single heartbeat snapshot immediately
                snap_ts = aggregator_manager.preferred_snapshot_ts()
                snapshot = aggregator_manager.build_snapshot_closure(event_ts=snap_ts)
                if snapshot is not None:
                    self._closed_buffer.append(snapshot)
                    return self._closed_buffer.pop(0)

                # Nothing at all to emit; loop to next poll
                continue

            # Normal ingestion from file: process row and emit as soon as we have a closure
            self._ingest_row(row_ts, o, h, l, c, v)
            if self._closed_buffer:
                return self._closed_buffer.pop(0)

    # -----------------------------
    # Ingestion helpers
    # -----------------------------
    def _ingest_row(self, row_ts: int, o: float, h: float, l: float, c: float, v: float) -> None:
        """Common ingestion for a single base row (used for file + poll drains)."""
        global aggregator_manager

        # Initialize last seen timestamp (we still ingest this row)
        if self._last_ts is None:
            self._last_ts = row_ts
            self._last_close = o

        # Fill any full-minute gaps before this row
        while self._last_ts is not None and row_ts > self._last_ts + self.base_ms:
            missing_ts = self._last_ts + self.base_ms
            if self.cfg.end_ts and missing_ts > self.cfg.end_ts:
                pending = aggregator_manager.flush()
                if pending:
                    self._closed_buffer.extend(pending)
                return
            if self.cfg.verbose:
                dt_str = datetime.fromtimestamp(missing_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                print(f"{Fore.RED}[MISSING]{Style.RESET_ALL} Candle missing at {missing_ts} ({dt_str}) => generating fake candle (0 volume)")
            dummy = self._last_close if self._last_close is not None else o
            new_closures = aggregator_manager.on_subcandle(missing_ts, dummy, dummy, dummy, dummy, 0.0)
            self._closed_buffer.extend(new_closures)
            self._last_ts = missing_ts
            self._last_close = dummy

        # Enforce end date bound (mostly relevant if end_ts was given)
        if self.cfg.end_ts and row_ts > self.cfg.end_ts:
            pending = aggregator_manager.flush()
            if pending:
                self._closed_buffer.extend(pending)
            return

        # Skip strictly older timestamps; allow equal timestamps as snapshot updates.
        if self._last_ts is not None and row_ts < self._last_ts:
            if self.cfg.verbose:
                print(f"{WARNING} Skipping out-of-order/older timestamp: {row_ts}")
            return

        # Normal or snapshot-update ingestion:
        closures = aggregator_manager.on_subcandle(row_ts, o, h, l, c, v)
        self._closed_buffer.extend(closures)
        self._last_ts = row_ts
        self._last_close = c

    # -----------------------------
    # Polling helpers
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

    def _announce_polling_mode(self) -> None:
        """
        Print a single update when we transition from file iteration to polling mode.
        """
        if self._polling_notice_emitted or self.poll_interval_seconds is None:
            return

        interval_s = self.poll_interval_seconds
        tf = self.cfg.base_timeframe
        print(
            f"{UPDATE} Historical files exhausted — entering polling mode. "
            f"Polling every {interval_s}s (base timeframe: {tf})."
        )
        self._polling_notice_emitted = True

    def _poll_for_new_data_and_buffer(
        self,
        *,
        sleep_first: bool = True,
        resync: bool = True,
        stop_after_first_emission: bool = False
    ) -> None:
        """
        Optionally sleep, optionally resync from the remote source, then drain new rows
        from disk into the local closure buffer.

        Args:
            sleep_first:
                If True, sleep for the configured poll interval before syncing/draining.
                Disabled on the first poll and during immediate re-drains.
            resync:
                If True, call synchronize_candle_data() to fetch fresh data.
                For immediate re-drains, set False to avoid an unnecessary follow-up sync.
            stop_after_first_emission:
                If True, stop draining as soon as we enqueue a **real** CandleClosure
                triggered by a row whose timestamp strictly advances beyond the starting
                last_ts. Same-ts snapshots are ignored for early-break purposes.

        Timing diagnostics:
            Prints a detailed timing summary when debug_timing is enabled, including
            the critical metric `first_emit_after_sync_s` which is the delay from
            the end of synchronize_candle_data() to the first emitted closure.
        """
        import time as _time  # local alias to emphasize perf_counter/sleep scope
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
            t_sync_end_perf = _time.perf_counter()  # mark sync-finish moment

        # 3) Decide start point for reading:
        #    - Resync pass: inclusive (>= last_ts) to pick up same-ts snapshot updates.
        #    - Re-drain pass: strict (> last_ts) to avoid re-reading the same row.
        if resync:
            start_from = self._last_ts if self._last_ts is not None else (self.cfg.start_ts or 0)
        else:
            start_from = (self._last_ts + 1) if self._last_ts is not None else (self.cfg.start_ts or 0)

        # 3a) (Diagnostics) File-list time & count
        list_files_s = 0.0
        files_cnt = None
        if self.debug_timing:
            t0_list = _time.perf_counter()
            try:
                files_cnt = len(self._list_csv_files_since(start_from))
            except Exception:
                files_cnt = None
            list_files_s = _time.perf_counter() - t0_list

        # 4) Drain rows until first emission (or EOF)
        t0_drain = _time.perf_counter()
        gen = self._load_candles_since(start_from)

        drained_any = False
        rows_scanned = 0
        first_row_ts: Optional[int] = None
        emitted_row_ts: Optional[int] = None
        closures_before = len(self._closed_buffer)
        ingest_time_s = 0.0
        first_emit_after_sync_s = None  # <-- key metric

        for row in gen:
            drained_any = True
            rows_scanned += 1
            row_ts, o, h, l, c, v = row
            if first_row_ts is None:
                first_row_ts = row_ts

            t0_ingest = _time.perf_counter()
            before = len(self._closed_buffer)
            self._ingest_row(row_ts, o, h, l, c, v)
            t1_ingest = _time.perf_counter()
            ingest_time_s += (t1_ingest - t0_ingest)

            # FIX: break only when a **real closure** occurs from a row that strictly
            # advances the last_ts baseline. Same-ts snapshots no longer cause an early break.
            if stop_after_first_emission and len(self._closed_buffer) > before and row_ts > initial_last_ts:
                emitted_row_ts = row_ts
                # If a sync happened this cycle, measure delay from sync end to first emit
                if t_sync_end_perf is not None:
                    first_emit_after_sync_s = (t1_ingest - t_sync_end_perf)
                # We advanced beyond the starting timestamp -> allow immediate re-drain
                self._redrain_after_first_emission = True
                break

        drain_s = _time.perf_counter() - t0_drain
        closures_added = len(self._closed_buffer) - closures_before

        # 5) Timing summary
        if self.debug_timing:
            advanced = (emitted_row_ts is not None and emitted_row_ts > initial_last_ts)
            now_str_end = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"{self._timing_prefix} {now_str_end} poll_summary "
                f"sleep_s={sleep_s:.3f} sync_s={sync_s:.3f} list_files_s={list_files_s:.3f} "
                f"drain_s={drain_s:.3f} ingest_s={ingest_time_s:.3f} files={files_cnt} "
                f"rows_scanned={rows_scanned} first_row_ts={first_row_ts} emitted_row_ts={emitted_row_ts} "
                f"first_emit_after_sync_s={(first_emit_after_sync_s if first_emit_after_sync_s is not None else -1):.3f} "
                f"advanced={advanced} closures_added={closures_added} redrain={self._redrain_after_first_emission}"
            )

        if self.cfg.verbose and resync and not drained_any:
            print(f"{INFO} Poll returned no new rows; emitting snapshot.")

    # -----------------------------
    # File/CSV helpers (restored)
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

    def _list_csv_files_since(self, start_ts: int) -> List[str]:
        """
        List CSV files whose file-range intersects [start_ts, +inf).
        (Inclusive lower bound to ensure we don't drop the file that contains start_ts.)
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
            if f_end < start_ts:
                continue
            selected_files.append(fullpath)
        return selected_files

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
        Load candles with ts >= start_inclusive_ts from disk, covering
        any files that might have appeared/expanded since last read.
        Yields (ts, o, h, l, c, v).

        Rationale: On a resync pass we include the row at exactly the last-seen
        timestamp so we can ingest updated snapshots (same timestamp, fresher OHLCV).
        For re-drains we call this with (last_ts + 1) so the effect is strict.
        """
        file_list = self._list_csv_files_since(start_inclusive_ts)  # inclusive lower bound
        for csv_path in file_list:
            if self.cfg.verbose:
                print(f"{INFO} (poll) Reading file: {csv_path}")
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

                    # Inclusive lower bound enforced by caller; for re-drain we pass last_ts+1
                    if ts < start_inclusive_ts:
                        continue
                    if self.cfg.end_ts and ts > self.cfg.end_ts:
                        return  # respect explicit end bound if ever set

                    yield (ts, o, h, l, c, v)

    # -----------------------------
    # Utilities
    # -----------------------------
    @staticmethod
    def _truthy_env(val: Optional[str]) -> bool:
        if val is None:
            return False
        s = val.strip().lower()
        return s not in ("", "0", "false", "no", "off")


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

    def _truthy_env(val: Optional[str]) -> bool:
        return (val or "").strip().lower() in ("1", "true", "yes", "on", "y", "t")

    # --- Validate paths and timeframes ---
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

    # Ensure the base TF is included
    if base_timeframe not in parsed_tfs:
        parsed_tfs.insert(0, base_timeframe)

    start_ts = parse_timestamp(start_date, True)
    end_ts = parse_timestamp(end_date, False)

    # If no explicit start, back off 200 bars of the largest TF and align
    if start_ts is None:
        largest_tf = max(parsed_tfs, key=lambda x: TIMEFRAMES[x])
        largest_tf_ms = TIMEFRAMES[largest_tf]

        if end_ts is None:
            end_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
            if verbose:
                print(f"{INFO} No end date supplied. Using 'now' => {end_ts}.")

        raw_start_ts = end_ts - (200 * largest_tf_ms)
        start_ts = (raw_start_ts // largest_tf_ms) * largest_tf_ms

        if verbose:
            print(f"{INFO} No start date supplied. Using highest timeframe '{largest_tf}' "
                  f"({largest_tf_ms} ms) and 200 bars => raw start = {raw_start_ts}, aligned to {start_ts}.")

    # --- Build manager and config ---
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
        verbose=verbose,
        poll_interval_seconds=poll_interval_seconds,
    )

    # --- Enable "trace-at-return" for synchronize_candle_data by rebinding the imported symbol ---
    # This guarantees you see a TRACE line at the *exact* moment the function returns,
    # regardless of what the inner candles-sync logs measure.
    should_wrap = _truthy_env(os.environ.get("CANDLES_SYNC_TRACE")) or verbose
    if should_wrap:
        # We rebind the *imported* symbol `synchronize_candle_data` in this module’s globals
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

    return CandleIterator(cfg)



if __name__ == "__main__":
    print(f"{ERROR} This module should not be run directly. Use example.py or your main script.")
    sys.exit(1)
