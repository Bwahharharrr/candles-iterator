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

    Enhancements (and fixes):
    - "Seed next open" behavior: immediately after finalizing a candle, we create
      a synthetic *open* bar at the next boundary with OHLC set to the last close
      and volume=0. This ensures the logs always show an open bar the instant a
      candle closes.
    - For non-base aggregators (e.g., 5m when base is 1m), we accept real-time
      base partial updates via on_base_partial(). get_current_open_candle()
      merges committed state with the current 1m partial snapshot to preview OHLC
      and volume non-destructively.
    - FIX: prevent double-count in higher TF preview volumes by ignoring a
      "partial" whose timestamp is already finalized (i.e., equals last_sub_ts).
    """
    def __init__(self, timeframe: str, is_base=False, sub_factor=1):
        self.tf = timeframe
        self.tf_ms = TIMEFRAMES[timeframe]
        self.is_base = is_base
        self.sub_factor = sub_factor

        self.current_boundary: Optional[int] = None

        # Seeded "next open" (preview open bar right after a closure)
        self._seed_next_open_ts: Optional[int] = None
        self._seed_next_open_price: Optional[float] = None

        # Live partial snapshot of the current base (1m) candle for higher TF preview
        # Only used when is_base == False
        self._live_partial: Optional[Dict[str, float]] = None  # keys: ts,o,h,l,c,v

        self.reset()

    def reset(self):
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
    def on_base_csv_row(self, ts, o, h, l, c, v):
        """
        Called only for the base aggregator. Treat each base row as the latest
        snapshot for its minute; finalize previous minute if needed; form/update
        the current minute; seed the next open bar on closure.
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
                # capture close for seeding before reset
                closing_price = self.close_px if self.close_px is not None else self.open_px
                self._finalize_aggregator_candle(real_event_ts=self.last_sub_ts or self.current_boundary)
                # seed next open at the very next boundary with last close
                next_bound = self.current_boundary + self.tf_ms
                if closing_price is not None:
                    self._seed_next_open(next_bound, closing_price)
            else:
                self._record_zero_candle(real_event_ts=self.current_boundary)
                # No seed when there was no previous close.

            self.current_boundary += self.tf_ms
            self.reset()

        # Initialize aggregator candle if empty (for this boundary)
        if self.open_ts is None:
            self.open_ts = self.current_boundary
            self.open_px = o
            self.high_px = o
            self.low_px = o
            self.close_px = o
            self.volume = 0.0
            self.sub_count = 0

        # Update OHLC with the latest snapshot for this minute
        self.high_px = max(self.high_px, h)
        self.low_px = min(self.low_px, l)
        self.close_px = c
        # Base aggregator holds *current minute* volume snapshot
        self.volume = float(v)
        self.sub_count += 1
        self.last_sub_ts = base_ts

        # Finalize if we're clearly behind the boundary (next minute for base TF)
        fully_behind_now = (base_ts + self.tf_ms <= now_ms)
        if (base_ts > (self.current_boundary + self.tf_ms - 1)) or fully_behind_now:
            closing_price = self.close_px if self.close_px is not None else self.open_px
            self._finalize_aggregator_candle(real_event_ts=base_ts)
            # Seed the just-opened next minute immediately with last close
            if closing_price is not None:
                next_bound = self.current_boundary + self.tf_ms
                self._seed_next_open(next_bound, closing_price)
            self.current_boundary += self.tf_ms
            self.reset()

    # -----------------------------
    # Higher timeframe ingestion (from closed base candles)
    # -----------------------------
    def on_base_candle_closed(self, base_ts, o, h, l, c, v):
        """
        For higher timeframes (is_base=False). We accumulate each *closed* 1m subcandle.
        Once we have sub_factor subcandles, we finalize. Also handle multi-interval jumps.
        """
        if self.is_base:
            raise RuntimeError("Base aggregator called on_base_candle_closed? (bug)")

        # Initialize boundary if needed
        if self.current_boundary is None:
            self.current_boundary = (base_ts // self.tf_ms) * self.tf_ms

        # Handle jumps across boundaries (fill/finalize along the way)
        while base_ts >= self.current_boundary + self.tf_ms:
            if self.sub_count > 0:
                # finalize current higher-TF candle, seed next open
                closing_price = self.close_px if self.close_px is not None else self.open_px
                self._finalize_aggregator_candle(real_event_ts=self.last_sub_ts or self.current_boundary)
                if closing_price is not None:
                    self._seed_next_open(self.current_boundary + self.tf_ms, closing_price)
            else:
                self._record_zero_candle(real_event_ts=self.last_sub_ts or self.current_boundary)
                # no seed when there was no previous close

            self.current_boundary += self.tf_ms
            self.reset()

        # Initialize higher-TF open candle if none exists
        if self.open_ts is None:
            self.open_ts = self.current_boundary
            self.open_px = o
            self.high_px = o
            self.low_px = o
            self.close_px = o
            self.volume = 0.0
            self.sub_count = 0

        # Update committed OHLC & volume with this *closed* subcandle
        self.high_px = max(self.high_px, h)
        self.low_px = min(self.low_px, l)
        self.close_px = c
        self.volume += v
        self.sub_count += 1
        self.last_sub_ts = base_ts

        # If a previously stored "partial" is exactly this now-closed minute, drop it
        if self._live_partial is not None:
            lp_ts = int(self._live_partial["ts"])
            if lp_ts == base_ts:
                self._live_partial = None

        # Finalize if we've reached the required count of subcandles
        if self.sub_count >= self.sub_factor:
            closing_price = self.close_px if self.close_px is not None else self.open_px
            self._finalize_aggregator_candle(real_event_ts=base_ts)
            if closing_price is not None:
                # Seed next higher-TF open bar immediately
                self._seed_next_open(self.current_boundary + self.tf_ms, closing_price)
            self.current_boundary += self.tf_ms
            self.reset()

    # -----------------------------
    # Higher timeframe live preview (from *open* base candles)
    # -----------------------------
    def on_base_partial(self, base_ts: int, o: float, h: float, l: float, c: float, v: float):
        """
        Called for non-base aggregators to preview the impact of the *current open* 1m candle
        on the higher timeframe's forming candle. We do NOT commit this; we only show it in
        get_current_open_candle() by overlaying it on the committed state.

        Volume preview = committed sum of closed 1m + current open 1m volume (non-accumulating).
        """
        if self.is_base:
            return  # base aggregator does not need this

        # If base_ts equals the last finalized sub-candle timestamp, it's no longer "partial".
        if self.last_sub_ts is not None and int(base_ts) == int(self.last_sub_ts):
            return  # ignore; prevents double-counting the just-closed minute

        boundary = (base_ts // self.tf_ms) * self.tf_ms

        # Initialize boundary if we haven't yet (starting a new higher-TF window)
        if self.current_boundary is None:
            self.current_boundary = boundary

        # Store the latest snapshot (non-destructive, used in preview only)
        self._live_partial = {"ts": float(base_ts), "o": float(o), "h": float(h),
                              "l": float(l), "c": float(c), "v": float(v)}

    # -----------------------------
    # Helpers: record/finalize/seed
    # -----------------------------
    def _seed_next_open(self, next_ts: int, price: float) -> None:
        """
        Remember a synthetic next open candle to display immediately after a closure.
        """
        self._seed_next_open_ts = int(next_ts)
        self._seed_next_open_price = float(price)

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
        Returns a preview of the *currently open* candle for this aggregator.

        Behavior:
        - If a committed open candle exists (self.open_ts is not None), use it as the base.
        - Else, if we have a seeded next open (post-closure), return that seed with volume=0.
        - For non-base aggregators, if a live base partial is present for this boundary,
          overlay it (OHLC + add its volume once) to preview the in-progress higher-TF bar.
        """
        # Choose the boundary to preview
        boundary_ts = None
        o = h = l = c = None
        committed_vol = 0.0

        if self.open_ts is not None:
            # We have an actual forming candle from committed subcandles
            boundary_ts = self.open_ts
            o = self.open_px
            h = self.high_px
            l = self.low_px
            c = self.close_px
            committed_vol = float(self.volume)
        elif self._seed_next_open_ts is not None and self._seed_next_open_price is not None:
            # Use the seeded open candle (created instantly after closure)
            boundary_ts = self._seed_next_open_ts
            o = h = l = c = float(self._seed_next_open_price)
            committed_vol = 0.0

        # For non-base aggregators, overlay the current base partial if it belongs to this boundary
        if not self.is_base and self._live_partial is not None:
            lp_ts = int(self._live_partial["ts"])

            # If that "partial" is actually the last finalized minute, ignore it (fix double)
            if self.last_sub_ts is not None and lp_ts == int(self.last_sub_ts):
                preview_vol = committed_vol
            else:
                lp_boundary = (lp_ts // self.tf_ms) * self.tf_ms

                # If we had neither committed nor seeded boundary, allow preview purely from live partial:
                if boundary_ts is None:
                    boundary_ts = lp_boundary
                    o = h = l = c = float(self._live_partial["o"])
                    committed_vol = 0.0

                if boundary_ts == lp_boundary:
                    # If no committed subcandle yet, the true open should be the first 1m's open
                    if self.sub_count == 0:
                        o = float(self._live_partial["o"])

                    # Merge highs/lows and set latest close
                    h = max(float(h), float(self._live_partial["h"])) if h is not None else float(self._live_partial["h"])
                    l = min(float(l), float(self._live_partial["l"])) if l is not None else float(self._live_partial["l"])
                    c = float(self._live_partial["c"])
                    # Preview volume adds the *current* 1m partial volume once (non-accumulating)
                    preview_vol = committed_vol + float(self._live_partial["v"])
                else:
                    preview_vol = committed_vol
        else:
            preview_vol = committed_vol

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

    Key parts:
    - After any candle finalizes (base or higher), we seed the next open bar so
      logs immediately show an open candle with OHLC=last close and volume=0.
    - While ingesting base rows, we forward *only the genuine open minute* as a
      live base partial to higher aggregators (guarding against re-sending the
      just-closed minute). This prevents double-count on volume previews.
    - NEW: We **freeze closed candles per closure timestamp** so that each
      CandleClosure reflects the correct closed 1m/5m/etc at *that* event time,
      even if later rows arrive before we emit.
    - NEW: We **always emit a closure snapshot** on every ingested row (CSV or
      poll), even if no candle finalized, so the caller gets up-to-date open
      previews after each sync.
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

        # For each aggregator closure, we keep:
        #   latest_closed_candles: last known closed for each tf (for snapshots)
        #   _closed_by_ts: per-closure-timestamp frozen closed candles for that moment
        self.latest_closed_candles: Dict[str, Tuple[str, int, float, float, float, float, float]] = {}
        self._closed_by_ts: Dict[int, Dict[str, Tuple[str, int, float, float, float, float, float]]] = {}

        # Track which closure timestamps need to be emitted
        self.pending_closures: Dict[int, set] = {}

        self._all_timeframes = [self.base_tf] + higher_tfs

        # Ensure we only produce final partial closure once
        self._final_partial_emitted = False

    def record_closure(self, aggregator_closure_ts, tf, label_ts, o, h, l, c, vol):
        """
        Called by an aggregator to record a newly finalized candle.
        If tf == base_tf, that candle is also fed to each higher aggregator.
        """
        if tf == self.base_tf:
            # Forward *closed* base candle into higher aggregators
            for agg in self.higher_aggs:
                agg.on_base_candle_closed(aggregator_closure_ts, o, h, l, c, vol)

        # Update latest snapshot for quick "heartbeat" snapshots
        self.latest_closed_candles[tf] = (tf, label_ts, o, h, l, c, vol)

        # Freeze this closed candle under its *event timestamp*
        if aggregator_closure_ts not in self._closed_by_ts:
            self._closed_by_ts[aggregator_closure_ts] = {}
        self._closed_by_ts[aggregator_closure_ts][tf] = (tf, label_ts, o, h, l, c, vol)

        # Mark this timestamp as needing emission
        if aggregator_closure_ts not in self.pending_closures:
            self.pending_closures[aggregator_closure_ts] = set()
        self.pending_closures[aggregator_closure_ts].add(tf)

    def on_subcandle(self, ts, o, h, l, c, v) -> List[CandleClosure]:
        """
        Called for each row of base timeframe data.
        - Ingest into the base aggregator (which may finalize previous minute)
        - Forward the *live* base snapshot to higher aggregators for real-time preview,
          but ONLY if the row corresponds to the *currently open* base minute
          (prevents re-sending the just-closed minute as "partial").
        - Build closures for any finalizations that occurred; if none finalized,
          still emit a "heartbeat" snapshot closure using this row's timestamp.
        """
        # Feed into base
        self.base_agg.on_base_csv_row(ts, o, h, l, c, v)

        # Only forward genuine open-minute partials to higher TFs
        if self.base_agg.open_ts is not None and int(ts) == int(self.base_agg.open_ts):
            for agg in self.higher_aggs:
                agg.on_base_partial(ts, o, h, l, c, v)

        # Build closures (and ensure at least one snapshot per row)
        return self._build_and_return_closures(snapshot_event_ts=ts)

    # -----------------------------
    # Internal: build closures
    # -----------------------------
    def _compose_closed_snapshot_for_ts(self, closure_ts: int) -> Dict[str, Candle]:
        """
        Build a dict of closed candles to represent the state *at or before* closure_ts.
        Prefer the frozen values that closed exactly at closure_ts; otherwise use the
        latest known closed for that timeframe whose label_ts <= closure_ts.
        """
        out: Dict[str, Candle] = {}

        # Start with those that *actually* closed at closure_ts
        frozen = self._closed_by_ts.get(closure_ts, {})

        # Fill for every timeframe we track
        for tf in self._all_timeframes:
            if tf in frozen:
                (tfid, lbl_ts, oo, hh, ll, cc, vv) = frozen[tf]
                out[tf] = Candle(tfid, lbl_ts, oo, hh, ll, cc, vv)
                continue

            # Else, use the most recent closed we know about, but only if it's not from the future
            rec = self.latest_closed_candles.get(tf)
            if rec is not None:
                (tfid, lbl_ts, oo, hh, ll, cc, vv) = rec
                if lbl_ts <= closure_ts:
                    out[tf] = Candle(tfid, lbl_ts, oo, hh, ll, cc, vv)

        return out

    def _build_snapshot_closure(self, snapshot_event_ts: Optional[int]) -> List[CandleClosure]:
        """
        Build a single "heartbeat" closure reflecting the latest closed values we know
        and the current open previews. Used when no candle finalized on this row/poll.
        """
        if snapshot_event_ts is None:
            return []

        # Closed: take whatever is latest (they are <= snapshot_event_ts in normal flow)
        closed_candles: Dict[str, Candle] = {}
        for tf, rec in self.latest_closed_candles.items():
            (tfid, lbl_ts, oo, hh, ll, cc, vv) = rec
            closed_candles[tf] = Candle(tfid, lbl_ts, oo, hh, ll, cc, vv)

        # Open previews from all aggregators (base + higher)
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
        """
        If there are pending finalized candles, emit one closure per closure timestamp,
        using the *frozen* closed values captured at that time (prevents the "03:40
        closure shows 03:41 data" race). Otherwise, emit a single snapshot closure
        (heartbeat) so the caller sees updated open previews after every sync/poll.
        """
        out: List[CandleClosure] = []

        if self.pending_closures:
            for closure_ts in sorted(self.pending_closures.keys()):
                closed_candles = self._compose_closed_snapshot_for_ts(closure_ts)

                open_candles: Dict[str, Candle] = {}
                # Gather current open (including seeds/previews) from each aggregator
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

            # We've emitted all pending closures; clear both trackers for those timestamps
            self.pending_closures.clear()
            # Preserve history for potential lookbacks if desired in future,
            # but we no longer need the per-ts freeze once emitted.
            self._closed_by_ts.clear()

            return out

        # No finalized candles this round => emit a heartbeat snapshot
        return self._build_snapshot_closure(snapshot_event_ts)

    # -----------------------------
    # Flush
    # -----------------------------
    def flush(self) -> List[CandleClosure]:
        """
        Force finalization when we're behind and emit one final partial snapshot.
        Also seed next opens immediately after any forced finalizations so the
        'open' section in logs appears as expected.
        """
        # Finalize base if behind and seed the next open
        if self.base_agg.open_ts is not None:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            if self.base_agg.open_ts + self.base_agg.tf_ms <= now_ms:
                if self.verbose:
                    print(f"{Fore.MAGENTA}[DEBUG]{Style.RESET_ALL} forcibly finalizing base aggregator partial.")
                closing_price = self.base_agg.close_px if self.base_agg.close_px is not None else self.base_agg.open_px
                self.base_agg._finalize_aggregator_candle(real_event_ts=self.base_agg.last_sub_ts)
                # Seed immediately
                if closing_price is not None and self.base_agg.current_boundary is not None:
                    self.base_agg._seed_next_open(self.base_agg.current_boundary + self.base_agg.tf_ms, closing_price)
                self.base_agg.current_boundary = (self.base_agg.current_boundary or 0) + self.base_agg.tf_ms
                self.base_agg.reset()

        # Finalize higher TFs that happen to have enough subs and seed next opens
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

        out: List[CandleClosure] = []

        # Emit any newly finalized closures (they will use frozen per-ts data)
        if self.pending_closures:
            closures = self._build_and_return_closures()
            out.extend(closures)

        # FINAL PARTIAL CLOSURE: include seeded/previews as open_candles
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
    We fill missing steps with zero-volume candles, then call `flush()` at the end
    to produce any final partial closure exactly once.

    Fixes:
    - After history is exhausted and we emit a final partial closure, we roll back
      one base interval so the first real row of the just-opened minute isn't skipped.
    - Polling remains quiet unless verbose; generator is rebuilt to read strictly
      greater-than last seen timestamp.
    """

    DEFAULT_POST_HISTORY_FILL_LIMIT: int = 10_000  # safety cap

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
        self._current_row = None
        self._last_ts = None
        self._last_close = None

        self.base_ms = TIMEFRAMES[self.cfg.base_timeframe]

        # Polling-interval resolution and one-time announcement toggle
        self.poll_interval_seconds: int = self._resolve_poll_interval_seconds()
        self.poll_interval_ms: int = max(MIN_POLL_INTERVAL_SECONDS, int(self.poll_interval_seconds)) * 1000
        self._polling_notice_emitted: bool = False
        self._final_flush_done: bool = False  # ensure we do the post-history flush only once

        # Track how many synthetic "final fill" candles we generated right before polling
        self._post_history_fill_count: int = 0
        self._replay_adjust_applied: bool = False

    def __iter__(self) -> Iterator[CandleClosure]:
        return self

    def __next__(self) -> CandleClosure:
        global aggregator_manager

        # Return buffered closures first
        if self._closed_buffer:
            return self._closed_buffer.pop(0)

        while True:
            if self._current_row:
                row_ts, o, h, l, c, v = self._current_row
                self._current_row = None
            else:
                try:
                    row_ts, o, h, l, c, v = next(self._candles_gen)
                except StopIteration:
                    # If an explicit end_ts was set, we truly end.
                    if self.cfg.end_ts is not None:
                        if not self._final_flush_done:
                            all_closures = aggregator_manager.flush()
                            self._closed_buffer.extend(all_closures)
                            self._final_flush_done = True
                            if self._closed_buffer:
                                return self._closed_buffer.pop(0)
                        raise StopIteration

                    # Live mode (no end_ts): switch to polling.
                    if not self._polling_notice_emitted:
                        self._announce_polling_mode()

                    # First time we exhaust history: fill to the current boundary and flush once.
                    if not self._final_flush_done:
                        self._post_history_fill_count = 0  # reset counter each transition
                        if self._last_ts is not None:
                            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                            current_minute_boundary = (now_ms // self.base_ms) * self.base_ms

                            # fill up to current_minute_boundary
                            dummy_price = self._last_close
                            next_ts = self._last_ts + self.base_ms

                            # safety cap to avoid runaway loops if timestamps are corrupted
                            steps = 0
                            while next_ts <= current_minute_boundary and steps < self.DEFAULT_POST_HISTORY_FILL_LIMIT:
                                if self.cfg.verbose:
                                    dt_str = datetime.fromtimestamp(next_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                                    print(f"{Fore.RED}[MISSING]{Style.RESET_ALL} (final fill) Candle for {next_ts} ({dt_str}) => generating fake candle (0 volume)")
                                closures = aggregator_manager.on_subcandle(
                                    next_ts, dummy_price, dummy_price, dummy_price, dummy_price, 0.0
                                )
                                self._closed_buffer.extend(closures)
                                self._last_ts = next_ts
                                self._last_close = dummy_price
                                next_ts += self.base_ms
                                self._post_history_fill_count += 1
                                steps += 1

                        # Final partial closure once
                        all_closures = aggregator_manager.flush()
                        self._closed_buffer.extend(all_closures)
                        self._final_flush_done = True

                        # CRITICAL FIX:
                        # If we just "opened" the next boundary via final-fill, make sure we will
                        # read that very boundary from disk on the next poll by *rolling back*
                        # our last_ts by exactly one interval. This avoids skipping the first real
                        # row for that boundary due to the 'non-increasing timestamp' guard.
                        if self._post_history_fill_count > 0 and not self._replay_adjust_applied and self._last_ts is not None:
                            old_last = self._last_ts
                            self._last_ts = self._last_ts - self.base_ms
                            self._replay_adjust_applied = True
                            if self.cfg.verbose:
                                print(f"{UPDATE} Adjusting replay window to include just-opened minute: "
                                      f"last_ts {old_last} -> {self._last_ts}")

                        if self._closed_buffer:
                            return self._closed_buffer.pop(0)

                    # From here on, we block/poll until new data appears.
                    self._poll_for_new_data()
                    # After polling, we rebuild the generator to read just-arrived rows.
                    continue

            # Initialize last seen timestamp
            if self._last_ts is None:
                self._last_ts = row_ts
                self._last_close = o
                if self.cfg.end_ts and row_ts > self.cfg.end_ts:
                    raise StopIteration

            # Detect and fill gaps between CSV rows
            if row_ts > self._last_ts + self.base_ms:
                missing_ts = self._last_ts + self.base_ms
                if self.cfg.end_ts and missing_ts > self.cfg.end_ts:
                    pending = aggregator_manager.flush()
                    if pending:
                        self._closed_buffer.extend(pending)
                        return self._closed_buffer.pop(0)
                if self.cfg.verbose:
                    dt_str = datetime.fromtimestamp(missing_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                    print(f"{Fore.RED}[MISSING]{Style.RESET_ALL} Candle missing at {missing_ts} ({dt_str}) => generating fake candle (0 volume)")
                dummy = self._last_close
                new_closures = aggregator_manager.on_subcandle(missing_ts, dummy, dummy, dummy, dummy, 0.0)
                self._closed_buffer.extend(new_closures)
                self._last_ts = missing_ts
                self._last_close = dummy
                self._current_row = (row_ts, o, h, l, c, v)
                if self._closed_buffer:
                    return self._closed_buffer.pop(0)
                continue

            # Enforce end date bound
            if self.cfg.end_ts and row_ts > self.cfg.end_ts:
                pending = aggregator_manager.flush()
                if pending:
                    self._closed_buffer.extend(pending)
                    return self._closed_buffer.pop(0)
                raise StopIteration

            # Skip non-increasing timestamps
            if row_ts <= self._last_ts:
                if self.cfg.verbose:
                    print(f"{WARNING} Skipping non-increasing timestamp: {row_ts}")
                continue

            # Normal candle: process and buffer any closures
            closures = aggregator_manager.on_subcandle(row_ts, o, h, l, c, v)
            self._closed_buffer.extend(closures)
            self._last_ts = row_ts
            self._last_close = c
            if self._closed_buffer:
                return self._closed_buffer.pop(0)

    # -----------------------------
    # Internal helpers
    # -----------------------------
    def _resolve_poll_interval_seconds(self) -> int:
        """
        Determine the polling interval to report when entering polling mode.
        Priority:
            1) ENV: CANDLES_SYNC_POLL_INTERVAL_SECS
            2) cfg.poll_interval_seconds (CLI flag)
            3) base timeframe duration in seconds
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

        return max(MIN_POLL_INTERVAL_SECONDS, TIMEFRAMES[self.cfg.base_timeframe] // 1000)

    def _announce_polling_mode(self) -> None:
        """
        Print a single update when we transition from file iteration to polling mode.
        """
        if self._polling_notice_emitted:
            return

        interval_s = self.poll_interval_seconds
        tf = self.cfg.base_timeframe
        print(
            f"{UPDATE} Historical files exhausted — entering polling mode. "
            f"Polling every {interval_s}s (base timeframe: {tf})."
        )
        self._polling_notice_emitted = True

    def _poll_for_new_data(self) -> None:
        """
        Sleep for the poll interval, sync latest candles, and point the generator
        to new rows strictly after the last-seen timestamp.
        """
        # Sleep first to align with “poll every N seconds”
        time.sleep(self.poll_interval_seconds)

        # Sync latest data; keep this quiet unless verbose requested at iterator level.
        ok = synchronize_candle_data(
            exchange=self.cfg.exchange,
            ticker=self.cfg.ticker,
            timeframe=self.cfg.base_timeframe,
            end_date_str=None,  # always pull up to latest
            polling=1,
            verbose=self.cfg.verbose  # honor CLI -v
        )
        if not ok and self.cfg.verbose:
            print(f"{WARNING} Poll sync failed; will retry on next interval.{Style.RESET_ALL}")

        # Rebuild generator to read rows with ts > last_ts
        start_from = self._last_ts if self._last_ts is not None else (self.cfg.start_ts or 0)
        self._candles_gen = self._load_candles_since(start_from)

    # -----------------------------
    # File/CSV helpers (unchanged logic)
    # -----------------------------
    def _file_day_range(self, filename: str) -> Tuple[int, int]:
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

        selected_files = []
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
        List CSV files whose day-range intersects [start_ts, +inf).
        """
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

        selected_files = []
        for f in raw_files:
            fullpath = os.path.join(self.data_path, f)
            f_start, f_end = self._file_day_range(f)
            if f_end < start_ts:
                continue
            selected_files.append(fullpath)
        return selected_files

    def _load_candles_from_disk(self) -> Iterator[Tuple[int, float, float, float, float, float]]:
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

    def _load_candles_since(self, start_exclusive_ts: int) -> Iterator[Tuple[int, float, float, float, float, float]]:
        """
        Load candles strictly after start_exclusive_ts from disk, covering
        any files that might have appeared/expanded since last read.
        """
        file_list = self._list_csv_files_since(start_exclusive_ts + 1)
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

                    if ts <= start_exclusive_ts:
                        continue
                    if self.cfg.end_ts and ts > self.cfg.end_ts:
                        return  # respect explicit end bound if ever set

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

    # Ensure the base TF is included
    if base_timeframe not in parsed_tfs:
        parsed_tfs.insert(0, base_timeframe)

    start_ts = parse_timestamp(start_date, True)
    end_ts = parse_timestamp(end_date, False)

    # ----------------------------------------------------------------------
    # FIX / IMPROVEMENT:
    # 1) Identify the highest timeframe.
    # 2) Compute 200 bars back from 'end_ts' in that timeframe.
    # 3) Align 'start_ts' to the boundary of that highest timeframe.
    # 4) Only do this if 'start_ts' was not supplied by the user.
    # ----------------------------------------------------------------------
    if start_ts is None:
        largest_tf = max(parsed_tfs, key=lambda x: TIMEFRAMES[x])
        largest_tf_ms = TIMEFRAMES[largest_tf]

        if end_ts is None:
            # If still no end_ts, assume 'now'.
            end_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
            if verbose:
                print(f"{INFO} No end date supplied. Using 'now' => {end_ts}.")

        # We go 200 candles back in the largest timeframe
        raw_start_ts = end_ts - (200 * largest_tf_ms)

        # Align to largest_tf boundary
        aligned_start_ts = (raw_start_ts // largest_tf_ms) * largest_tf_ms

        start_ts = aligned_start_ts

        if verbose:
            print(f"{INFO} No start date supplied. Using highest timeframe '{largest_tf}' ("
                  f"{largest_tf_ms} ms) and 200 bars => raw start = {raw_start_ts}, aligned to {aligned_start_ts}.")

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
