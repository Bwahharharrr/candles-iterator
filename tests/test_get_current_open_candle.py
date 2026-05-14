"""Tests for CandleAggregator.get_current_open_candle().

Phase P1-2 of the test-coverage plan: pin the 5 return paths.

Target: candle_iterator.py:784-836.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from candle_iterator.candle_iterator import (
    AggregatorManager,
    Candle,
    CandleAggregator,
    Partial,
)


DAY_MS = 86_400_000
MON_APR_13 = int(datetime(2026, 4, 13, tzinfo=timezone.utc).timestamp() * 1000)


def _mk_higher_agg(timeframe="3D", anchor_offset=0):
    """A higher aggregator with a no-op manager."""
    mgr = AggregatorManager(base_tf="1D", higher_tfs=[timeframe])
    agg = [a for a in mgr.higher_aggs if a.tf == timeframe][0]
    return mgr, agg


# --------------------------------------------------------------------------- #
# Path 1: None when all empty
# --------------------------------------------------------------------------- #


class TestNoneWhenEmpty:
    def test_fresh_agg_returns_none(self):
        _, agg = _mk_higher_agg()
        assert agg.get_current_open_candle() is None

    def test_base_agg_fresh_returns_none(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        assert mgr.base_agg.get_current_open_candle() is None


# --------------------------------------------------------------------------- #
# Path 2: Committed-only
# --------------------------------------------------------------------------- #


class TestCommittedOnly:
    def test_committed_state_returns_candle(self):
        _, agg = _mk_higher_agg()
        # Manually set committed state (simulates having ingested 1 sub)
        agg.open_ts = MON_APR_13
        agg.open_px = 100.0
        agg.high_px = 105.0
        agg.low_px = 95.0
        agg.close_px = 102.0
        agg.volume = 1500.0
        agg.sub_count = 1
        agg.current_boundary = MON_APR_13
        agg.last_sub_ts = MON_APR_13
        candle = agg.get_current_open_candle()
        assert candle is not None
        assert candle.timestamp == MON_APR_13
        assert candle.open == 100.0
        assert candle.high == 105.0
        assert candle.low == 95.0
        assert candle.close == 102.0
        assert candle.volume == 1500.0
        assert candle.timeframe == "3D"


# --------------------------------------------------------------------------- #
# Path 3: Seed-only (after legitimate close, no new sub yet)
# --------------------------------------------------------------------------- #


class TestSeedOnly:
    def test_seed_returns_synthetic_zero_volume_candle(self):
        _, agg = _mk_higher_agg()
        agg._seed_next_open_ts = MON_APR_13 + 3 * DAY_MS
        agg._seed_next_open_price = 102.0
        candle = agg.get_current_open_candle()
        assert candle is not None
        assert candle.timestamp == MON_APR_13 + 3 * DAY_MS
        # OHLC all equal the seed price
        assert candle.open == 102.0
        assert candle.high == 102.0
        assert candle.low == 102.0
        assert candle.close == 102.0
        assert candle.volume == 0.0

    def test_committed_state_shadows_seed(self):
        """When BOTH committed (open_ts set) and seed are present, committed
        wins — get_current_open_candle uses the open_ts path.
        """
        _, agg = _mk_higher_agg()
        agg.open_ts = MON_APR_13
        agg.open_px = 100.0
        agg.high_px = 105.0
        agg.low_px = 95.0
        agg.close_px = 102.0
        agg.volume = 1500.0
        agg.sub_count = 1
        agg.last_sub_ts = MON_APR_13
        # Also set seed (stale; from a prior cycle)
        agg._seed_next_open_ts = MON_APR_13 + 3 * DAY_MS
        agg._seed_next_open_price = 99.0
        candle = agg.get_current_open_candle()
        # Committed takes precedence
        assert candle.timestamp == MON_APR_13
        assert candle.open == 100.0


# --------------------------------------------------------------------------- #
# Path 4: Committed + live partial overlay
# --------------------------------------------------------------------------- #


class TestCommittedPlusLiveOverlay:
    def test_overlay_merges_high_low_close_volume(self):
        _, agg = _mk_higher_agg()
        # Committed
        agg.open_ts = MON_APR_13
        agg.open_px = 100.0
        agg.high_px = 105.0
        agg.low_px = 98.0
        agg.close_px = 102.0
        agg.volume = 1500.0
        agg.sub_count = 1
        agg.current_boundary = MON_APR_13
        agg.last_sub_ts = MON_APR_13
        # Live partial inside the same boundary (MON_APR_13 to MON+3*DAY_MS)
        agg._live_partial = Partial(
            ts=MON_APR_13 + DAY_MS, o=102.0, h=110.0, l=96.0, c=109.0, v=200.0,
        )
        candle = agg.get_current_open_candle()
        # Overlay: max-h, min-l, latest-c, sum-vol; open stays committed
        assert candle.timestamp == MON_APR_13  # committed boundary
        assert candle.open == 100.0  # committed open unchanged
        assert candle.high == 110.0  # max(105, 110)
        assert candle.low == 96.0    # min(98, 96)
        assert candle.close == 109.0  # partial close
        assert candle.volume == 1700.0  # 1500 + 200

    def test_overlay_skipped_when_partial_ts_matches_last_sub_ts(self):
        """Double-count guard: if `_live_partial.ts == last_sub_ts`, the
        partial corresponds to a SUB already ingested → overlay skipped.
        """
        _, agg = _mk_higher_agg()
        agg.open_ts = MON_APR_13
        agg.open_px = 100.0
        agg.high_px = 105.0
        agg.low_px = 98.0
        agg.close_px = 102.0
        agg.volume = 1500.0
        agg.sub_count = 1
        agg.current_boundary = MON_APR_13
        agg.last_sub_ts = MON_APR_13 + DAY_MS  # same as partial ts below
        agg._live_partial = Partial(
            ts=MON_APR_13 + DAY_MS, o=102.0, h=110.0, l=96.0, c=109.0, v=200.0,
        )
        candle = agg.get_current_open_candle()
        # Overlay skipped — committed values intact
        assert candle.high == 105.0
        assert candle.low == 98.0
        assert candle.close == 102.0
        assert candle.volume == 1500.0

    def test_overlay_skipped_when_partial_boundary_mismatch(self):
        """If the live partial's boundary differs from the committed boundary,
        overlay is skipped (line 817: `if boundary_ts == lp_boundary`).
        """
        _, agg = _mk_higher_agg()
        agg.open_ts = MON_APR_13
        agg.open_px = 100.0
        agg.high_px = 105.0
        agg.low_px = 98.0
        agg.close_px = 102.0
        agg.volume = 1500.0
        agg.sub_count = 1
        agg.current_boundary = MON_APR_13
        agg.last_sub_ts = MON_APR_13
        # Live partial at a ts that aligns to a DIFFERENT 3D boundary
        agg._live_partial = Partial(
            ts=MON_APR_13 + 10 * DAY_MS, o=999.0, h=999.0, l=999.0, c=999.0, v=999.0,
        )
        candle = agg.get_current_open_candle()
        # Overlay skipped because the partial's boundary != committed boundary
        assert candle.high == 105.0
        assert candle.volume == 1500.0


# --------------------------------------------------------------------------- #
# Path 5: Seed + live partial overlay
# --------------------------------------------------------------------------- #


class TestSeedPlusLiveOverlay:
    def test_seed_with_matching_partial_uses_partial_data(self):
        """When open_ts is None but seed is set and live_partial's boundary
        matches the seed boundary, the partial's values populate the candle.
        """
        _, agg = _mk_higher_agg()
        seed_ts = MON_APR_13 + 3 * DAY_MS
        agg._seed_next_open_ts = seed_ts
        agg._seed_next_open_price = 102.0
        # Live partial inside the SEED boundary
        agg._live_partial = Partial(
            ts=seed_ts + DAY_MS, o=103.0, h=108.0, l=99.0, c=107.0, v=500.0,
        )
        candle = agg.get_current_open_candle()
        # When sub_count == 0 (path 5), open from partial
        assert candle.timestamp == seed_ts
        assert candle.open == 103.0
        assert candle.high == 108.0
        assert candle.low == 99.0
        assert candle.close == 107.0
        assert candle.volume == 500.0

    def test_seed_with_mismatched_partial_falls_back_to_seed_only(self):
        """Seed + live partial whose boundary doesn't match the seed → use
        seed-only (path 3 shape).
        """
        _, agg = _mk_higher_agg()
        agg._seed_next_open_ts = MON_APR_13 + 3 * DAY_MS
        agg._seed_next_open_price = 102.0
        # Live partial far in the future
        agg._live_partial = Partial(
            ts=MON_APR_13 + 20 * DAY_MS, o=999.0, h=999.0, l=999.0, c=999.0, v=999.0,
        )
        candle = agg.get_current_open_candle()
        # Falls back to seed values
        assert candle.timestamp == MON_APR_13 + 3 * DAY_MS
        assert candle.open == 102.0
        assert candle.high == 102.0
        assert candle.volume == 0.0


# --------------------------------------------------------------------------- #
# Defensive: base aggregator never uses live overlay
# --------------------------------------------------------------------------- #


class TestBaseAggregatorIgnoresLivePartial:
    def test_base_agg_does_not_overlay_live_partial(self):
        """Lines 808-823: the overlay block is guarded by `not self.is_base`.
        For the base aggregator, even if `_live_partial` is somehow set, the
        committed Candle is returned untouched.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        agg = mgr.base_agg
        agg.open_ts = MON_APR_13
        agg.open_px = 100.0
        agg.high_px = 105.0
        agg.low_px = 95.0
        agg.close_px = 102.0
        agg.volume = 1000.0
        agg.sub_count = 1
        agg.last_sub_ts = MON_APR_13
        # Even if live_partial is set (which should never happen for base),
        # the overlay must NOT fire
        agg._live_partial = Partial(
            ts=MON_APR_13, o=999.0, h=999.0, l=999.0, c=999.0, v=999.0,
        )
        candle = agg.get_current_open_candle()
        assert candle.high == 105.0
        assert candle.volume == 1000.0


# --------------------------------------------------------------------------- #
# Timestamp identity invariant
# --------------------------------------------------------------------------- #


class TestTimestampIdentity:
    def test_returned_ts_is_open_or_seed_never_live_partial(self):
        """The returned candle.timestamp must be either committed open_ts
        or seed_next_open_ts — never the live_partial.ts.
        """
        _, agg = _mk_higher_agg()
        agg.open_ts = MON_APR_13
        agg.open_px = 100.0
        agg.high_px = 105.0
        agg.low_px = 95.0
        agg.close_px = 102.0
        agg.volume = 1000.0
        agg.sub_count = 1
        agg.last_sub_ts = MON_APR_13
        agg._live_partial = Partial(
            ts=MON_APR_13 + DAY_MS, o=102.0, h=110.0, l=96.0, c=109.0, v=200.0,
        )
        candle = agg.get_current_open_candle()
        # Returned ts == committed open_ts, NOT live_partial.ts
        assert candle.timestamp == MON_APR_13
        assert candle.timestamp != MON_APR_13 + DAY_MS
