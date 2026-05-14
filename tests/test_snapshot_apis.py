"""Tests for AggregatorManager snapshot APIs.

Phase P1-4 of the test-coverage plan: pin preferred_snapshot_ts running-max
semantics and set_closure_source reflection in subsequent emissions.

Targets:
  - preferred_snapshot_ts()   line 909-922
  - set_closure_source()      line 924-926
  - build_snapshot_closure()  source reflection (P0-3 covered ts/filter/None)
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from candle_iterator.candle_iterator import (
    AggregatorManager,
    Candle,
    CandleClosureSource,
)


DAY_MS = 86_400_000
MON_APR_13 = int(datetime(2026, 4, 13, tzinfo=timezone.utc).timestamp() * 1000)


def _feed_base(mgr, bars):
    for ts, o, h, l, c, v in bars:
        mgr.base_agg.on_base_csv_row(ts, o, h, l, c, v)


# --------------------------------------------------------------------------- #
# preferred_snapshot_ts
# --------------------------------------------------------------------------- #


class TestPreferredSnapshotTs:
    def test_empty_manager_returns_none(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        assert mgr.preferred_snapshot_ts() is None

    def test_only_base_open_returns_base_ts(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        _feed_base(mgr, [(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)])
        assert mgr.preferred_snapshot_ts() == MON_APR_13

    def test_only_closed_returns_max_closed_ts(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        mgr.latest_closed_candles["1D"] = Candle("1D", 100, 1, 2, 0.5, 1.5, 10)
        # Clear base aggregator state (which gets set by the closure path)
        # Actually since we manually populated, base_agg is fresh
        assert mgr.preferred_snapshot_ts() == 100

    def test_max_across_closed_and_open(self):
        """When both closed and open exist, returns the running max."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        mgr.latest_closed_candles["1D"] = Candle("1D", 100, 1, 2, 0.5, 1.5, 10)
        _feed_base(mgr, [(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)])
        # MON_APR_13 (open ts) is much larger than 100 (closed)
        assert mgr.preferred_snapshot_ts() == MON_APR_13

    def test_max_across_multiple_higher_aggs(self):
        """Multiple higher aggs with different open_ts → max wins."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D", "1W"])
        three_d, one_w = mgr.higher_aggs
        # Manually set distinct open_ts on each
        three_d.open_ts = MON_APR_13
        three_d.open_px = three_d.high_px = three_d.low_px = three_d.close_px = 100.0
        three_d.volume = 1000.0
        three_d.sub_count = 1
        three_d.last_sub_ts = MON_APR_13
        one_w.open_ts = MON_APR_13 + 5 * DAY_MS  # larger
        one_w.open_px = one_w.high_px = one_w.low_px = one_w.close_px = 100.0
        one_w.volume = 1000.0
        one_w.sub_count = 1
        one_w.last_sub_ts = MON_APR_13 + 5 * DAY_MS
        assert mgr.preferred_snapshot_ts() == MON_APR_13 + 5 * DAY_MS


# --------------------------------------------------------------------------- #
# set_closure_source
# --------------------------------------------------------------------------- #


class TestSetClosureSource:
    def test_initial_source_is_historical(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        assert mgr.closure_source == CandleClosureSource.HISTORICAL

    def test_set_to_live(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        mgr.set_closure_source(CandleClosureSource.LIVE)
        assert mgr.closure_source == CandleClosureSource.LIVE

    def test_round_trip_back_to_historical(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        mgr.set_closure_source(CandleClosureSource.LIVE)
        mgr.set_closure_source(CandleClosureSource.HISTORICAL)
        assert mgr.closure_source == CandleClosureSource.HISTORICAL


# --------------------------------------------------------------------------- #
# Source reflection in subsequent emissions
# --------------------------------------------------------------------------- #


class TestSourceReflectionInEmissions:
    def test_build_snapshot_closure_carries_historical(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        _feed_base(mgr, [(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)])
        cc = mgr.build_snapshot_closure(event_ts=MON_APR_13)
        assert cc is not None
        assert cc.source == CandleClosureSource.HISTORICAL

    def test_build_snapshot_closure_carries_live_after_switch(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        _feed_base(mgr, [(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)])
        mgr.set_closure_source(CandleClosureSource.LIVE)
        cc = mgr.build_snapshot_closure(event_ts=MON_APR_13)
        assert cc is not None
        assert cc.source == CandleClosureSource.LIVE

    def test_make_closure_pending_path_carries_live(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        # Drive 2 bars so bar-0 finalizes → pending closure
        _feed_base(mgr, [
            (MON_APR_13,           100.0, 105.0, 95.0, 102.0, 1000.0),
            (MON_APR_13 + DAY_MS,  102.0, 110.0, 100.0, 108.0, 1500.0),
        ])
        mgr.set_closure_source(CandleClosureSource.LIVE)
        out = mgr._make_closure(event_ts=None, include_pending=True)
        # All emitted closures from this call carry LIVE
        for cc in out:
            assert cc.source == CandleClosureSource.LIVE

    def test_switch_does_not_retroactively_change_prior_emissions(self):
        """Source switch only applies to closures emitted AFTER the switch."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        _feed_base(mgr, [(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)])
        # Emit BEFORE the switch
        cc_hist = mgr.build_snapshot_closure(event_ts=MON_APR_13)
        # Now switch to LIVE
        mgr.set_closure_source(CandleClosureSource.LIVE)
        # The PRIOR emission still has HISTORICAL
        assert cc_hist.source == CandleClosureSource.HISTORICAL
        # New emission has LIVE
        cc_live = mgr.build_snapshot_closure(event_ts=MON_APR_13)
        assert cc_live.source == CandleClosureSource.LIVE
