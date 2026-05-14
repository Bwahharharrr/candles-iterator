"""Tests for CandleAggregator._consume_subcandle.

Phase P0-4 of the test-coverage plan: pin the multi-interval boundary-jump
state machine and zero-candle emission semantics.

Targets:
  - CandleAggregator._consume_subcandle   line 676-733
  - _finalize_aggregator_candle           line 764-782
  - _record_zero_candle                   line 755-762
  - _seed_next_open                       line 751-753
  - _clear_open_state                     line 738-749
  - _align_to_boundary                    line 622-631 (only used as input)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, NamedTuple

import pytest

from candle_iterator.candle_iterator import AggregatorManager, CandleAggregator


DAY_MS = 86_400_000
MIN_MS = 60_000
HOUR_MS = 3_600_000
MON_APR_13 = int(datetime(2026, 4, 13, tzinfo=timezone.utc).timestamp() * 1000)


# --------------------------------------------------------------------------- #
# Spy helper: monkeypatch manager.record_closure to capture every call
# --------------------------------------------------------------------------- #


class RecCall(NamedTuple):
    aggregator_closure_ts: int
    tf: str
    label_ts: int
    o: float
    h: float
    l: float
    c: float
    vol: float


def _install_spy(mgr) -> List[RecCall]:
    """Replace mgr.record_closure with a logging passthrough; return the log."""
    log: List[RecCall] = []
    original = mgr.record_closure

    def spy(aggregator_closure_ts, tf, label_ts, o, h, l, c, vol):
        log.append(RecCall(aggregator_closure_ts, tf, label_ts, o, h, l, c, vol))
        original(aggregator_closure_ts, tf, label_ts, o, h, l, c, vol)

    mgr.record_closure = spy
    return log


def _feed_csv(base_agg, *bars):
    for ts, o, h, l, c, v in bars:
        base_agg.on_base_csv_row(ts, o, h, l, c, v)


# --------------------------------------------------------------------------- #
# A. Base aggregator (is_base=True, sub_factor=1)
# --------------------------------------------------------------------------- #


class TestBaseSingleAndJumps:
    def test_first_bar_initializes_boundary_no_closure(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        log = _install_spy(mgr)
        _feed_csv(mgr.base_agg, (MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0))
        assert log == []  # no closure on first bar
        assert mgr.base_agg.current_boundary == MON_APR_13
        assert mgr.base_agg.sub_count == 1
        assert mgr.base_agg.last_sub_ts == MON_APR_13

    def test_two_consecutive_bars_finalize_first(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        log = _install_spy(mgr)
        _feed_csv(
            mgr.base_agg,
            (MON_APR_13,              100.0, 105.0, 95.0, 102.0, 1000.0),
            (MON_APR_13 + DAY_MS,     102.0, 110.0, 100.0, 108.0, 1500.0),
        )
        # Exactly one finalize call for bar-1 (label_ts==MON_APR_13)
        assert len(log) == 1
        call = log[0]
        assert call.tf == "1D"
        assert call.label_ts == MON_APR_13
        # real_event_ts == last_sub_ts of the finalized bar == MON_APR_13
        assert call.aggregator_closure_ts == MON_APR_13
        assert call.o == 100.0
        assert call.h == 105.0
        assert call.l == 95.0
        assert call.c == 102.0
        assert call.vol == 1000.0
        # Seed-next-open recorded at the NEXT boundary with the prior close
        assert mgr.base_agg._seed_next_open_ts is not None
        # After ingesting bar-2, open_ts == its boundary; seed is shadowed
        assert mgr.base_agg.open_ts == MON_APR_13 + DAY_MS
        assert mgr.base_agg.sub_count == 1

    def test_three_period_jump_with_two_zero_candles(self):
        """Feed bar at ts0, then bar at ts0 + 3*DAY_MS. While-loop runs 3x:
           iter 1: finalize bar-0 at boundary=ts0.
           iter 2: zero-candle at boundary=ts0+DAY_MS.
           iter 3: zero-candle at boundary=ts0+2*DAY_MS.
        Then ingest the new bar at boundary=ts0+3*DAY_MS."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        log = _install_spy(mgr)
        _feed_csv(
            mgr.base_agg,
            (MON_APR_13,                   100.0, 105.0, 95.0, 102.0, 1000.0),
            (MON_APR_13 + 3 * DAY_MS,      200.0, 210.0, 190.0, 205.0, 500.0),
        )
        assert len(log) == 3
        # Call 1: real finalize of bar-0
        assert log[0].label_ts == MON_APR_13
        assert log[0].o == 100.0
        assert log[0].c == 102.0
        assert log[0].vol == 1000.0
        # Call 2 + 3: zero candles, all-zero OHLCV
        for zc in (log[1], log[2]):
            assert zc.tf == "1D"
            assert zc.o == 0.0 and zc.h == 0.0 and zc.l == 0.0 and zc.c == 0.0
            assert zc.vol == 0.0
        # Zero-candle label_ts values are the empty boundaries
        assert log[1].label_ts == MON_APR_13 + DAY_MS
        assert log[2].label_ts == MON_APR_13 + 2 * DAY_MS
        # Closure stream ordering: finalize-with-data BEFORE zero-candles
        assert log[0].label_ts < log[1].label_ts < log[2].label_ts
        # Final state: new bar opened at the correct boundary
        assert mgr.base_agg.current_boundary == MON_APR_13 + 3 * DAY_MS
        assert mgr.base_agg.sub_count == 1

    def test_two_period_jump_emits_one_zero_candle(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        log = _install_spy(mgr)
        _feed_csv(
            mgr.base_agg,
            (MON_APR_13,               100.0, 105.0, 95.0, 102.0, 1000.0),
            (MON_APR_13 + 2 * DAY_MS,  200.0, 210.0, 190.0, 205.0, 500.0),
        )
        assert len(log) == 2
        assert log[0].label_ts == MON_APR_13          # finalize bar-0
        assert log[1].label_ts == MON_APR_13 + DAY_MS  # zero
        assert log[1].o == log[1].h == log[1].l == log[1].c == log[1].vol == 0.0

    def test_seed_next_open_set_to_next_boundary_with_prior_close(self):
        """After a SINGLE finalize (no jump beyond), inspect the seed state."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        _feed_csv(
            mgr.base_agg,
            (MON_APR_13,           100.0, 105.0, 95.0, 102.0, 1000.0),
            (MON_APR_13 + DAY_MS,  102.0, 110.0, 100.0, 108.0, 1500.0),
        )
        # The seed was set during the finalize step. After ingesting bar-2,
        # open_ts == MON_APR_13+DAY_MS, but the seed value still reflects what
        # was seeded at finalize time: next boundary after MON_APR_13.
        assert mgr.base_agg._seed_next_open_ts == MON_APR_13 + DAY_MS
        assert mgr.base_agg._seed_next_open_price == 102.0  # bar-0's close

    def test_misaligned_input_ts_aligns_to_boundary(self):
        """A base_ts that's mid-bar must align: current_boundary = floor.
        For 1D and base_ts at 09:30 UTC, boundary = 00:00 of that day."""
        misaligned = MON_APR_13 + 9 * HOUR_MS + 30 * MIN_MS
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        _feed_csv(mgr.base_agg, (misaligned, 100.0, 105.0, 95.0, 102.0, 1000.0))
        assert mgr.base_agg.current_boundary == MON_APR_13  # aligned floor
        assert mgr.base_agg.last_sub_ts == misaligned       # raw ts retained

    def test_exact_boundary_advances_cleanly(self):
        """base_ts == current_boundary + n*tf_ms should finalize n-1 buckets
        (or n if including zero-candle iterations) and ingest at exact boundary.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        log = _install_spy(mgr)
        _feed_csv(
            mgr.base_agg,
            (MON_APR_13,              100.0, 105.0, 95.0, 102.0, 1000.0),
            (MON_APR_13 + DAY_MS,     102.0, 110.0, 100.0, 108.0, 1500.0),
        )
        assert mgr.base_agg.current_boundary == MON_APR_13 + DAY_MS
        assert len(log) == 1  # exactly one finalize, no zero-candles


# --------------------------------------------------------------------------- #
# B. Higher aggregator (is_base=False, sub_factor>1)
# --------------------------------------------------------------------------- #


class TestHigherTfAccumulation:
    def test_partial_sub_factor_does_not_finalize(self):
        """Feed 2 of 3 daily bars to a 3D higher agg → no 3D closure."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        log = _install_spy(mgr)
        _feed_csv(
            mgr.base_agg,
            (MON_APR_13,             100.0, 105.0, 95.0, 102.0, 1000.0),
            (MON_APR_13 + DAY_MS,    102.0, 110.0, 100.0, 108.0, 1500.0),
        )
        # Only the 1D finalize, no 3D
        assert all(call.tf != "3D" for call in log)

    def test_four_bars_completes_3d_sub_factor(self):
        """Cascade arithmetic: 3D needs sub_factor=3 cascaded subs. The
        cascade fires once PER FINALIZED BASE BAR, and base finalizes when
        the NEXT bar arrives. So N base bars → N-1 finalizes → N-1 cascaded
        subs to 3D. For 3 subs: feed 4 base bars.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        log = _install_spy(mgr)
        _feed_csv(
            mgr.base_agg,
            (MON_APR_13,                100.0, 110.0, 95.0, 108.0, 1000.0),
            (MON_APR_13 + DAY_MS,       108.0, 115.0, 90.0, 112.0, 2000.0),
            (MON_APR_13 + 2 * DAY_MS,   112.0, 120.0, 100.0, 118.0, 1500.0),
            (MON_APR_13 + 3 * DAY_MS,   118.0, 122.0, 116.0, 120.0,  800.0),
        )
        # 3D should appear exactly once: bars 0,1,2 aggregated; finalized
        # when bar-3's ingestion cascades bar-2's close into 3D
        three_d_calls = [c for c in log if c.tf == "3D"]
        assert len(three_d_calls) == 1
        d3 = three_d_calls[0]
        assert d3.o == 100.0       # bar-0's open
        assert d3.h == 120.0       # max(110,115,120)
        assert d3.l == 90.0        # min(95,90,100)
        assert d3.c == 118.0       # bar-2's close
        assert d3.vol == 4500.0    # 1000+2000+1500
        assert d3.label_ts == MON_APR_13
        # real_event_ts == bar-2's ts (the bar that closed and triggered cascade)
        assert d3.aggregator_closure_ts == MON_APR_13 + 2 * DAY_MS


class TestHigherTfMultiBoundaryJump:
    """Test the higher-TF jump state machine via DIRECT _consume_subcandle calls.

    Cascade-driven tests are deliberately avoided here because base zero-candle
    propagation interferes with higher-TF boundary math (a base zero-candle
    cascades as a zero subcandle to the higher TF, polluting OHLCV and
    advancing sub_count). To isolate the higher-TF jump logic, drive the
    higher agg directly.
    """

    def test_data_jump_finalize_then_zero_candles(self):
        """Direct higher-TF jump with prior data: prime sub_count=1, then
        jump by N tf_ms (N>=2). Iter 1 finalizes (data), iters 2..N emit
        zero-candles.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        three_d = [a for a in mgr.higher_aggs if a.tf == "3D"][0]
        log = _install_spy(mgr)
        # Prime: 1 subcandle at base_ts=MON_APR_13 (sets current_boundary, sub_count=1)
        three_d._consume_subcandle(
            MON_APR_13, 100.0, 110.0, 95.0, 105.0, 1000.0, volume_delta=True,
        )
        assert three_d.current_boundary == MON_APR_13
        assert three_d.sub_count == 1
        # Now jump: base_ts = MON + 10*DAY → loop iterates while base_ts >= cb + 3*DAY
        # Iter 1: 10D >= 3D → finalize at MON (sub_count=1>0). Advance to MON+3D.
        # Iter 2: 10D >= MON+3D+3D=MON+6D → zero at MON+3D. Advance to MON+6D.
        # Iter 3: 10D >= MON+6D+3D=MON+9D → zero at MON+6D. Advance to MON+9D.
        # Iter 4: 10D >= MON+9D+3D=MON+12D? No. Exit loop.
        # Ingest new bar at boundary=MON+9D.
        three_d._consume_subcandle(
            MON_APR_13 + 10 * DAY_MS, 200.0, 210.0, 190.0, 205.0, 500.0,
            volume_delta=True,
        )
        three_d_calls = [c for c in log if c.tf == "3D"]
        assert len(three_d_calls) == 3
        # Call 1: data finalize
        assert three_d_calls[0].label_ts == MON_APR_13
        assert three_d_calls[0].o == 100.0
        assert three_d_calls[0].c == 105.0
        assert three_d_calls[0].vol == 1000.0
        # Calls 2,3: zero candles
        for zc in three_d_calls[1:]:
            assert zc.o == 0.0 and zc.h == 0.0 and zc.l == 0.0 and zc.c == 0.0
            assert zc.vol == 0.0
        assert three_d_calls[1].label_ts == MON_APR_13 + 3 * DAY_MS
        assert three_d_calls[2].label_ts == MON_APR_13 + 6 * DAY_MS
        # Final state: current_boundary lands at MON + 9*DAY (aligned floor of MON+10D for 3D)
        assert three_d.current_boundary == MON_APR_13 + 9 * DAY_MS
        # And the new bar is the first sub of the new bucket
        assert three_d.sub_count == 1
        assert three_d.open_px == 200.0

    def test_no_finalize_when_initial_sub_count_zero_before_jump(self):
        """When higher agg has sub_count=0 entering the jump, ALL while-loop
        iterations emit zero-candles (no data finalize).
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        three_d = [a for a in mgr.higher_aggs if a.tf == "3D"][0]
        log = _install_spy(mgr)
        # Prime current_boundary without contributing to sub_count
        three_d.current_boundary = MON_APR_13
        # Now feed a base_ts that triggers a 2-iter jump
        three_d._consume_subcandle(
            MON_APR_13 + 6 * DAY_MS, 100.0, 105.0, 95.0, 102.0, 1000.0,
            volume_delta=True,
        )
        # Expected: iter 1 zero at MON, iter 2 zero at MON+3D. Then ingest.
        three_d_calls = [c for c in log if c.tf == "3D"]
        assert len(three_d_calls) == 2
        for zc in three_d_calls:
            assert zc.o == 0.0 and zc.h == 0.0 and zc.l == 0.0 and zc.c == 0.0
        assert three_d_calls[0].label_ts == MON_APR_13
        assert three_d_calls[1].label_ts == MON_APR_13 + 3 * DAY_MS


class TestHigherTfCompletionTiming:
    def test_finalize_real_event_ts_equals_triggering_base_ts(self):
        """When higher TF finalizes at sub_count==sub_factor in the same
        _consume_subcandle call (line 727-733), real_event_ts == base_ts.

        For 3D to finalize via cascade: N=4 base bars → 3 cascaded subs.
        The 4th bar's ingestion cascades bar-2's finalize at base_ts=MON+2D.
        That cascade fills 3D's 3rd sub → finalize at real_event_ts=MON+2D.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        log = _install_spy(mgr)
        _feed_csv(
            mgr.base_agg,
            (MON_APR_13,                100.0, 105.0, 95.0, 102.0, 1000.0),
            (MON_APR_13 + DAY_MS,       102.0, 110.0, 100.0, 108.0, 1500.0),
            (MON_APR_13 + 2 * DAY_MS,   108.0, 112.0, 106.0, 110.0,  800.0),
            (MON_APR_13 + 3 * DAY_MS,   110.0, 113.0, 109.0, 111.0,  600.0),
        )
        three_d = [c for c in log if c.tf == "3D"][0]
        # The cascade that finalized 3D was triggered by bar-2's finalize
        assert three_d.aggregator_closure_ts == MON_APR_13 + 2 * DAY_MS

    def test_two_back_to_back_higher_completions(self):
        """For 2 complete 3D candles, need 6 cascaded subs → 7 base bars.
        First 3D contains bars 0,1,2; second contains bars 3,4,5.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        log = _install_spy(mgr)
        bars = []
        for i in range(7):
            base = MON_APR_13 + i * DAY_MS
            o = 100.0 + i
            h = o + 5
            l = o - 5
            c = o + 2
            v = 1000.0 + i * 100
            bars.append((base, o, h, l, c, v))
        _feed_csv(mgr.base_agg, *bars)
        three_d_calls = [c for c in log if c.tf == "3D"]
        assert len(three_d_calls) == 2
        first, second = three_d_calls
        assert first.label_ts == MON_APR_13                  # buckets[0]
        assert second.label_ts == MON_APR_13 + 3 * DAY_MS    # buckets[1]
        # First 3D: bars 0,1,2 → open=100, h=max(105,106,107)=107,
        # l=min(95,96,97)=95, close=104, vol=3300
        assert first.o == 100.0
        assert first.h == 107.0
        assert first.l == 95.0
        assert first.c == 104.0
        assert first.vol == 3300.0
        # First 3D triggered when bar-3 ingests bar-2 finalize → ts=MON+2D
        assert first.aggregator_closure_ts == MON_APR_13 + 2 * DAY_MS
        # Second 3D: bars 3,4,5 → open=103, h=max(108,109,110)=110,
        # l=min(98,99,100)=98, close=107, vol=4200
        assert second.o == 103.0
        assert second.h == 110.0
        assert second.l == 98.0
        assert second.c == 107.0
        assert second.vol == 4200.0
        # Second 3D triggered when bar-6 ingests bar-5 finalize → ts=MON+5D
        assert second.aggregator_closure_ts == MON_APR_13 + 5 * DAY_MS


# --------------------------------------------------------------------------- #
# Integrated cascade contract: base zero-candle propagates as zero subcandle
# --------------------------------------------------------------------------- #


class TestIntegratedCascadeContract:
    """Document the integrated cascade behavior: when the base emits a
    zero-candle for an empty boundary, that zero-candle propagates to higher
    TFs as a zero subcandle (via record_closure → on_base_candle_closed),
    polluting the higher-TF OHLCV (low becomes 0, close becomes 0, sub_count
    advances). This is existing behavior — the test pins it so a refactor
    cannot silently change cascade semantics.
    """

    def test_base_zero_candle_propagates_to_higher_as_zero_sub(self):
        """Two base bars with a 2-day gap between them. The middle day emits
        a base zero-candle. Verify the 3D higher TF received a zero sub via
        the cascade (3D's low_px will be 0 after that sub).
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        three_d = [a for a in mgr.higher_aggs if a.tf == "3D"][0]
        log = _install_spy(mgr)
        _feed_csv(
            mgr.base_agg,
            (MON_APR_13,                  100.0, 110.0,  95.0, 105.0, 1000.0),
            (MON_APR_13 + 2 * DAY_MS,     200.0, 210.0, 190.0, 205.0,  500.0),
        )
        # Base log should contain: bar-0 finalize, then zero-candle at MON+DAY
        base_calls = [c for c in log if c.tf == "1D"]
        assert len(base_calls) == 2
        assert base_calls[0].label_ts == MON_APR_13
        assert base_calls[0].o == 100.0
        assert base_calls[1].label_ts == MON_APR_13 + DAY_MS
        assert base_calls[1].o == 0.0  # zero-candle
        # 3D received both as cascaded subs. Its sub_count is now 2 (sub_factor=3
        # not yet reached), so no 3D finalize.
        three_d_calls = [c for c in log if c.tf == "3D"]
        assert three_d_calls == []
        assert three_d.sub_count == 2
        # OHLCV after the cascade: bar-0 contributed (100,110,95,105), then
        # zero sub contributed (0,0,0,0). Update: high=max(110,0)=110,
        # low=min(95,0)=0, close=0 (last sub's close).
        assert three_d.high_px == 110.0
        assert three_d.low_px == 0.0
        assert three_d.close_px == 0.0


# --------------------------------------------------------------------------- #
# E. Stale live partial scenarios
# --------------------------------------------------------------------------- #


class TestLivePartialEdgeCases:
    def test_stale_past_live_partial_preserved_by_cascade(self):
        """A live partial set at a PAST ts (before any cascade fires for it)
        must NOT be dropped by an unrelated future cascade. Lines 722-724
        only drop when base_ts == _live_partial.ts.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        three_d = [a for a in mgr.higher_aggs if a.tf == "3D"][0]
        # Feed several base bars first so cascades have fired for MON
        mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)
        mgr.base_agg.on_base_csv_row(MON_APR_13 + DAY_MS, 102.0, 110.0, 100.0, 108.0, 1500.0)
        # Now set live partial at a stale past ts (won't match next cascade)
        three_d.on_base_partial(
            MON_APR_13 - DAY_MS, 50.0, 55.0, 45.0, 52.0, 100.0
        )
        assert three_d._live_partial is not None
        # Feed bar 2 → cascade fires for bar-1 with base_ts=MON+DAY
        mgr.base_agg.on_base_csv_row(MON_APR_13 + 2 * DAY_MS, 108.0, 112.0, 106.0, 110.0, 800.0)
        # Stale partial preserved (MON-DAY != MON+DAY)
        assert three_d._live_partial is not None
        assert three_d._live_partial.ts == MON_APR_13 - DAY_MS


# --------------------------------------------------------------------------- #
# C. volume_delta and OHLCV aggregation
# --------------------------------------------------------------------------- #


class TestVolumeMode:
    def test_base_volume_replaces_not_accumulates(self):
        """Base uses volume_delta=False: snapshot updates REPLACE volume.
        Feed the same ts twice with different volumes; the second wins."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        _feed_csv(
            mgr.base_agg,
            (MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0),
            (MON_APR_13, 100.0, 110.0, 90.0, 108.0, 2500.0),  # snapshot update
        )
        # Same ts → no boundary jump, sub_count increments but volume replaces
        assert mgr.base_agg.volume == 2500.0  # replaced, not summed
        assert mgr.base_agg.high_px == 110.0  # max
        assert mgr.base_agg.low_px == 90.0    # min

    def test_higher_volume_accumulates(self):
        """Higher uses volume_delta=True: sum of subcandle volumes.
        4 base bars → 3 cascaded subs → 3D finalizes; vol = sum of bars 0,1,2.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        log = _install_spy(mgr)
        _feed_csv(
            mgr.base_agg,
            (MON_APR_13,              100.0, 105.0, 95.0, 102.0,  500.0),
            (MON_APR_13 + DAY_MS,     102.0, 110.0, 100.0, 108.0, 1000.0),
            (MON_APR_13 + 2 * DAY_MS, 108.0, 115.0, 105.0, 112.0, 1500.0),
            (MON_APR_13 + 3 * DAY_MS, 112.0, 117.0, 110.0, 115.0,  700.0),
        )
        three_d = [c for c in log if c.tf == "3D"][0]
        assert three_d.vol == 3000.0  # bars 0,1,2: 500 + 1000 + 1500


class TestHighLowAggregation:
    def test_higher_tf_high_low_are_max_min_across_subs(self):
        """4 base bars for one 3D candle. Plant the high in bar-1 and low
        in bar-2 (both within the first 3D bucket; bar-3 just triggers
        finalize).
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        log = _install_spy(mgr)
        _feed_csv(
            mgr.base_agg,
            (MON_APR_13,             100.0, 105.0,  95.0, 102.0, 1000.0),
            (MON_APR_13 + DAY_MS,    102.0, 130.0,  98.0, 110.0, 1000.0),  # high here
            (MON_APR_13 + 2*DAY_MS,  110.0, 115.0,  80.0, 112.0, 1000.0),  # low here
            (MON_APR_13 + 3*DAY_MS,  112.0, 116.0, 110.0, 114.0, 1000.0),  # trigger
        )
        three_d = [c for c in log if c.tf == "3D"][0]
        assert three_d.h == 130.0
        assert three_d.l == 80.0


# --------------------------------------------------------------------------- #
# D. Live partial: matching ts dropped, non-matching preserved
# --------------------------------------------------------------------------- #


class TestLivePartialInteraction:
    def test_matching_ts_drops_live_partial(self):
        """The cascade fires for the FINALIZED base bar with base_ts equal to
        that bar's real_event_ts. So setting _live_partial at the base bar's
        ts BEFORE the next bar's ingestion makes the cascade drop it.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        three_d = [a for a in mgr.higher_aggs if a.tf == "3D"][0]
        # Feed bar 0 (no cascade yet — first bar doesn't finalize)
        mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)
        assert three_d._live_partial is None
        # Set live partial at the SAME ts as the about-to-be-cascaded bar
        three_d.on_base_partial(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 500.0)
        assert three_d._live_partial is not None
        # Feed bar 1 → base finalizes bar-0 → cascade with base_ts=MON_APR_13
        # In 3D's _consume_subcandle: _live_partial.ts==base_ts → drop
        mgr.base_agg.on_base_csv_row(MON_APR_13 + DAY_MS, 102.0, 110.0, 100.0, 108.0, 1500.0)
        assert three_d._live_partial is None

    def test_nonmatching_ts_preserves_live_partial(self):
        """Live partial at a ts that does NOT match the cascade base_ts is
        preserved. Cascade fires with base_ts=MON; live partial is set at
        a FUTURE ts (MON+5D). Should remain after cascade.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        three_d = [a for a in mgr.higher_aggs if a.tf == "3D"][0]
        mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)
        # Set live partial in the FUTURE (won't match the next cascade)
        three_d.on_base_partial(MON_APR_13 + 5 * DAY_MS, 200.0, 210.0, 190.0, 205.0, 500.0)
        assert three_d._live_partial is not None
        # Feed bar 1 at MON+DAY → cascade with base_ts=MON
        # _live_partial.ts (MON+5D) != base_ts (MON) → don't drop
        mgr.base_agg.on_base_csv_row(MON_APR_13 + DAY_MS, 102.0, 110.0, 100.0, 108.0, 1500.0)
        assert three_d._live_partial is not None
        assert three_d._live_partial.ts == MON_APR_13 + 5 * DAY_MS
