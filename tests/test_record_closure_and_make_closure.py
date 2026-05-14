"""Tests for AggregatorManager.record_closure + _make_closure.

Phase P0-3 of the test-coverage plan: pin the "as-of-ts" stream contract.

Pinned invariants:
  - Same aggregator_closure_ts merges into the trailing pending bucket;
    new TF in that bucket increments _pending_tf_count; same TF replaces
    without incrementing.
  - Distinct aggregator_closure_ts values produce distinct buckets in
    APPEND ORDER (not sorted — caller responsibility).
  - _make_closure(include_pending=True) emits one CandleClosure per bucket,
    rolling _last_emitted_closed_state across calls, clears pending,
    resets _pending_tf_count.
  - _make_closure(include_pending=False) filters latest_closed_candles by
    candle.timestamp <= event_ts; returns [] iff no closed + no open.
  - Base→higher cascade at same as-of-ts merges into one bucket.
  - Snapshot emissions are dict-copies (no aliasing across emissions).

Targets:
  - record_closure          line 968-1001
  - _make_closure           line 1016-1093
  - build_snapshot_closure  line 938-965 (verified via the public seam once)
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from candle_iterator.candle_iterator import (
    AggregatorManager,
    Candle,
    CandleClosure,
)


DAY_MS = 86_400_000
MON_APR_13 = int(datetime(2026, 4, 13, tzinfo=timezone.utc).timestamp() * 1000)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _mk_mgr(higher=()):
    """Manager with base=1D and optional higher TFs. No anchor offsets."""
    return AggregatorManager(base_tf="1D", higher_tfs=list(higher))


def _record(mgr, ts, tf, label_ts=None, ohlcv=(100.0, 105.0, 95.0, 102.0, 1000.0)):
    """Convenience: invoke record_closure with sensible defaults."""
    if label_ts is None:
        label_ts = ts
    o, h, l, c, v = ohlcv
    mgr.record_closure(
        aggregator_closure_ts=ts,
        tf=tf,
        label_ts=label_ts,
        o=o, h=h, l=l, c=c, vol=v,
    )


def _feed_base(mgr, bars):
    """Drive the manager naturally via base_agg.on_base_csv_row."""
    for ts, o, h, l, c, v in bars:
        mgr.base_agg.on_base_csv_row(ts, o, h, l, c, v)


def _make_bars(start_ts, n, step_ms=DAY_MS):
    return [(start_ts + i * step_ms, 100.0, 105.0, 95.0, 102.0, 1000.0) for i in range(n)]


# --------------------------------------------------------------------------- #
# record_closure — bucket merge and _pending_tf_count
# --------------------------------------------------------------------------- #


class TestRecordClosureBucketMerge:
    def test_first_call_appends_new_bucket(self):
        mgr = _mk_mgr()
        _record(mgr, ts=1000, tf="1D")
        assert len(mgr._pending) == 1
        assert mgr._pending[0][0] == 1000
        assert "1D" in mgr._pending[0][1]
        assert mgr._pending_tf_count == 1

    def test_same_ts_different_tf_merges_into_same_bucket(self):
        """Three TFs at same event_ts → one bucket, count=3."""
        mgr = _mk_mgr(higher=["3D", "1W"])
        _record(mgr, ts=2000, tf="1D")
        _record(mgr, ts=2000, tf="3D")
        _record(mgr, ts=2000, tf="1W")
        assert len(mgr._pending) == 1
        assert mgr._pending[0][0] == 2000
        assert set(mgr._pending[0][1].keys()) == {"1D", "3D", "1W"}
        assert mgr._pending_tf_count == 3

    def test_same_ts_same_tf_replaces_without_double_count(self):
        """Re-emitting the same TF at same event_ts must NOT inflate count."""
        mgr = _mk_mgr()
        _record(mgr, ts=3000, tf="1D", ohlcv=(1, 2, 0.5, 1.5, 10))
        _record(mgr, ts=3000, tf="1D", ohlcv=(2, 3, 1.5, 2.5, 20))
        assert len(mgr._pending) == 1
        bucket_dict = mgr._pending[0][1]
        assert list(bucket_dict.keys()) == ["1D"]
        # Replacement: second OHLCV must overwrite first
        assert bucket_dict["1D"].close == 2.5
        assert bucket_dict["1D"].volume == 20
        assert mgr._pending_tf_count == 1

    def test_distinct_ts_appends_new_bucket(self):
        mgr = _mk_mgr()
        _record(mgr, ts=1000, tf="1D")
        _record(mgr, ts=2000, tf="1D")
        _record(mgr, ts=3000, tf="1D")
        assert len(mgr._pending) == 3
        assert [b[0] for b in mgr._pending] == [1000, 2000, 3000]
        assert mgr._pending_tf_count == 3


# --------------------------------------------------------------------------- #
# record_closure — append ordering (not sorted)
# --------------------------------------------------------------------------- #


class TestRecordClosureAppendOrder:
    def test_out_of_order_ts_preserves_append_order(self):
        """Direct-invocation contract: _pending preserves append order even
        when ts values are out-of-order. Producer (CandleAggregator) is
        responsible for monotonic ordering.
        """
        mgr = _mk_mgr()
        _record(mgr, ts=2000, tf="1D")
        _record(mgr, ts=1000, tf="1D")
        _record(mgr, ts=1500, tf="1D")
        assert [b[0] for b in mgr._pending] == [2000, 1000, 1500]

    def test_latest_closed_candles_overwritten_per_tf(self):
        """latest_closed_candles[tf] always holds the most recently recorded
        candle for that TF, regardless of ts order.
        """
        mgr = _mk_mgr()
        _record(mgr, ts=1000, tf="1D", ohlcv=(1, 2, 0.5, 1.5, 10))
        _record(mgr, ts=500, tf="1D", ohlcv=(3, 4, 2.5, 3.5, 30))
        # The second call (with smaller ts) overwrites because we store by tf
        assert mgr.latest_closed_candles["1D"].close == 3.5


# --------------------------------------------------------------------------- #
# record_closure — base→higher cascade at same as-of-ts
# --------------------------------------------------------------------------- #


class TestRecordClosureCascade:
    def test_base_to_higher_cascade_merges_into_one_bucket(self):
        """Drive 3 daily bars into mgr with base=1D, higher=2D (sub_factor=2).

        After bar 3 ingestion:
          - Base finalizes bar-2 at aggregator_closure_ts == bar-2's ts.
          - Cascade: 2D's sub_count reaches 2 and finalizes at the SAME
            aggregator_closure_ts.
          - Both record_closure calls land in ONE pending bucket.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["2D"])
        _feed_base(mgr, _make_bars(MON_APR_13, 3))
        # Bar 0 ingestion: no closure (no boundary jump yet).
        # Bar 1 ingestion: jump → close bar-0 at ts=MON_APR_13. Bucket {1D}.
        # Bar 2 ingestion: jump → close bar-1 at ts=MON_APR_13+DAY_MS.
        #                  2D's sub_count goes 1→2 → finalize at same ts. Bucket {1D, 2D}.
        assert len(mgr._pending) == 2
        first_bucket = mgr._pending[0]
        second_bucket = mgr._pending[1]
        assert first_bucket[0] == MON_APR_13
        assert set(first_bucket[1].keys()) == {"1D"}
        assert second_bucket[0] == MON_APR_13 + DAY_MS
        assert set(second_bucket[1].keys()) == {"1D", "2D"}
        # Counter: 1 + 2 = 3 total unique TF-bucket entries
        assert mgr._pending_tf_count == 3


# --------------------------------------------------------------------------- #
# _make_closure — pending consumption
# --------------------------------------------------------------------------- #


class TestMakeClosurePending:
    def test_emits_one_closure_per_bucket_in_append_order(self):
        mgr = _mk_mgr()
        _record(mgr, ts=1000, tf="1D", ohlcv=(1, 2, 0.5, 1.5, 10))
        _record(mgr, ts=2000, tf="1D", ohlcv=(2, 3, 1.5, 2.5, 20))
        _record(mgr, ts=3000, tf="1D", ohlcv=(3, 4, 2.5, 3.5, 30))
        out = mgr._make_closure(event_ts=None, include_pending=True)
        assert len(out) == 3
        assert [cc.timestamp for cc in out] == [1000, 2000, 3000]

    def test_consume_clears_pending_and_resets_counter(self):
        mgr = _mk_mgr()
        _record(mgr, ts=1000, tf="1D")
        _record(mgr, ts=2000, tf="1D")
        assert mgr._pending and mgr._pending_tf_count == 2
        mgr._make_closure(event_ts=None, include_pending=True)
        assert mgr._pending == []
        assert mgr._pending_tf_count == 0

    def test_each_emitted_closure_carries_rolling_state(self):
        """Bucket 1: {1D@ts=1000} → emitted closed_candles == {1D}.
        Bucket 2: {1W@ts=2000} → emitted closed_candles == {1D from baseline,
                                                          1W new delta}.
        """
        mgr = _mk_mgr(higher=["1W"])
        _record(mgr, ts=1000, tf="1D")
        _record(mgr, ts=2000, tf="1W")
        out = mgr._make_closure(event_ts=None, include_pending=True)
        assert len(out) == 2
        assert set(out[0].closed_candles.keys()) == {"1D"}
        assert set(out[1].closed_candles.keys()) == {"1D", "1W"}

    def test_rolling_state_persists_across_make_closure_calls(self):
        """First call records 1D and consumes. Second call records 1W;
        the emitted 1W closure must still see 1D in closed_candles
        (rolling forward from _last_emitted_closed_state).
        """
        mgr = _mk_mgr(higher=["1W"])
        _record(mgr, ts=1000, tf="1D")
        first_out = mgr._make_closure(event_ts=None, include_pending=True)
        assert first_out[0].closed_candles.keys() == {"1D"}

        _record(mgr, ts=2000, tf="1W")
        second_out = mgr._make_closure(event_ts=None, include_pending=True)
        assert len(second_out) == 1
        assert set(second_out[0].closed_candles.keys()) == {"1D", "1W"}

    def test_last_emitted_closed_state_carries_union(self):
        mgr = _mk_mgr(higher=["1W"])
        _record(mgr, ts=1000, tf="1D")
        _record(mgr, ts=2000, tf="1W")
        mgr._make_closure(event_ts=None, include_pending=True)
        assert set(mgr._last_emitted_closed_state.keys()) == {"1D", "1W"}

    def test_emitted_closed_candles_are_independent_snapshots(self):
        """Mutating one emitted closure's closed_candles dict must not affect
        another emitted closure's dict. (Contract: per-bucket dict-copy.)
        """
        mgr = _mk_mgr(higher=["1W"])
        _record(mgr, ts=1000, tf="1D")
        _record(mgr, ts=2000, tf="1W")
        out = mgr._make_closure(event_ts=None, include_pending=True)
        assert len(out) == 2
        # First closure has only 1D; mutate it
        out[0].closed_candles.clear()
        # Second closure must still have both 1D and 1W
        assert set(out[1].closed_candles.keys()) == {"1D", "1W"}

    def test_pending_path_ignores_event_ts(self):
        """When include_pending=True and pending exists, event_ts is NOT
        applied as a snapshot filter — the pending buckets are emitted.

        Critical: use an event_ts STRICTLY LESS THAN the bucket ts. A buggy
        implementation that filtered closed_candles by `candle.ts <= event_ts`
        would silently drop the closure on the snapshot path; a buggy path
        that used event_ts as the emitted closure.timestamp would also be
        caught here.
        """
        mgr = _mk_mgr()
        _record(mgr, ts=1000, tf="1D")
        out = mgr._make_closure(event_ts=500, include_pending=True)
        assert len(out) == 1
        assert out[0].timestamp == 1000  # bucket ts, NOT event_ts
        assert "1D" in out[0].closed_candles  # not filtered out by <=500


# --------------------------------------------------------------------------- #
# Non-contiguous same-ts: merge only with TRAILING bucket
# --------------------------------------------------------------------------- #


class TestNonContiguousSameTs:
    def test_repeat_ts_after_intervening_ts_appends_new_bucket(self):
        """`merge into trailing bucket` only — same ts repeated after an
        intervening ts must create a NEW bucket, not retro-merge into the
        first matching one.
        """
        mgr = _mk_mgr()
        _record(mgr, ts=1000, tf="1D")
        _record(mgr, ts=2000, tf="1D")
        _record(mgr, ts=1000, tf="1D")  # same as first, but not trailing
        assert [b[0] for b in mgr._pending] == [1000, 2000, 1000]
        assert mgr._pending_tf_count == 3


# --------------------------------------------------------------------------- #
# Cascade OHLCV correctness
# --------------------------------------------------------------------------- #


class TestCascadeOHLCV:
    def test_2d_higher_carries_aggregated_ohlcv(self):
        """Feed two distinctive 1D bars; verify the cascaded 2D candle's
        OHLCV is the correct aggregation:
          - 2D.open = bar0.open
          - 2D.high = max(bar0.high, bar1.high)
          - 2D.low  = min(bar0.low, bar1.low)
          - 2D.close = bar1.close
          - 2D.volume = bar0.vol + bar1.vol
        Drive a 3rd bar to trigger 2D finalization via record_closure cascade.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["2D"])
        bars = [
            (MON_APR_13,             100.0, 110.0, 95.0, 108.0, 1000.0),
            (MON_APR_13 + DAY_MS,    108.0, 115.0, 90.0, 112.0, 2000.0),
            (MON_APR_13 + 2*DAY_MS,  112.0, 113.0, 111.0, 112.5, 500.0),
        ]
        _feed_base(mgr, bars)
        out = mgr._make_closure(event_ts=None, include_pending=True)
        # Find the closure carrying 2D
        two_d_closures = [cc for cc in out if "2D" in cc.closed_candles]
        assert len(two_d_closures) == 1
        d2 = two_d_closures[0].closed_candles["2D"]
        assert d2.open == 100.0
        assert d2.high == 115.0
        assert d2.low == 90.0
        assert d2.close == 112.0
        assert d2.volume == 3000.0


# --------------------------------------------------------------------------- #
# _make_closure — as-of invariant
# --------------------------------------------------------------------------- #


class TestAsOfInvariant:
    def test_closed_candle_ts_le_closure_ts_across_all_buckets(self):
        """For each emitted closure, every candle.timestamp in closed_candles
        must be <= closure.timestamp (the as-of contract).
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        _feed_base(mgr, _make_bars(MON_APR_13, 5))
        out = mgr._make_closure(event_ts=None, include_pending=True)
        for cc in out:
            for tf, candle in cc.closed_candles.items():
                assert candle.timestamp <= cc.timestamp, (
                    f"As-of violated: closure.ts={cc.timestamp} "
                    f"has {tf}={candle.timestamp}"
                )

    def test_monotonic_emitted_closure_ts_driven(self):
        """When driven via on_base_csv_row, emitted closures are monotonic
        in timestamp (producer guarantee, NOT _make_closure's responsibility).
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        emitted = []
        for ts, o, h, l, c, v in _make_bars(MON_APR_13, 10):
            mgr.base_agg.on_base_csv_row(ts, o, h, l, c, v)
            emitted.extend(mgr._make_closure(event_ts=None, include_pending=True))
        ts_seq = [cc.timestamp for cc in emitted]
        assert ts_seq == sorted(ts_seq), f"Stream not monotonic: {ts_seq}"


# --------------------------------------------------------------------------- #
# _make_closure — snapshot path (include_pending=False)
# --------------------------------------------------------------------------- #


class TestMakeClosureSnapshot:
    def test_snapshot_filters_closed_by_event_ts(self):
        """`closed_candles` must include only those with candle.timestamp <= event_ts."""
        mgr = _mk_mgr(higher=["1W"])
        # Manually populate latest_closed_candles
        mgr.latest_closed_candles["1D"] = Candle("1D", 100, 1, 2, 0.5, 1.5, 10)
        mgr.latest_closed_candles["1W"] = Candle("1W", 200, 1, 2, 0.5, 1.5, 10)
        out = mgr._make_closure(event_ts=150, include_pending=False)
        assert len(out) == 1
        assert set(out[0].closed_candles.keys()) == {"1D"}
        assert "1W" not in out[0].closed_candles

    def test_snapshot_empty_when_no_closed_and_no_open(self):
        mgr = _mk_mgr()
        out = mgr._make_closure(event_ts=100, include_pending=False)
        assert out == []

    def test_snapshot_with_only_open_candles_emits(self):
        """If no closed candles exist but an open candle exists, snapshot
        path must still emit a closure carrying the open candle.
        """
        mgr = _mk_mgr()
        # Feed one bar — base has open state but no closure recorded yet
        mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)
        # Sanity: no pending closures yet
        assert mgr._pending == []
        out = mgr._make_closure(event_ts=MON_APR_13, include_pending=False)
        assert len(out) == 1
        assert out[0].open_candles  # open candle present
        assert out[0].closed_candles == {}  # no closures yet

    def test_snapshot_with_event_ts_none_uses_preferred(self):
        """event_ts=None falls back to preferred_snapshot_ts() and uses that."""
        mgr = _mk_mgr()
        mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)
        out = mgr._make_closure(event_ts=None, include_pending=False)
        assert len(out) == 1
        assert out[0].timestamp == MON_APR_13

    def test_snapshot_empty_when_event_ts_none_and_empty_manager(self):
        """No data at all + event_ts=None → preferred_snapshot_ts returns
        None → empty list.
        """
        mgr = _mk_mgr()
        out = mgr._make_closure(event_ts=None, include_pending=False)
        assert out == []


# --------------------------------------------------------------------------- #
# include_pending=False does NOT consume pending
# --------------------------------------------------------------------------- #


class TestIncludePendingFalse:
    def test_snapshot_with_pending_present_does_not_consume(self):
        mgr = _mk_mgr()
        _record(mgr, ts=1000, tf="1D")
        # latest_closed_candles is populated by record_closure → snapshot path
        # will see it and emit one closure based on event_ts filter.
        out = mgr._make_closure(event_ts=5000, include_pending=False)
        # Pending must still be there
        assert len(mgr._pending) == 1
        assert mgr._pending[0][0] == 1000
        assert mgr._pending_tf_count == 1
        # Snapshot returns one closure carrying the closed 1D
        assert len(out) == 1
        assert "1D" in out[0].closed_candles


# --------------------------------------------------------------------------- #
# Public seam: build_snapshot_closure
# --------------------------------------------------------------------------- #


class TestBuildSnapshotClosure:
    def test_returns_none_on_empty_manager(self):
        mgr = _mk_mgr()
        assert mgr.build_snapshot_closure(event_ts=None) is None
        assert mgr.build_snapshot_closure(event_ts=12345) is None

    def test_filters_closed_by_event_ts(self):
        mgr = _mk_mgr()
        mgr.latest_closed_candles["1D"] = Candle("1D", 100, 1, 2, 0.5, 1.5, 10)
        mgr.latest_closed_candles["1W"] = Candle("1W", 300, 1, 2, 0.5, 1.5, 10)
        cc = mgr.build_snapshot_closure(event_ts=200)
        assert isinstance(cc, CandleClosure)
        assert cc.timestamp == 200
        assert "1D" in cc.closed_candles
        assert "1W" not in cc.closed_candles

    def test_preferred_ts_used_when_event_ts_none(self):
        mgr = _mk_mgr()
        mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)
        cc = mgr.build_snapshot_closure(event_ts=None)
        assert cc is not None
        assert cc.timestamp == MON_APR_13
