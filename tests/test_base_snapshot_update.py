"""Tests for CandleAggregator._consume_subcandle on the BASE aggregator
when receiving same-ts snapshot updates.

Phase P1-8 of the test-coverage plan: pin that re-ingesting the same
`base_ts` on the base aggregator does NOT break state (sub_factor=1 quirk).

For the base aggregator (is_base=True, sub_factor=1), the
`if not self.is_base and self.sub_count >= self.sub_factor` check
prevents premature finalization. This test pins that invariant: a refactor
adding a `sub_count >= 1` shortcut on base would silently break snapshot
updates and cause spurious closures.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, NamedTuple

import pytest

from candle_iterator.candle_iterator import AggregatorManager


DAY_MS = 86_400_000
MON_APR_13 = int(datetime(2026, 4, 13, tzinfo=timezone.utc).timestamp() * 1000)


class RecCall(NamedTuple):
    aggregator_closure_ts: int
    tf: str
    label_ts: int


def _install_spy(mgr):
    log: List[RecCall] = []
    original = mgr.record_closure
    def spy(aggregator_closure_ts, tf, label_ts, o, h, l, c, vol):
        log.append(RecCall(aggregator_closure_ts, tf, label_ts))
        original(aggregator_closure_ts, tf, label_ts, o, h, l, c, vol)
    mgr.record_closure = spy
    return log


class TestBaseSnapshotUpdateInvariant:
    def test_repeat_same_ts_does_not_finalize_base(self):
        """Feed the same base_ts twice — sub_count increments but no closure
        is emitted because base is is_base=True (line 727 guard).
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        log = _install_spy(mgr)
        mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)
        mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 110.0, 90.0, 108.0, 1500.0)
        # sub_count incremented but NO closure recorded
        assert mgr.base_agg.sub_count == 2
        assert log == []

    def test_repeat_same_ts_updates_h_l_c_v(self):
        """Same-ts re-ingest acts as a snapshot update: H/L/C/V are updated
        (per the standard update at lines 711-719).
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)
        mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 110.0, 90.0, 108.0, 1500.0)
        # H=max, L=min, C=latest, V=latest (volume_delta=False)
        assert mgr.base_agg.high_px == 110.0
        assert mgr.base_agg.low_px == 90.0
        assert mgr.base_agg.close_px == 108.0
        assert mgr.base_agg.volume == 1500.0

    def test_many_same_ts_repeats_still_no_finalize(self):
        """Many same-ts re-ingests must NOT cause an accidental finalize on
        the base (no `sub_count >= 1` shortcut should exist).
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        log = _install_spy(mgr)
        for _ in range(10):
            mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)
        assert mgr.base_agg.sub_count == 10
        assert log == []

    def test_same_ts_then_next_ts_finalizes_correctly(self):
        """After several same-ts updates, the FIRST strictly-next-boundary
        bar finalizes the accumulated state cleanly with the LATEST snapshot
        values.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        log = _install_spy(mgr)
        # 3 snapshots at MON_APR_13 with evolving high/low/close
        mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 105.0, 95.0, 102.0, 1000.0)
        mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 110.0, 92.0, 108.0, 1200.0)
        mgr.base_agg.on_base_csv_row(MON_APR_13, 100.0, 115.0, 90.0, 112.0, 1500.0)
        # Now next bar
        mgr.base_agg.on_base_csv_row(MON_APR_13 + DAY_MS, 112.0, 120.0, 110.0, 118.0, 2000.0)
        # Exactly one closure for the MON_APR_13 bar
        assert len(log) == 1
        assert log[0].label_ts == MON_APR_13
        # The closure carries the LATEST snapshot values
        closed = mgr.latest_closed_candles["1D"]
        assert closed.open == 100.0
        assert closed.high == 115.0
        assert closed.low == 90.0
        assert closed.close == 112.0
        assert closed.volume == 1500.0  # replaced, not summed
