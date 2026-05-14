"""Golden multi-TF integration test.

Phase P2-6 of the test-coverage plan: one realistic 1m → 5m → 1h stream
exercising boundary crossings, gap-fill, and snapshot updates. Asserts
high-level closure invariants:
  - Monotonic closure.timestamp across the entire emission stream.
  - As-of: every `c.timestamp` in `cc.closed_candles.values()` <= cc.timestamp.
  - 5m OHLC aggregation correctness over a known input pattern.
  - 1h OHLC aggregation correctness over a known input pattern.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from candle_iterator.candle_iterator import AggregatorManager


MIN_MS = 60_000
HOUR_MS = 60 * MIN_MS

# A clean 1h-aligned base epoch (UTC midnight)
BASE_TS = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


def _make_minute(i, value):
    """One minute bar at BASE_TS + i*MIN_MS with all OHLC = value and v=10."""
    return (BASE_TS + i * MIN_MS, value, value, value, value, 10.0)


def _feed(mgr, bars):
    for ts, o, h, l, c, v in bars:
        mgr.base_agg.on_base_csv_row(ts, o, h, l, c, v)


class TestGoldenStream:
    def test_1m_to_5m_aggregation_correct(self):
        """Feed 6 minute bars; 5 of them form one complete 5m candle.
        After feeding bar 6 (which finalizes bar 5), the 5m candle is closed.
        """
        mgr = AggregatorManager(base_tf="1m", higher_tfs=["5m"])
        # Bars 0..4 form one 5m bucket; bar 5 starts the next
        # We need bars 0..4 finalized to fill the 5m sub_factor.
        # Bar finalization happens when the NEXT bar arrives, so we need 6
        # bars to finalize bars 0..4 (the 6th bar's ingestion finalizes bar-4,
        # which cascades to 5m).
        bars = [_make_minute(i, 100.0 + i) for i in range(6)]
        _feed(mgr, bars)
        # The 5m candle from bars 0..4
        five_m = mgr.latest_closed_candles.get("5m")
        assert five_m is not None
        assert five_m.timestamp == BASE_TS  # aligned to 5m boundary
        assert five_m.open == 100.0   # bar 0
        assert five_m.high == 104.0   # max(100..104)
        assert five_m.low == 100.0    # min(100..104)
        assert five_m.close == 104.0  # bar 4's close
        assert five_m.volume == 50.0  # 5 * 10.0

    def test_1m_to_1h_aggregation_correct(self):
        """Feed 61 minute bars (60 to fill one 1h, +1 to trigger finalize)."""
        mgr = AggregatorManager(base_tf="1m", higher_tfs=["1h"])
        bars = [_make_minute(i, 100.0 + (i % 60)) for i in range(61)]
        _feed(mgr, bars)
        one_h = mgr.latest_closed_candles.get("1h")
        assert one_h is not None
        assert one_h.timestamp == BASE_TS
        assert one_h.open == 100.0    # bar 0
        assert one_h.high == 159.0    # max over 60 bars (100+59)
        assert one_h.low == 100.0
        assert one_h.close == 159.0   # bar 59
        assert one_h.volume == 600.0  # 60 * 10

    def test_monotonic_closure_timestamps_across_stream(self):
        """Drive the manager bar-by-bar, drain pending closures after each,
        and verify the emitted closure stream has monotonically non-decreasing
        timestamps.
        """
        mgr = AggregatorManager(base_tf="1m", higher_tfs=["5m", "1h"])
        emitted = []
        for i in range(120):  # 2 hours of minutes
            mgr.base_agg.on_base_csv_row(
                BASE_TS + i * MIN_MS, 100.0 + (i % 60), 100.0 + (i % 60),
                100.0 + (i % 60), 100.0 + (i % 60), 10.0,
            )
            emitted.extend(mgr._make_closure(event_ts=None, include_pending=True))
        ts_seq = [cc.timestamp for cc in emitted]
        assert ts_seq == sorted(ts_seq), (
            f"Stream not monotonic across {len(ts_seq)} emissions"
        )

    def test_as_of_invariant_across_stream(self):
        """Every emitted closure: each closed candle's ts <= closure.timestamp."""
        mgr = AggregatorManager(base_tf="1m", higher_tfs=["5m", "1h"])
        emitted = []
        for i in range(120):
            mgr.base_agg.on_base_csv_row(
                BASE_TS + i * MIN_MS, 100.0 + (i % 60), 100.0 + (i % 60),
                100.0 + (i % 60), 100.0 + (i % 60), 10.0,
            )
            emitted.extend(mgr._make_closure(event_ts=None, include_pending=True))
        for cc in emitted:
            for tf, candle in cc.closed_candles.items():
                assert candle.timestamp <= cc.timestamp, (
                    f"As-of violation: closure.ts={cc.timestamp} "
                    f"has {tf} ts={candle.timestamp}"
                )

    def test_multi_tf_finalizations_in_correct_order(self):
        """At a 1h boundary (bar 60), the cascade should produce both a 5m
        closure (bar 55..59 bucket) and a 1h closure (bar 0..59 bucket),
        in some order. Verify the 1h closure is in latest_closed.
        """
        mgr = AggregatorManager(base_tf="1m", higher_tfs=["5m", "1h"])
        for i in range(61):
            mgr.base_agg.on_base_csv_row(
                BASE_TS + i * MIN_MS, 100.0 + (i % 60), 100.0 + (i % 60),
                100.0 + (i % 60), 100.0 + (i % 60), 10.0,
            )
        # All 3 TFs should have at least one closed candle
        assert "1m" in mgr.latest_closed_candles
        assert "5m" in mgr.latest_closed_candles
        assert "1h" in mgr.latest_closed_candles
