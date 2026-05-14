"""Tests for AggregatorManager.flush() guards beyond force_close_final.

Phase P1-1 of the test-coverage plan. Existing `test_force_close_final.py`
covers the force_close_final flag, basic empty-flush, idempotency, and
higher-TF cascade. This file pins:

  - Empty-manager guard (line 1149-1151).
  - Higher-TF logical completion (sub_count >= sub_factor mid-flush).
  - _final_partial_emitted set unconditionally on first flush (quirk pin).
  - Final partial ordering (after pending closures).
  - Final partial timestamp = max(open candle timestamps).
  - Final partial is_final=True; not emitted if no open candles.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from candle_iterator.candle_iterator import AggregatorManager


DAY_MS = 86_400_000
MON_APR_13 = int(datetime(2026, 4, 13, tzinfo=timezone.utc).timestamp() * 1000)


def _feed(mgr, bars):
    for ts, o, h, l, c, v in bars:
        mgr.base_agg.on_base_csv_row(ts, o, h, l, c, v)


def _bars(start_ts, n):
    return [(start_ts + i * DAY_MS, 100.0, 105.0, 95.0, 102.0, 1000.0) for i in range(n)]


# --------------------------------------------------------------------------- #
# A. Empty-manager guard (line 1149-1151)
# --------------------------------------------------------------------------- #


class TestEmptyManagerGuard:
    def test_higher_agg_with_no_data_skipped(self):
        """Higher aggregator with last_sub_ts=None AND current_boundary=None
        is skipped in the logical-completion loop, avoiding epoch-0 stamps.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        three_d = [a for a in mgr.higher_aggs if a.tf == "3D"][0]
        # Sanity: completely fresh
        assert three_d.last_sub_ts is None
        assert three_d.current_boundary is None
        result = mgr.flush()
        # No closures emitted; no exception
        assert result == []

    def test_empty_flush_sets_final_partial_emitted_flag(self):
        """Documenting quirk: even an empty flush sets _final_partial_emitted
        to True. A later flush with new data will NOT emit a final partial
        because the flag is one-way.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        assert mgr._final_partial_emitted is False
        mgr.flush()
        # Quirk pin: flag is set even when no closure was appended
        assert mgr._final_partial_emitted is True

    def test_empty_flush_followed_by_data_does_not_emit_final_partial(self):
        """Quirk consequence: if a consumer flushes early (perhaps before
        any data arrives), then later ingests data and flushes again, the
        SECOND flush won't include a final partial.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        mgr.flush()  # poisons the flag
        _feed(mgr, _bars(MON_APR_13, 1))
        result = mgr.flush()
        # No partials emitted (flag is True)
        partials = [c for c in result if c.is_final]
        assert partials == []


# --------------------------------------------------------------------------- #
# B. Higher-TF logical completion mid-flush (line 1148-1160)
# --------------------------------------------------------------------------- #


class TestHigherTfLogicalCompletion:
    def test_logical_completion_finalizes_when_sub_count_at_threshold(self):
        """Feed 3 daily bars + manually set 3D's sub_count to sub_factor=3
        without driving a 4th bar. flush() should detect sub_count >= sub_factor
        and finalize the 3D candle (logical completion).

        Realistic state construction: drive 3 bars (3D has sub_count=2 after
        2 cascaded subs). Then manually bump sub_count to 3 to simulate a
        cascade we want to test the guard for.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        _feed(mgr, _bars(MON_APR_13, 3))
        three_d = [a for a in mgr.higher_aggs if a.tf == "3D"][0]
        # After 3 bars: 3D got 2 cascaded subs (from bars 0, 1)
        assert three_d.sub_count == 2
        # Manually nudge sub_count to threshold (simulates a 3rd cascade)
        three_d.sub_count = 3
        # Drain any pending closures from the natural ingestion to isolate
        mgr._pending.clear()
        mgr._pending_tf_count = 0
        result = mgr.flush()
        # The logical-completion block should have finalized 3D
        three_d_closures = [c for c in result if "3D" in c.closed_candles]
        assert len(three_d_closures) >= 1

    def test_no_logical_completion_when_below_threshold(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        _feed(mgr, _bars(MON_APR_13, 2))
        three_d = [a for a in mgr.higher_aggs if a.tf == "3D"][0]
        assert three_d.sub_count < three_d.sub_factor
        result = mgr.flush()
        # No 3D closure (below threshold)
        three_d_closures = [c for c in result if "3D" in c.closed_candles]
        assert three_d_closures == []


# --------------------------------------------------------------------------- #
# C. Final partial: presence, contents, ordering
# --------------------------------------------------------------------------- #


class TestFinalPartial:
    def test_final_partial_has_is_final_true(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        _feed(mgr, _bars(MON_APR_13, 1))
        result = mgr.flush()
        partials = [c for c in result if c.is_final]
        assert len(partials) == 1

    def test_final_partial_timestamp_is_max_of_open_candles(self):
        """Multiple open candles → final partial ts = max(oc.ts)."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        _feed(mgr, _bars(MON_APR_13, 1))
        # Both 1D and 3D have open candles; pick the highest timestamp
        open_candles = mgr._current_open_candles()
        max_ts = max(oc.timestamp for oc in open_candles.values())
        result = mgr.flush()
        partials = [c for c in result if c.is_final]
        assert len(partials) == 1
        assert partials[0].timestamp == max_ts

    def test_no_final_partial_when_no_open_candles(self):
        """Empty manager → no open candles → no final partial appended."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        result = mgr.flush()
        partials = [c for c in result if c.is_final]
        assert partials == []

    def test_pending_closures_emitted_before_final_partial(self):
        """Drive 2 daily bars so base finalizes bar-0 (pending closure), then
        flush. The pending closure must appear BEFORE the final partial.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=[])
        _feed(mgr, _bars(MON_APR_13, 2))
        # Don't drain pending; flush handles it
        result = mgr.flush()
        is_final_flags = [c.is_final for c in result]
        # There IS a final partial
        assert True in is_final_flags
        # And it is the LAST element (no non-final entries follow)
        first_final_idx = is_final_flags.index(True)
        assert first_final_idx == len(is_final_flags) - 1, (
            f"final partial must be the last entry; got is_final_flags="
            f"{is_final_flags}"
        )
        # And at least one pending closure precedes it
        assert first_final_idx >= 1
        # Sanity: the leading entries are all non-final
        assert not any(is_final_flags[:first_final_idx])
