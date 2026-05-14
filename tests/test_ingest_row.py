"""Tests for CandleIterator._ingest_row.

Phase P0-5 of the test-coverage plan: pin the gap-fill, snapshot-update,
older-row skip, and end_ts overrun semantics.

Target: candle_iterator/candle_iterator.py:1402-1444.

Strategy: instantiate CandleIterator directly with a FakeManager that
records every on_subcandle / flush call. The factory machinery (network
sync, ENV resolution) is bypassed because it has no bearing on _ingest_row.
"""
from __future__ import annotations

from typing import List, NamedTuple, Optional

import pytest

from candle_iterator.candle_iterator import (
    CandleIterator,
    CandleClosure,
    Config,
)


DAY_MS = 86_400_000
MIN_MS = 60_000


# --------------------------------------------------------------------------- #
# FakeManager: minimal stub matching the protocol _ingest_row depends on
# --------------------------------------------------------------------------- #


class SubcandleCall(NamedTuple):
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float


class FakeManager:
    """Captures `on_subcandle` calls and lets the test configure return
    closure-lists per call. `flush()` returns whatever was pre-configured.
    """

    def __init__(self):
        self.subcandle_calls: List[SubcandleCall] = []
        self.flush_calls: int = 0
        # The closure returned per on_subcandle call. Default: empty list.
        self._subcandle_returns: List[List[CandleClosure]] = []
        # The closure returned per flush call. Default: empty list.
        self._flush_returns: List[List[CandleClosure]] = []

    def queue_subcandle_return(self, closures: List[CandleClosure]) -> None:
        self._subcandle_returns.append(list(closures))

    def queue_flush_return(self, closures: List[CandleClosure]) -> None:
        self._flush_returns.append(list(closures))

    def on_subcandle(self, ts, o, h, l, c, v):
        self.subcandle_calls.append(SubcandleCall(ts, o, h, l, c, v))
        if self._subcandle_returns:
            return self._subcandle_returns.pop(0)
        return []

    def flush(self):
        self.flush_calls += 1
        if self._flush_returns:
            return self._flush_returns.pop(0)
        return []


# --------------------------------------------------------------------------- #
# Helper: build a CandleIterator with FakeManager, no IO sync
# --------------------------------------------------------------------------- #


def _mk_iter(tmp_path, *, base_tf="1m", end_ts: Optional[int] = None,
             verbose: bool = False):
    """Construct a CandleIterator wired to FakeManager + empty data dir.

    Bypasses the factory; we don't want network sync or auto-start-date logic.
    """
    data_path = tmp_path / "EODHD" / "candles" / "TEST.US" / base_tf
    data_path.mkdir(parents=True)
    cfg = Config(
        exchange="EODHD",
        ticker="TEST.US",
        base_timeframe=base_tf,
        start_ts=None,
        end_ts=end_ts,
        data_dir=str(tmp_path),
        verbose=verbose,
        poll_interval_seconds=None,
    )
    fake_mgr = FakeManager()
    it = CandleIterator(cfg, fake_mgr)
    return it, fake_mgr


def _make_closure(ts: int) -> CandleClosure:
    """Sentinel closure for ordering assertions; identity by ts."""
    return CandleClosure(timestamp=ts, closed_candles={}, open_candles={})


# --------------------------------------------------------------------------- #
# A. First-row initialization
# --------------------------------------------------------------------------- #


class TestFirstRowInit:
    def test_first_row_sets_last_ts_and_last_close(self, tmp_path):
        it, mgr = _mk_iter(tmp_path)
        assert it._last_ts is None
        assert it._last_close is None
        it._ingest_row(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        # After ingest, _last_ts == row_ts, _last_close == c (the row's close)
        assert it._last_ts == 1000
        assert it._last_close == 102.0  # bar's close, NOT the temp `o` from init
        # No gap-fill dummies; one real on_subcandle
        assert len(mgr.subcandle_calls) == 1
        assert mgr.subcandle_calls[0].ts == 1000
        assert mgr.subcandle_calls[0].v == 1000.0  # NOT a dummy (v=0)
        assert mgr.flush_calls == 0


# --------------------------------------------------------------------------- #
# B. Gap-fill (carry-forward dummies)
# --------------------------------------------------------------------------- #


class TestGapFill:
    def test_no_gap_consecutive_bars(self, tmp_path):
        """ts0, ts0+base_ms → no dummies, two real subcandles."""
        it, mgr = _mk_iter(tmp_path, base_tf="1m")
        it._ingest_row(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        it._ingest_row(1000 + MIN_MS, 102.0, 110.0, 100.0, 108.0, 1500.0)
        assert len(mgr.subcandle_calls) == 2
        assert mgr.subcandle_calls[0].ts == 1000
        assert mgr.subcandle_calls[1].ts == 1000 + MIN_MS
        # No dummies — both calls have non-zero volume
        for call in mgr.subcandle_calls:
            assert call.v > 0

    def test_single_gap_emits_one_dummy(self, tmp_path):
        """ts0, ts0+2*base_ms → one dummy at ts0+base_ms, then real row."""
        it, mgr = _mk_iter(tmp_path, base_tf="1m")
        it._ingest_row(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        it._ingest_row(1000 + 2 * MIN_MS, 110.0, 115.0, 105.0, 112.0, 2000.0)
        assert len(mgr.subcandle_calls) == 3  # first real + dummy + second real
        # Call 0: first real row
        assert mgr.subcandle_calls[0] == SubcandleCall(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        # Call 1: dummy (OHLC all equal _last_close=102.0, v=0)
        dummy = mgr.subcandle_calls[1]
        assert dummy.ts == 1000 + MIN_MS
        assert dummy.o == dummy.h == dummy.l == dummy.c == 102.0
        assert dummy.v == 0.0
        # Call 2: second real row (with its own o)
        assert mgr.subcandle_calls[2].ts == 1000 + 2 * MIN_MS
        assert mgr.subcandle_calls[2].v == 2000.0

    def test_three_gap_emits_three_dummies(self, tmp_path):
        """ts0, ts0+4*base_ms → three dummies at +1, +2, +3, then real row."""
        it, mgr = _mk_iter(tmp_path, base_tf="1m")
        it._ingest_row(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        it._ingest_row(1000 + 4 * MIN_MS, 110.0, 115.0, 105.0, 112.0, 2000.0)
        # 1 first real + 3 dummies + 1 second real = 5
        assert len(mgr.subcandle_calls) == 5
        # Dummies at +1, +2, +3 base_ms
        for i, expected_ts in enumerate(
            [1000 + MIN_MS, 1000 + 2 * MIN_MS, 1000 + 3 * MIN_MS], start=1
        ):
            call = mgr.subcandle_calls[i]
            assert call.ts == expected_ts
            assert call.o == call.h == call.l == call.c == 102.0
            assert call.v == 0.0

    def test_dummy_carries_forward_close_chain(self, tmp_path):
        """Across multiple dummies, _last_close stays at the original close
        because each dummy's OHLC = prior close, so the chain is constant.
        """
        it, mgr = _mk_iter(tmp_path, base_tf="1m")
        it._ingest_row(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        it._ingest_row(1000 + 3 * MIN_MS, 200.0, 210.0, 190.0, 205.0, 3000.0)
        # State after gap-fill but before real row: _last_close should have
        # been the constant 102.0 across all dummies. After real ingest: 205.
        assert it._last_close == 205.0
        # Final ts == real row ts
        assert it._last_ts == 1000 + 3 * MIN_MS

    def test_real_row_uses_its_own_OHLCV_not_dummy_carry(self, tmp_path):
        it, mgr = _mk_iter(tmp_path, base_tf="1m")
        it._ingest_row(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        it._ingest_row(1000 + 2 * MIN_MS, 999.0, 999.0, 999.0, 999.0, 9999.0)
        # The last subcandle call is the real row, with its OWN OHLCV
        final = mgr.subcandle_calls[-1]
        assert final.ts == 1000 + 2 * MIN_MS
        assert final.o == 999.0
        assert final.v == 9999.0


# --------------------------------------------------------------------------- #
# C. end_ts overrun
# --------------------------------------------------------------------------- #


class TestEndTsOverrun:
    def test_row_within_end_ts_normal_ingest(self, tmp_path):
        it, mgr = _mk_iter(tmp_path, end_ts=5000)
        it._ingest_row(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        assert len(mgr.subcandle_calls) == 1
        assert mgr.flush_calls == 0

    def test_row_strictly_beyond_end_ts_flushes_no_ingest(self, tmp_path):
        """Seed _last_ts first so we don't hit the first-row init path."""
        it, mgr = _mk_iter(tmp_path, end_ts=5000)
        it._last_ts = 4000  # seed past the first-row init
        it._last_close = 100.0
        it._ingest_row(6000, 200.0, 210.0, 190.0, 205.0, 500.0)
        # Beyond end_ts → flush + return; no on_subcandle for this row
        assert mgr.flush_calls == 1
        # No subcandle call for the overrun row (last call would have been seed)
        assert mgr.subcandle_calls == []

    def test_row_exactly_at_end_ts_allowed(self, tmp_path):
        """row_ts == end_ts is allowed (condition is strict >)."""
        it, mgr = _mk_iter(tmp_path, end_ts=5000)
        it._ingest_row(5000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        assert len(mgr.subcandle_calls) == 1
        assert mgr.flush_calls == 0

    def test_mid_gap_fill_end_ts_overrun_stops_loop(self, tmp_path):
        """Gap-fill loop stops when missing_ts > end_ts."""
        # base=1m, end_ts=1000+2*MIN_MS. First row at 1000, second at 1000+5*MIN_MS.
        # Dummies would be at +1, +2, +3, +4. +3 > end_ts → flush + return.
        end_ts = 1000 + 2 * MIN_MS
        it, mgr = _mk_iter(tmp_path, base_tf="1m", end_ts=end_ts)
        it._ingest_row(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        it._ingest_row(1000 + 5 * MIN_MS, 200.0, 210.0, 190.0, 205.0, 500.0)
        # Expect: real row 1 + 2 dummies (at +1, +2), then loop exits because
        # +3 > end_ts. flush called. Second real row never ingested.
        ts_seq = [c.ts for c in mgr.subcandle_calls]
        assert ts_seq == [1000, 1000 + MIN_MS, 1000 + 2 * MIN_MS]
        assert mgr.flush_calls == 1
        # The second real row's ts must NOT appear
        assert 1000 + 5 * MIN_MS not in ts_seq

    def test_dummy_exactly_at_end_ts_is_allowed(self, tmp_path):
        """Dummies at missing_ts == end_ts pass the (missing_ts > end_ts)
        check and are emitted. Only strictly-greater triggers the flush.
        """
        end_ts = 1000 + 2 * MIN_MS
        it, mgr = _mk_iter(tmp_path, base_tf="1m", end_ts=end_ts)
        it._ingest_row(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        it._ingest_row(1000 + 3 * MIN_MS, 200.0, 210.0, 190.0, 205.0, 500.0)
        # Dummies at +1 MIN_MS (=61000) and +2 MIN_MS (=121000=end_ts).
        # +2 <= end_ts → ok. Then real row at +3 MIN_MS > end_ts → flush+return.
        ts_seq = [c.ts for c in mgr.subcandle_calls]
        assert ts_seq == [1000, 1000 + MIN_MS, 1000 + 2 * MIN_MS]
        # The +3 real row triggers the post-loop end_ts check → flush
        assert mgr.flush_calls == 1


class TestEndTsFirstRowOverrun:
    def test_first_row_beyond_end_ts_sets_last_ts_then_flushes(self, tmp_path):
        """Pin the subtle state mutation in the first-row-overrun path:
        - Line 1406-1408 sets `_last_ts = row_ts` BEFORE the overrun check.
        - Line 1428 then detects `row_ts > end_ts` and calls flush+return.
        Net result: _last_ts is mutated even though no on_subcandle fires.
        """
        it, mgr = _mk_iter(tmp_path, end_ts=5000)
        assert it._last_ts is None
        flush_closure = _make_closure(ts=4999)
        mgr.queue_flush_return([flush_closure])
        it._ingest_row(6000, 200.0, 210.0, 190.0, 205.0, 500.0)
        # Despite the overrun, _last_ts was set during the init block
        assert it._last_ts == 6000
        # No on_subcandle for the overrun row
        assert mgr.subcandle_calls == []
        # flush() was called and its closures landed in the buffer
        assert mgr.flush_calls == 1
        assert list(it._closed_buffer) == [flush_closure]


class TestMidGapFlushOrdering:
    def test_dummy_closures_then_flush_closures_in_buffer_order(self, tmp_path):
        """During mid-gap-fill, dummies emit BEFORE the overrun-triggered flush.
        Pin that emit order in `_closed_buffer`.
        """
        end_ts = 1000 + 2 * MIN_MS
        it, mgr = _mk_iter(tmp_path, base_tf="1m", end_ts=end_ts)
        # First call: no closures
        mgr.queue_subcandle_return([])
        # Dummy 1 (at +1 MIN_MS): emits one closure
        dummy_1_closure = _make_closure(ts=1000 + MIN_MS)
        mgr.queue_subcandle_return([dummy_1_closure])
        # Dummy 2 (at +2 MIN_MS): emits one closure
        dummy_2_closure = _make_closure(ts=1000 + 2 * MIN_MS)
        mgr.queue_subcandle_return([dummy_2_closure])
        # Mid-gap overrun flush returns its own closure
        flush_closure = _make_closure(ts=end_ts + 1)
        mgr.queue_flush_return([flush_closure])

        it._ingest_row(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        it._ingest_row(1000 + 5 * MIN_MS, 200.0, 210.0, 190.0, 205.0, 500.0)
        # Buffer: dummy_1, dummy_2, flush_closure (in this exact order)
        assert list(it._closed_buffer) == [dummy_1_closure, dummy_2_closure, flush_closure]
        assert mgr.flush_calls == 1


class TestEndTsZeroFalsy:
    def test_end_ts_zero_treated_as_unbounded_documenting(self, tmp_path):
        """`if self.cfg.end_ts` is truthy — end_ts=0 short-circuits as falsy,
        meaning end_ts=0 is currently treated as UNBOUNDED. This pins the
        existing falsy behavior; if epoch-0 should ever become a valid bound,
        the implementation needs `is not None` semantics.
        """
        it, mgr = _mk_iter(tmp_path, end_ts=0)
        it._ingest_row(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        # Despite row_ts=1000 > end_ts=0, no flush — end_ts=0 is falsy.
        assert mgr.flush_calls == 0
        assert len(mgr.subcandle_calls) == 1


# --------------------------------------------------------------------------- #
# D. Older row skip / equal-ts snapshot update
# --------------------------------------------------------------------------- #


class TestOlderRowSkipAndSnapshotUpdate:
    def test_strictly_older_row_skipped(self, tmp_path):
        it, mgr = _mk_iter(tmp_path)
        it._ingest_row(2000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        # Try to ingest an older row
        it._ingest_row(1000, 50.0, 60.0, 40.0, 55.0, 500.0)
        # Only the first call should have hit on_subcandle
        assert len(mgr.subcandle_calls) == 1
        assert mgr.subcandle_calls[0].ts == 2000
        # _last_ts unchanged
        assert it._last_ts == 2000
        assert it._last_close == 102.0

    def test_equal_ts_treated_as_snapshot_update(self, tmp_path):
        """row_ts == _last_ts is NOT skipped (strict < condition). It
        re-ingests as a snapshot update and updates _last_close.
        """
        it, mgr = _mk_iter(tmp_path)
        it._ingest_row(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        it._ingest_row(1000, 100.0, 115.0, 90.0, 110.0, 2500.0)  # snapshot update
        assert len(mgr.subcandle_calls) == 2
        assert mgr.subcandle_calls[0].c == 102.0
        assert mgr.subcandle_calls[1].c == 110.0
        # _last_close reflects the LATEST snapshot
        assert it._last_close == 110.0


# --------------------------------------------------------------------------- #
# E. Closure buffering — order of dummies before real row
# --------------------------------------------------------------------------- #


class TestClosureBuffering:
    def test_dummy_closures_then_real_closures_appended_in_order(self, tmp_path):
        """Verify _closed_buffer captures gap-fill closures BEFORE real-ingest
        closures by queueing distinct sentinel returns on the FakeManager.
        """
        it, mgr = _mk_iter(tmp_path)
        # First call: no closures.
        mgr.queue_subcandle_return([])
        # Second call (dummy): one sentinel closure.
        dummy_closure = _make_closure(ts=1000 + MIN_MS)
        mgr.queue_subcandle_return([dummy_closure])
        # Third call (real second row): another sentinel.
        real_closure = _make_closure(ts=1000 + 2 * MIN_MS)
        mgr.queue_subcandle_return([real_closure])

        it._ingest_row(1000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        it._ingest_row(1000 + 2 * MIN_MS, 110.0, 115.0, 105.0, 112.0, 2000.0)
        # Buffer order: [dummy_closure, real_closure]
        assert list(it._closed_buffer) == [dummy_closure, real_closure]

    def test_flush_closures_appended_on_end_ts_overrun(self, tmp_path):
        flush_closure = _make_closure(ts=4500)
        it, mgr = _mk_iter(tmp_path, end_ts=5000)
        it._last_ts = 4000  # seed
        it._last_close = 100.0
        mgr.queue_flush_return([flush_closure])
        it._ingest_row(6000, 200.0, 210.0, 190.0, 205.0, 500.0)
        # Flush closure landed in the buffer
        assert list(it._closed_buffer) == [flush_closure]

    def test_empty_flush_return_does_not_break(self, tmp_path):
        """If manager.flush() returns [] (no pending), buffer must remain
        unchanged on end_ts overrun.
        """
        it, mgr = _mk_iter(tmp_path, end_ts=5000)
        it._last_ts = 4000
        it._last_close = 100.0
        it._ingest_row(6000, 200.0, 210.0, 190.0, 205.0, 500.0)
        assert mgr.flush_calls == 1
        assert list(it._closed_buffer) == []
