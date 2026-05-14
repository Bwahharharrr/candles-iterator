"""Tests for CandleIterator.__next__ finite-stream StopIteration path.

Phase P1-3 of the test-coverage plan: pin the final-flush + idempotency
contract at lines 1337-1346.

Target finite modes:
  - cfg.end_ts is not None
  - poll_interval_seconds is None (polling disabled)
"""
from __future__ import annotations

from typing import List

import pytest

from candle_iterator.candle_iterator import (
    AggregatorManager,
    CandleClosure,
    CandleIterator,
    Config,
)


def _mk_iter(tmp_path, *, end_ts=None, poll_interval=None):
    data_path = tmp_path / "EODHD" / "candles" / "TEST.US" / "1m"
    data_path.mkdir(parents=True)
    cfg = Config(
        exchange="EODHD", ticker="TEST.US", base_timeframe="1m",
        start_ts=None, end_ts=end_ts, data_dir=str(tmp_path),
        verbose=False, poll_interval_seconds=poll_interval,
    )
    mgr = AggregatorManager(base_tf="1m", higher_tfs=[])
    return CandleIterator(cfg, mgr), mgr


def _install_flush_spy(mgr, *, returns=None):
    calls = []
    queue = list(returns) if returns else []
    original = mgr.flush
    def spy():
        calls.append(True)
        if queue:
            return queue.pop(0)
        return original()
    mgr.flush = spy
    return calls


def _make_closure(ts):
    return CandleClosure(timestamp=ts, closed_candles={}, open_candles={})


# --------------------------------------------------------------------------- #
# Finite mode: end_ts set
# --------------------------------------------------------------------------- #


class TestFiniteEndTsMode:
    def test_empty_gen_with_end_ts_flushes_then_stops(self, tmp_path):
        it, mgr = _mk_iter(tmp_path, end_ts=999999, poll_interval=None)
        flush_calls = _install_flush_spy(mgr, returns=[
            [_make_closure(100), _make_closure(200)]
        ])
        first = next(it)
        second = next(it)
        # First two next() calls return the flushed closures FIFO
        assert first.timestamp == 100
        assert second.timestamp == 200
        # Third call → StopIteration
        with pytest.raises(StopIteration):
            next(it)
        # flush called exactly once
        assert len(flush_calls) == 1

    def test_empty_flush_raises_stop_immediately(self, tmp_path):
        it, mgr = _mk_iter(tmp_path, end_ts=999999, poll_interval=None)
        flush_calls = _install_flush_spy(mgr, returns=[[]])
        with pytest.raises(StopIteration):
            next(it)
        assert it._final_flush_done is True
        assert len(flush_calls) == 1


# --------------------------------------------------------------------------- #
# Finite mode: poll_interval=None
# --------------------------------------------------------------------------- #


class TestFinitePollDisabledMode:
    def test_empty_gen_with_poll_disabled_flushes_then_stops(self, tmp_path):
        it, mgr = _mk_iter(tmp_path, end_ts=None, poll_interval=None)
        flush_calls = _install_flush_spy(mgr, returns=[[_make_closure(50)]])
        first = next(it)
        assert first.timestamp == 50
        with pytest.raises(StopIteration):
            next(it)
        assert len(flush_calls) == 1


# --------------------------------------------------------------------------- #
# Idempotency: flush exactly once across many StopIteration calls
# --------------------------------------------------------------------------- #


class TestFlushIdempotent:
    def test_flush_called_exactly_once_across_many_stops(self, tmp_path):
        it, mgr = _mk_iter(tmp_path, end_ts=999999, poll_interval=None)
        flush_calls = _install_flush_spy(mgr, returns=[[]])
        # Call next() many times after exhaustion; flush called only once
        for _ in range(5):
            with pytest.raises(StopIteration):
                next(it)
        assert len(flush_calls) == 1

    def test_final_flush_done_set_even_on_empty_flush(self, tmp_path):
        it, mgr = _mk_iter(tmp_path, end_ts=999999, poll_interval=None)
        _install_flush_spy(mgr, returns=[[]])
        with pytest.raises(StopIteration):
            next(it)
        assert it._final_flush_done is True


# --------------------------------------------------------------------------- #
# Buffered-before-exhaust ordering
# --------------------------------------------------------------------------- #


class TestBufferedBeforeExhaustOrdering:
    def test_existing_buffer_drains_before_flush_called(self, tmp_path):
        """If _closed_buffer already has closures (e.g., from a prior ingest),
        they are emitted FIRST. The final flush only runs once the buffer is
        empty AND the generator is exhausted.
        """
        it, mgr = _mk_iter(tmp_path, end_ts=999999, poll_interval=None)
        pre_buffered = _make_closure(10)
        it._closed_buffer.append(pre_buffered)
        flush_calls = _install_flush_spy(mgr, returns=[[_make_closure(99)]])
        # First call: returns the pre-buffered closure WITHOUT calling flush
        first = next(it)
        assert first is pre_buffered
        assert len(flush_calls) == 0  # flush not yet called
        # Second call: gen exhausts → flush called → returns flushed closure
        second = next(it)
        assert second.timestamp == 99
        assert len(flush_calls) == 1
        # Third call: StopIteration
        with pytest.raises(StopIteration):
            next(it)


# --------------------------------------------------------------------------- #
# Polling-enabled mode does NOT take the finite path
# --------------------------------------------------------------------------- #


class TestPollingEnabledIsNotFinite:
    def test_polling_enabled_with_no_end_ts_does_not_flush_on_first_exhaust(
        self, tmp_path
    ):
        """When polling is enabled, the StopIteration branch transitions to
        polling (not final flush). This is the negative case for P1-3.
        """
        it, mgr = _mk_iter(tmp_path, end_ts=None, poll_interval=5)
        flush_calls = _install_flush_spy(mgr)
        # Stub the polling path so we don't do real IO
        it._poll_for_new_data_and_buffer = (
            lambda *, sleep_first, resync, stop_after_first_emission:
            it._closed_buffer.append(_make_closure(200))
        )
        first = next(it)
        # First call transitions to polling, returns a polled closure
        assert first.timestamp == 200
        # flush NOT called (we're in polling mode, not finite)
        assert len(flush_calls) == 0
        assert it._final_flush_done is False
