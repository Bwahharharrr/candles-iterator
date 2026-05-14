"""Tests for the HISTORICAL → LIVE catch-up flip in CandleIterator.__next__.

Phase P0-6 of the test-coverage plan: pin the catch-up phase mechanics
introduced in commit ecbd19f.

Targets:
  - CandleIterator.__next__               line 1324-1397
  - _suppress_poll_print/_restore_poll_print  line 1483-1503
  - restore_sync_logging (public API)     line 1479-1481

Strategy: instantiate CandleIterator directly with an empty data dir
(making `_candles_gen` immediately exhaust), then monkey-patch
`_poll_for_new_data_and_buffer` to control catch-up closures and
inspect the flip lifecycle deterministically.
"""
from __future__ import annotations

from typing import List, NamedTuple, Optional

import pytest

from candle_iterator.candle_iterator import (
    AggregatorManager,
    CandleClosure,
    CandleClosureSource,
    CandleIterator,
    Config,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


class PollArgs(NamedTuple):
    sleep_first: bool
    resync: bool
    stop_after_first_emission: bool


def _mk_iter(tmp_path, *, end_ts=None, poll_interval=1, callback=None):
    """Build a CandleIterator with empty data dir and a real AggregatorManager."""
    data_path = tmp_path / "EODHD" / "candles" / "TEST.US" / "1m"
    data_path.mkdir(parents=True)
    cfg = Config(
        exchange="EODHD",
        ticker="TEST.US",
        base_timeframe="1m",
        start_ts=None,
        end_ts=end_ts,
        data_dir=str(tmp_path),
        verbose=False,
        poll_interval_seconds=poll_interval,
    )
    mgr = AggregatorManager(base_tf="1m", higher_tfs=[])
    it = CandleIterator(cfg, mgr, on_before_initial_resync=callback)
    return it, mgr


def _make_closure(ts, source=CandleClosureSource.HISTORICAL):
    return CandleClosure(
        timestamp=ts, closed_candles={}, open_candles={},
        source=source,
    )


def _install_fake_poll(it, *, on_call=None):
    """Replace _poll_for_new_data_and_buffer with a recorder.
    `on_call` is invoked with (poll_index, it) so the test can populate
    `it._closed_buffer` per-call.
    """
    calls: List[PollArgs] = []

    def fake_poll(*, sleep_first, resync, stop_after_first_emission):
        calls.append(PollArgs(sleep_first, resync, stop_after_first_emission))
        if on_call is not None:
            on_call(len(calls) - 1, it)

    it._poll_for_new_data_and_buffer = fake_poll
    return calls


# --------------------------------------------------------------------------- #
# A. Initial state
# --------------------------------------------------------------------------- #


class TestInitialState:
    def test_initial_flags(self, tmp_path):
        it, mgr = _mk_iter(tmp_path)
        assert it._catchup_drain_pending is False
        assert it._polling_notice_emitted is False
        assert it._poll_print_suppressed is False
        assert mgr.closure_source == CandleClosureSource.HISTORICAL
        assert it._final_flush_done is False


# --------------------------------------------------------------------------- #
# B. First-poll entry path
# --------------------------------------------------------------------------- #


class TestFirstPollEntryPath:
    def test_first_next_call_fires_callback_and_first_poll(self, tmp_path):
        callback_log = []
        it, mgr = _mk_iter(
            tmp_path, callback=lambda: callback_log.append("fired"),
        )
        # Fake poll: first call enqueues two HISTORICAL closures
        def populate(idx, iterator):
            if idx == 0:
                for ts in [100, 200]:
                    iterator._closed_buffer.append(
                        _make_closure(ts, source=mgr.closure_source)
                    )
        calls = _install_fake_poll(it, on_call=populate)

        first = next(it)
        # Callback fired exactly once
        assert callback_log == ["fired"]
        # First poll invoked with the catch-up-drain signature
        assert len(calls) == 1
        assert calls[0] == PollArgs(
            sleep_first=False, resync=True, stop_after_first_emission=False
        )
        # _polling_notice_emitted toggled
        assert it._polling_notice_emitted is True
        # _catchup_drain_pending set
        assert it._catchup_drain_pending is True
        # Returned closure is the first buffered one (HISTORICAL)
        assert first.source == CandleClosureSource.HISTORICAL
        # Source still HISTORICAL (flip only after buffer empties)
        assert mgr.closure_source == CandleClosureSource.HISTORICAL

    def test_catchup_closures_drain_as_historical_no_extra_polls(self, tmp_path):
        """All N catch-up closures retain HISTORICAL source as they drain,
        and NO extra polls happen during the drain (buffer popleft branch).
        """
        it, mgr = _mk_iter(tmp_path)
        def populate(idx, iterator):
            if idx == 0:
                for ts in [100, 200, 300]:
                    iterator._closed_buffer.append(
                        _make_closure(ts, source=mgr.closure_source)
                    )
        calls = _install_fake_poll(it, on_call=populate)

        drained = [next(it), next(it), next(it)]
        assert all(cc.source == CandleClosureSource.HISTORICAL for cc in drained)
        # Only the first-poll call: no extra polls during drain
        assert len(calls) == 1
        # Manager source still HISTORICAL at this point
        assert mgr.closure_source == CandleClosureSource.HISTORICAL


# --------------------------------------------------------------------------- #
# C. LIVE flip when buffer empties
# --------------------------------------------------------------------------- #


class TestLiveFlip:
    def test_flip_to_live_when_buffer_empties(self, tmp_path):
        """After buffer empties + _catchup_drain_pending=True, next __next__
        call flips source to LIVE BEFORE the next poll runs.
        """
        it, mgr = _mk_iter(tmp_path)
        def populate(idx, iterator):
            if idx == 0:
                # Catch-up: 1 closure
                iterator._closed_buffer.append(
                    _make_closure(100, source=mgr.closure_source)
                )
            elif idx == 1:
                # Post-flip poll: 1 closure stamped with current (LIVE) source
                iterator._closed_buffer.append(
                    _make_closure(200, source=mgr.closure_source)
                )
        _install_fake_poll(it, on_call=populate)

        first = next(it)
        assert first.source == CandleClosureSource.HISTORICAL
        # Now buffer is empty, _catchup_drain_pending=True
        assert it._catchup_drain_pending is True
        # Second __next__: should flip BEFORE the next poll
        second = next(it)
        # After the flip, manager source is LIVE
        assert mgr.closure_source == CandleClosureSource.LIVE
        # Closure stamped at poll-time was LIVE
        assert second.source == CandleClosureSource.LIVE
        # Flag cleared
        assert it._catchup_drain_pending is False

    def test_empty_first_poll_then_flip_on_next_call(self, tmp_path):
        """If the first poll returns no closures, the pending flag is set but
        the flip itself happens only on the NEXT __next__ call's entry.

        This test drives TWO __next__ calls and asserts both the intermediate
        state (flag set, source still HISTORICAL) and the post-second-call
        state (flag cleared, source LIVE).
        """
        it, mgr = _mk_iter(tmp_path)
        def populate(idx, iterator):
            # First poll empty → __next__ loops back. To prevent infinite
            # loop, second poll (within same __next__ call) populates.
            if idx == 1:
                iterator._closed_buffer.append(
                    _make_closure(500, source=mgr.closure_source)
                )
        _install_fake_poll(it, on_call=populate)
        first = next(it)
        # The closure was stamped during the 2nd poll, before the flip
        # (flip only triggers at top of NEXT __next__).
        assert first.source == CandleClosureSource.HISTORICAL
        assert it._catchup_drain_pending is True
        assert mgr.closure_source == CandleClosureSource.HISTORICAL

        # Drive a second __next__: flag flips on entry, then polls populate.
        def populate2(idx, iterator):
            iterator._closed_buffer.append(
                _make_closure(600, source=mgr.closure_source)
            )
        it._poll_for_new_data_and_buffer = lambda *, sleep_first, resync, stop_after_first_emission: populate2(0, it)
        second = next(it)
        # Now post-flip: closure stamped as LIVE
        assert second.source == CandleClosureSource.LIVE
        assert it._catchup_drain_pending is False
        assert mgr.closure_source == CandleClosureSource.LIVE


# --------------------------------------------------------------------------- #
# D. Callback fires exactly once
# --------------------------------------------------------------------------- #


class TestCallbackFiresOnce:
    def test_callback_not_fired_a_second_time(self, tmp_path):
        callback_log = []
        it, mgr = _mk_iter(
            tmp_path, callback=lambda: callback_log.append("fired"),
        )
        def populate(idx, iterator):
            # Always enqueue something so __next__ returns
            iterator._closed_buffer.append(
                _make_closure(100 + idx, source=mgr.closure_source)
            )
        _install_fake_poll(it, on_call=populate)
        next(it)  # First call: fires callback
        # Drain to make buffer empty + flip
        next(it)  # Second call: flips to LIVE, polls again
        next(it)  # Third call: another poll, but no callback
        assert callback_log == ["fired"]  # exactly once

    def test_none_callback_does_not_raise(self, tmp_path):
        it, mgr = _mk_iter(tmp_path, callback=None)
        def populate(idx, iterator):
            iterator._closed_buffer.append(_make_closure(100))
        _install_fake_poll(it, on_call=populate)
        # Should not raise even though callback is None
        next(it)


# --------------------------------------------------------------------------- #
# E. Subsequent polls use normal cadence (or redrain)
# --------------------------------------------------------------------------- #


class TestSubsequentPollCadence:
    def test_post_catchup_poll_uses_normal_cadence(self, tmp_path):
        it, mgr = _mk_iter(tmp_path)
        def populate(idx, iterator):
            iterator._closed_buffer.append(
                _make_closure(100 + idx, source=mgr.closure_source)
            )
        calls = _install_fake_poll(it, on_call=populate)
        next(it)  # First poll: catch-up sig
        assert calls[0] == PollArgs(
            sleep_first=False, resync=True, stop_after_first_emission=False
        )
        next(it)  # Second poll: normal cadence
        assert calls[1] == PollArgs(
            sleep_first=True, resync=True, stop_after_first_emission=True
        )

    def test_redrain_branch_skips_sleep_and_resync(self, tmp_path):
        """If `_redrain_after_first_emission=True`, the poll uses
        (sleep_first=False, resync=False, stop_after_first_emission=True).
        Note: __next__ itself does NOT clear this flag (the real
        _poll_for_new_data_and_buffer does, at line 1535). Pin both:
        the redrain branch fires, and a real implementation would reset
        the flag (which our fake doesn't — so we verify the branch was
        taken, not the flag state after).
        """
        it, mgr = _mk_iter(tmp_path)
        def populate(idx, iterator):
            iterator._closed_buffer.append(
                _make_closure(100 + idx, source=mgr.closure_source)
            )
        calls = _install_fake_poll(it, on_call=populate)
        next(it)  # First poll: catch-up
        # Manually set redrain hint
        it._redrain_after_first_emission = True
        next(it)  # Second poll: redrain signature
        assert calls[1] == PollArgs(
            sleep_first=False, resync=False, stop_after_first_emission=True
        )
        # Reset the flag manually (simulating what real poll does) and
        # verify next iteration returns to normal cadence
        it._redrain_after_first_emission = False
        next(it)  # Third poll: normal cadence again
        assert calls[2] == PollArgs(
            sleep_first=True, resync=True, stop_after_first_emission=True
        )


# --------------------------------------------------------------------------- #
# F. Finite mode never enters catch-up
# --------------------------------------------------------------------------- #


class TestFiniteModeNoCatchup:
    def test_finite_with_end_ts_raises_StopIteration_no_callback(self, tmp_path):
        callback_log = []
        it, mgr = _mk_iter(
            tmp_path, end_ts=999999999, poll_interval=1,
            callback=lambda: callback_log.append("fired"),
        )
        calls = _install_fake_poll(it)
        with pytest.raises(StopIteration):
            next(it)
        # Callback never fired (we're not in polling mode)
        assert callback_log == []
        # _polling_notice_emitted never set
        assert it._polling_notice_emitted is False
        # _final_flush_done set
        assert it._final_flush_done is True
        # No polls made
        assert calls == []

    def test_polling_disabled_path_no_callback(self, tmp_path):
        callback_log = []
        it, mgr = _mk_iter(
            tmp_path, poll_interval=None,
            callback=lambda: callback_log.append("fired"),
        )
        calls = _install_fake_poll(it)
        with pytest.raises(StopIteration):
            next(it)
        assert callback_log == []
        assert calls == []
        assert it._final_flush_done is True


# --------------------------------------------------------------------------- #
# G. _suppress_poll_print / _restore_poll_print symmetry
# --------------------------------------------------------------------------- #


class TestPollPrintSuppression:
    def test_suppress_replaces_poll_print_and_restore_undoes(self, tmp_path, monkeypatch):
        """Use monkeypatch to save/restore the real poll_print attribute so
        no global state leaks across the suite.
        """
        it, _mgr = _mk_iter(tmp_path)
        import candles_sync.candles_sync as _cs_mod
        sentinel = lambda msg: msg
        monkeypatch.setattr(_cs_mod, "poll_print", sentinel, raising=False)

        assert it._poll_print_suppressed is False
        it._suppress_poll_print()
        assert it._poll_print_suppressed is True
        assert _cs_mod.poll_print is not sentinel  # replaced
        assert _cs_mod.poll_print("test") is None  # no-op
        it.restore_sync_logging()
        assert it._poll_print_suppressed is False
        assert _cs_mod.poll_print is sentinel  # restored

    def test_restore_is_noop_when_not_suppressed(self, tmp_path, monkeypatch):
        it, _mgr = _mk_iter(tmp_path)
        import candles_sync.candles_sync as _cs_mod
        sentinel = lambda msg: msg
        monkeypatch.setattr(_cs_mod, "poll_print", sentinel, raising=False)
        # Without prior suppress, restore should be no-op
        it._restore_poll_print()
        assert _cs_mod.poll_print is sentinel
        assert it._poll_print_suppressed is False

    def test_first_poll_path_suppresses_during_poll_not_after(
        self, tmp_path, monkeypatch
    ):
        """`_suppress_poll_print` must run BEFORE
        `_poll_for_new_data_and_buffer` so the resync subprocess's prints are
        suppressed. Pin the ordering by checking the suppression flag inside
        the fake poll callback.
        """
        import candles_sync.candles_sync as _cs_mod
        sentinel = lambda msg: msg
        monkeypatch.setattr(_cs_mod, "poll_print", sentinel, raising=False)

        it, mgr = _mk_iter(tmp_path)
        suppression_observed_during_poll = []

        def populate(idx, iterator):
            # Capture suppression state AT poll time
            suppression_observed_during_poll.append(iterator._poll_print_suppressed)
            iterator._closed_buffer.append(
                _make_closure(100, source=mgr.closure_source)
            )

        _install_fake_poll(it, on_call=populate)
        next(it)
        # Suppression flag was True DURING the first poll, not just after
        assert suppression_observed_during_poll == [True]
        # Cleanup
        it.restore_sync_logging()
