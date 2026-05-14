"""Tests for synchronize_candle_data wrapper idempotency.

Phase P1-9 of the test-coverage plan: pin that
`create_candle_iterator` wraps `synchronize_candle_data` at most once,
based on the `_return_traced` marker.

Target: candle_iterator.py:2072-2099.
"""
from __future__ import annotations

import pytest

from candle_iterator import candle_iterator as ci_mod
from candle_iterator.candle_iterator import create_candle_iterator


def _mk_dir(tmp_path, base_tf="1m"):
    p = tmp_path / "EODHD" / "candles" / "TEST.US" / base_tf
    p.mkdir(parents=True)
    return p


def _reset_sync_module_state(monkeypatch, sync_fn):
    """Replace the module-level `synchronize_candle_data` and ensure no
    `_return_traced` marker is present (so wrapping can fire).
    """
    # Remove any prior marker on the existing module function (if any)
    monkeypatch.setattr(ci_mod, "synchronize_candle_data", sync_fn)


# --------------------------------------------------------------------------- #
# Wrapper installed exactly once when CANDLES_SYNC_TRACE is truthy
# --------------------------------------------------------------------------- #


class TestWrapperIdempotency:
    def test_first_call_wraps_when_tracing_enabled(self, tmp_path, monkeypatch):
        """With CANDLES_SYNC_TRACE truthy, the wrapper is installed on the
        first factory call. The module-level `synchronize_candle_data` becomes
        the wrapped function (with `_return_traced=True`).
        """
        monkeypatch.delenv("CANDLES_SYNC_POLL_INTERVAL_SECS", raising=False)
        monkeypatch.setenv("CANDLES_SYNC_TRACE", "1")
        # Provide an un-wrapped sync fn
        def plain_sync(**kw):
            return True
        # Pre-condition: plain_sync has no _return_traced marker
        assert not getattr(plain_sync, "_return_traced", False)
        _reset_sync_module_state(monkeypatch, plain_sync)
        _mk_dir(tmp_path)
        it = create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1m", aggregation_timeframes=["1m"],
            data_dir=str(tmp_path),
        )
        # Post-condition: module-level `synchronize_candle_data` is now wrapped
        # (carries the marker)
        assert getattr(ci_mod.synchronize_candle_data, "_return_traced", False) is True

    def test_second_call_does_not_re_wrap(self, tmp_path, monkeypatch):
        """When the module-level fn already has `_return_traced=True`, the
        guard at line 2080 prevents re-wrapping. The current sync fn is the
        same object before and after the second factory call.
        """
        monkeypatch.delenv("CANDLES_SYNC_POLL_INTERVAL_SECS", raising=False)
        monkeypatch.setenv("CANDLES_SYNC_TRACE", "1")
        # Pre-mark a fn as already wrapped
        def already_wrapped(**kw):
            return True
        already_wrapped._return_traced = True
        _reset_sync_module_state(monkeypatch, already_wrapped)
        _mk_dir(tmp_path)
        create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1m", aggregation_timeframes=["1m"],
            data_dir=str(tmp_path),
        )
        # The module-level fn is the SAME object (not wrapped again)
        assert ci_mod.synchronize_candle_data is already_wrapped

    def test_no_wrap_when_tracing_disabled_and_not_verbose(self, tmp_path, monkeypatch):
        """When CANDLES_SYNC_TRACE is unset/falsy and verbose=False, the
        wrapper is NOT installed.
        """
        monkeypatch.delenv("CANDLES_SYNC_TRACE", raising=False)
        monkeypatch.delenv("CANDLES_SYNC_POLL_INTERVAL_SECS", raising=False)
        def plain_sync(**kw):
            return True
        _reset_sync_module_state(monkeypatch, plain_sync)
        _mk_dir(tmp_path)
        create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1m", aggregation_timeframes=["1m"],
            data_dir=str(tmp_path),
            verbose=False,
        )
        # Module-level fn is still plain_sync (no wrapper installed)
        assert ci_mod.synchronize_candle_data is plain_sync
        assert not getattr(ci_mod.synchronize_candle_data, "_return_traced", False)

    def test_wrap_when_verbose_even_without_trace_env(self, tmp_path, monkeypatch):
        """verbose=True triggers wrapping (line 2072: `should_wrap = ... or verbose`)."""
        monkeypatch.delenv("CANDLES_SYNC_TRACE", raising=False)
        monkeypatch.delenv("CANDLES_SYNC_POLL_INTERVAL_SECS", raising=False)
        def plain_sync(**kw):
            return True
        _reset_sync_module_state(monkeypatch, plain_sync)
        _mk_dir(tmp_path)
        create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1m", aggregation_timeframes=["1m"],
            data_dir=str(tmp_path),
            verbose=True,
        )
        # Wrapper installed
        assert getattr(ci_mod.synchronize_candle_data, "_return_traced", False) is True
