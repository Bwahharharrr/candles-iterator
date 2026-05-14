"""Tests for the polling-without-end-date branch of create_candle_iterator.

Phase P0-9 of the test-coverage plan: pin the regression fix at lines
2037-2047 that leaves `cfg.end_ts=None` when polling is enabled and no
explicit end_date is provided. Without this fix, the iterator becomes
finite and never enters polling.
"""
from __future__ import annotations

import os

import pytest

from candle_iterator import candle_iterator as ci_mod
from candle_iterator.candle_iterator import (
    ENV_POLL_INTERVAL_KEY,
    create_candle_iterator,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _stub_sync(monkeypatch):
    """Stub network sync; the factory calls it twice (during construction
    and during polling resync — only the construction call matters here)."""
    monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)


def _mk_dir(tmp_path, base_tf="1m"):
    """Create the expected base-TF data directory."""
    p = tmp_path / "EODHD" / "candles" / "TEST.US" / base_tf
    p.mkdir(parents=True)
    return p


def _clear_env(monkeypatch):
    monkeypatch.delenv(ENV_POLL_INTERVAL_KEY, raising=False)


# --------------------------------------------------------------------------- #
# Polling enabled (regression pin: end_ts stays None)
# --------------------------------------------------------------------------- #


class TestPollingEnabledKeepsEndTsNone:
    def test_arg_poll_interval_no_end_date_keeps_end_ts_none(self, tmp_path, monkeypatch):
        _clear_env(monkeypatch)
        _stub_sync(monkeypatch)
        _mk_dir(tmp_path)
        it = create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1m", aggregation_timeframes=["1m"],
            data_dir=str(tmp_path),
            poll_interval_seconds=5,
        )
        assert it.cfg.end_ts is None, (
            "Polling enabled + no end_date must preserve cfg.end_ts=None "
            "(regression pin)"
        )
        # Sanity: polling is genuinely enabled
        assert it.poll_interval_seconds == 5

    def test_env_poll_interval_no_end_date_keeps_end_ts_none(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        _mk_dir(tmp_path)
        monkeypatch.setenv(ENV_POLL_INTERVAL_KEY, "5")
        it = create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1m", aggregation_timeframes=["1m"],
            data_dir=str(tmp_path),
            poll_interval_seconds=None,
        )
        assert it.cfg.end_ts is None
        assert it.poll_interval_seconds == 5

    def test_env_takes_precedence_over_arg_keeps_end_ts_none(self, tmp_path, monkeypatch):
        """Env > arg per factory line 1995-2011: env is evaluated first; if
        valid, the arg is ignored.
        """
        _stub_sync(monkeypatch)
        _mk_dir(tmp_path)
        monkeypatch.setenv(ENV_POLL_INTERVAL_KEY, "10")
        it = create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1m", aggregation_timeframes=["1m"],
            data_dir=str(tmp_path),
            poll_interval_seconds=3,
        )
        assert it.cfg.end_ts is None
        # Env (10) wins over arg (3)
        assert it.poll_interval_seconds == 10


# --------------------------------------------------------------------------- #
# Polling disabled (legacy: implicit anchor_now_ms)
# --------------------------------------------------------------------------- #


class TestPollingDisabledImplicitEndTs:
    def test_no_poll_interval_no_end_date_sets_implicit_end_ts(self, tmp_path, monkeypatch):
        _clear_env(monkeypatch)
        _stub_sync(monkeypatch)
        _mk_dir(tmp_path)
        it = create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1m", aggregation_timeframes=["1m"],
            data_dir=str(tmp_path),
            poll_interval_seconds=None,
        )
        # Polling disabled + no end_date → implicit anchor_now_ms.
        assert it.cfg.end_ts is not None
        assert it.cfg.end_ts > 0
        # Polling disabled
        assert it.poll_interval_seconds is None


class TestSubMinPollIntervalNormalizesToDisabled:
    def test_sub_min_poll_interval_no_end_date_sets_implicit_end_ts(
        self, tmp_path, monkeypatch
    ):
        """A sub-MIN_POLL_INTERVAL_SECONDS value normalizes to
        resolved_poll_interval=None, which selects the polling-disabled
        implicit-end branch.
        """
        _clear_env(monkeypatch)
        _stub_sync(monkeypatch)
        _mk_dir(tmp_path)
        # MIN is 1, so 0 (or negative) is sub-min and rejected
        it = create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1m", aggregation_timeframes=["1m"],
            data_dir=str(tmp_path),
            poll_interval_seconds=0,  # sub-min → rejected
        )
        # Factory normalized polling to None → end_ts gets implicit value
        assert it.cfg.end_ts is not None
        # And polling is genuinely disabled
        assert it.poll_interval_seconds is None


# --------------------------------------------------------------------------- #
# Explicit end_date wins regardless of polling
# --------------------------------------------------------------------------- #


class TestExplicitEndDateAlwaysWins:
    def test_polling_disabled_explicit_end_date_used(self, tmp_path, monkeypatch):
        _clear_env(monkeypatch)
        _stub_sync(monkeypatch)
        _mk_dir(tmp_path)
        it = create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1m", aggregation_timeframes=["1m"],
            data_dir=str(tmp_path),
            end_date="2026-01-15",
            poll_interval_seconds=None,
        )
        # end_date=2026-01-15 → end-of-day 23:59:59.999 UTC
        expected = ci_mod.parse_timestamp("2026-01-15", is_start=False)
        assert it.cfg.end_ts == expected

    def test_polling_enabled_explicit_end_date_used(self, tmp_path, monkeypatch):
        _clear_env(monkeypatch)
        _stub_sync(monkeypatch)
        _mk_dir(tmp_path)
        it = create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1m", aggregation_timeframes=["1m"],
            data_dir=str(tmp_path),
            end_date="2026-01-15",
            poll_interval_seconds=5,
        )
        # User's explicit end_date wins even with polling enabled
        expected = ci_mod.parse_timestamp("2026-01-15", is_start=False)
        assert it.cfg.end_ts == expected
        assert it.poll_interval_seconds == 5
