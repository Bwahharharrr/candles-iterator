"""Tests for CandleIterator._resolve_poll_interval_seconds.

Phase P0-10 of the test-coverage plan: pin env > cfg > None precedence,
sub-MIN_POLL_INTERVAL_SECONDS rejection, parser-asymmetry between env
(`int(float(...))`) and cfg (`int(...)`).

Target: candle_iterator/candle_iterator.py:1449-1477.
"""
from __future__ import annotations

import pytest

from candle_iterator.candle_iterator import (
    ENV_POLL_INTERVAL_KEY,
    MIN_POLL_INTERVAL_SECONDS,
    AggregatorManager,
    CandleIterator,
    Config,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _mk_iter(tmp_path, *, poll_interval_seconds=None):
    data_path = tmp_path / "EODHD" / "candles" / "TEST.US" / "1m"
    data_path.mkdir(parents=True)
    cfg = Config(
        exchange="EODHD", ticker="TEST.US", base_timeframe="1m",
        start_ts=None, end_ts=None, data_dir=str(tmp_path),
        verbose=False, poll_interval_seconds=poll_interval_seconds,
    )
    mgr = AggregatorManager(base_tf="1m", higher_tfs=[])
    return CandleIterator(cfg, mgr)


def _clear_env(monkeypatch):
    monkeypatch.delenv(ENV_POLL_INTERVAL_KEY, raising=False)


# --------------------------------------------------------------------------- #
# Env precedence over cfg
# --------------------------------------------------------------------------- #


class TestEnvPrecedence:
    def test_env_valid_int_wins_over_cfg(self, tmp_path, monkeypatch):
        monkeypatch.setenv(ENV_POLL_INTERVAL_KEY, "10")
        it = _mk_iter(tmp_path, poll_interval_seconds=3)
        assert it._resolve_poll_interval_seconds() == 10

    def test_env_only(self, tmp_path, monkeypatch):
        monkeypatch.setenv(ENV_POLL_INTERVAL_KEY, "5")
        it = _mk_iter(tmp_path, poll_interval_seconds=None)
        assert it._resolve_poll_interval_seconds() == 5

    def test_env_float_string_truncated_to_int(self, tmp_path, monkeypatch):
        monkeypatch.setenv(ENV_POLL_INTERVAL_KEY, "2.9")
        it = _mk_iter(tmp_path, poll_interval_seconds=None)
        # Env uses int(float(...)) → 2
        assert it._resolve_poll_interval_seconds() == 2


# --------------------------------------------------------------------------- #
# Env falls back to cfg on invalid/empty/sub-min
# --------------------------------------------------------------------------- #


class TestEnvFallback:
    def test_env_non_numeric_falls_back_to_cfg(self, tmp_path, monkeypatch):
        monkeypatch.setenv(ENV_POLL_INTERVAL_KEY, "abc")
        it = _mk_iter(tmp_path, poll_interval_seconds=7)
        assert it._resolve_poll_interval_seconds() == 7

    def test_env_zero_falls_back_to_cfg(self, tmp_path, monkeypatch):
        """`"0"` is truthy as a string but 0 < MIN_POLL_INTERVAL_SECONDS=1
        so it's rejected and cfg is consulted.
        """
        monkeypatch.setenv(ENV_POLL_INTERVAL_KEY, "0")
        it = _mk_iter(tmp_path, poll_interval_seconds=4)
        assert it._resolve_poll_interval_seconds() == 4

    def test_env_negative_falls_back_to_cfg(self, tmp_path, monkeypatch):
        monkeypatch.setenv(ENV_POLL_INTERVAL_KEY, "-1")
        it = _mk_iter(tmp_path, poll_interval_seconds=4)
        assert it._resolve_poll_interval_seconds() == 4

    def test_env_empty_string_falls_back_to_cfg(self, tmp_path, monkeypatch):
        """Empty env value is falsy via `if env_val:` → treated as unset."""
        monkeypatch.setenv(ENV_POLL_INTERVAL_KEY, "")
        it = _mk_iter(tmp_path, poll_interval_seconds=4)
        assert it._resolve_poll_interval_seconds() == 4


# --------------------------------------------------------------------------- #
# Cfg-only (env unset)
# --------------------------------------------------------------------------- #


class TestCfgOnly:
    def test_cfg_valid_int(self, tmp_path, monkeypatch):
        _clear_env(monkeypatch)
        it = _mk_iter(tmp_path, poll_interval_seconds=5)
        assert it._resolve_poll_interval_seconds() == 5

    def test_cfg_none_returns_none(self, tmp_path, monkeypatch):
        _clear_env(monkeypatch)
        it = _mk_iter(tmp_path, poll_interval_seconds=None)
        assert it._resolve_poll_interval_seconds() is None

    def test_cfg_float_truncated_to_int(self, tmp_path, monkeypatch):
        """Cfg uses `int(...)` directly on the value. A float cfg truncates."""
        _clear_env(monkeypatch)
        it = _mk_iter(tmp_path, poll_interval_seconds=2.9)
        # int(2.9) == 2; 2 >= MIN → returns 2
        assert it._resolve_poll_interval_seconds() == 2

    def test_cfg_sub_min_returns_none(self, tmp_path, monkeypatch):
        _clear_env(monkeypatch)
        it = _mk_iter(tmp_path, poll_interval_seconds=0)
        assert it._resolve_poll_interval_seconds() is None

    def test_cfg_negative_returns_none(self, tmp_path, monkeypatch):
        _clear_env(monkeypatch)
        it = _mk_iter(tmp_path, poll_interval_seconds=-1)
        assert it._resolve_poll_interval_seconds() is None


# --------------------------------------------------------------------------- #
# Parser asymmetry: env handles float strings, cfg does not
# --------------------------------------------------------------------------- #


class TestParserAsymmetry:
    def test_cfg_float_string_raises_int_and_falls_to_none(self, tmp_path, monkeypatch):
        """Cfg uses `int(value)` directly. `int("1.9")` raises ValueError,
        which is caught → falls through to return None. Pins the asymmetry
        between env (`int(float(...))`) and cfg (`int(...)`).
        """
        _clear_env(monkeypatch)
        it = _mk_iter(tmp_path, poll_interval_seconds="1.9")
        assert it._resolve_poll_interval_seconds() is None

    def test_env_float_string_works(self, tmp_path, monkeypatch):
        """Env tolerates `"1.9"` because of `int(float(...))`."""
        monkeypatch.setenv(ENV_POLL_INTERVAL_KEY, "1.9")
        it = _mk_iter(tmp_path, poll_interval_seconds=None)
        assert it._resolve_poll_interval_seconds() == 1


# --------------------------------------------------------------------------- #
# Both invalid → None
# --------------------------------------------------------------------------- #


class TestBothInvalid:
    def test_env_invalid_cfg_none_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.setenv(ENV_POLL_INTERVAL_KEY, "garbage")
        it = _mk_iter(tmp_path, poll_interval_seconds=None)
        assert it._resolve_poll_interval_seconds() is None

    def test_env_subminute_cfg_subminute_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.setenv(ENV_POLL_INTERVAL_KEY, "0")
        it = _mk_iter(tmp_path, poll_interval_seconds=0)
        assert it._resolve_poll_interval_seconds() is None


# --------------------------------------------------------------------------- #
# Sanity: MIN_POLL_INTERVAL_SECONDS exposed and respected
# --------------------------------------------------------------------------- #


class TestMinBoundary:
    def test_min_boundary_value_accepted(self, tmp_path, monkeypatch):
        """A value exactly equal to MIN_POLL_INTERVAL_SECONDS (=1) is accepted."""
        _clear_env(monkeypatch)
        it = _mk_iter(tmp_path, poll_interval_seconds=MIN_POLL_INTERVAL_SECONDS)
        assert it._resolve_poll_interval_seconds() == MIN_POLL_INTERVAL_SECONDS
