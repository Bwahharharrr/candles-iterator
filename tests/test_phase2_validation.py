"""Tests for Phase 2 of the file-robustness plan: validation hardening.

Covers:
  P1-1: base_timeframe restricted to SUPPORTED_BASE_TIMEFRAMES = {1m, 1h, 1D}.
  P1-2: $HOME-unset detection via expansion-failure detection.
  P1-3: path-component validation for exchange/ticker/base_timeframe.
  P1-4: csv.writer uses lineterminator="\\n" in both _append_rows and
        partition_writer._serialize_rows.
"""
from __future__ import annotations

import os

import pandas as pd
import pytest

from candle_iterator import candle_iterator as ci_mod
from candle_iterator import partition_writer as pw
from candle_iterator.candle_iterator import (
    SUPPORTED_BASE_TIMEFRAMES,
    _append_rows,
    _validate_path_component,
    create_candle_iterator,
)


def _stub_sync(monkeypatch):
    monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)


def _mk_dir(tmp_path, base_tf="1m"):
    p = tmp_path / "EODHD" / "candles" / "TEST.US" / base_tf
    p.mkdir(parents=True)
    return p


def _mk_df(rows):
    return pd.DataFrame(rows, columns=["timestamp", "open", "close", "high", "low", "volume"])


# --------------------------------------------------------------------------- #
# P1-1: TF allow-list
# --------------------------------------------------------------------------- #


class TestSupportedBaseTimeframes:
    def test_constant_value(self):
        assert SUPPORTED_BASE_TIMEFRAMES == ("1m", "1h", "1D")

    @pytest.mark.parametrize("tf", ["1m", "1h", "1D"])
    def test_supported_tfs_construct_ok(self, tmp_path, monkeypatch, tf):
        _stub_sync(monkeypatch)
        _mk_dir(tmp_path, tf)
        it = create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe=tf, aggregation_timeframes=[tf],
            data_dir=str(tmp_path),
        )
        assert it.cfg.base_timeframe == tf

    @pytest.mark.parametrize("tf", ["5m", "15m", "30m", "2h", "4h", "1W", "3D"])
    def test_unsupported_tfs_raise_value_error(self, tmp_path, monkeypatch, tf):
        _stub_sync(monkeypatch)
        with pytest.raises(ValueError, match="Unsupported base_timeframe"):
            create_candle_iterator(
                exchange="EODHD", ticker="TEST.US",
                base_timeframe=tf, aggregation_timeframes=[tf],
                data_dir=str(tmp_path),
            )

    def test_unsupported_tf_error_lists_supported_set(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        with pytest.raises(ValueError) as exc_info:
            create_candle_iterator(
                exchange="EODHD", ticker="TEST.US",
                base_timeframe="5m", aggregation_timeframes=["5m"],
                data_dir=str(tmp_path),
            )
        msg = str(exc_info.value)
        # Mentions all 3 supported tfs
        assert "1m" in msg and "1h" in msg and "1D" in msg

    def test_invalid_tf_still_raises_invalid_first(self, tmp_path, monkeypatch):
        """Unknown TFs (not in TIMEFRAMES) still get the existing 'Invalid'
        error, not the 'Unsupported' one. Ordering: TIMEFRAMES check first.
        """
        _stub_sync(monkeypatch)
        with pytest.raises(ValueError, match="Invalid base timeframe"):
            create_candle_iterator(
                exchange="EODHD", ticker="TEST.US",
                base_timeframe="42x", aggregation_timeframes=["1m"],
                data_dir=str(tmp_path),
            )


# --------------------------------------------------------------------------- #
# P1-2: $HOME-unset detection
# --------------------------------------------------------------------------- #


class TestHomeUnsetDetection:
    def test_expanduser_failure_raises(self, monkeypatch):
        """Pin the production rule directly: if `os.path.expanduser` returns
        the input unchanged AND the input starts with `~`, raise. Monkeypatch
        expanduser so the test is deterministic on any platform (no
        dependency on $HOME / $USERPROFILE / pwd-module fallback).
        """
        monkeypatch.setattr(
            ci_mod.os.path, "expanduser",
            lambda s: s,  # simulate "no home directory available"
        )
        with pytest.raises(ValueError, match="home-directory expansion"):
            create_candle_iterator(
                exchange="EODHD", ticker="TEST.US",
                base_timeframe="1m", aggregation_timeframes=["1m"],
                data_dir="~/.corky",
            )

    def test_explicit_absolute_data_dir_skips_home_check(self, tmp_path, monkeypatch):
        """An absolute data_dir doesn't start with `~`, so the home-check is
        skipped entirely (even with $HOME stripped).
        """
        monkeypatch.delenv("HOME", raising=False)
        _stub_sync(monkeypatch)
        _mk_dir(tmp_path)
        # Should not raise even with no $HOME
        create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1m", aggregation_timeframes=["1m"],
            data_dir=str(tmp_path),  # absolute
        )

    def test_home_set_with_tilde_data_dir_proceeds(self, tmp_path, monkeypatch):
        """$HOME set + ~/.corky data_dir → expansion succeeds, no early
        ValueError fires. (Subsequent steps may fail because the expanded
        dir doesn't exist, but the home-check itself passes.)
        """
        monkeypatch.setenv("HOME", str(tmp_path))
        _stub_sync(monkeypatch)
        # Expansion will be `tmp_path/.corky/EODHD/.../1m`; doesn't exist →
        # raises "Data directory not found", NOT the home-unset error.
        with pytest.raises(ValueError, match="Data directory not found"):
            create_candle_iterator(
                exchange="EODHD", ticker="TEST.US",
                base_timeframe="1m", aggregation_timeframes=["1m"],
                data_dir="~/.corky",
            )


# --------------------------------------------------------------------------- #
# P1-3: path-component validation
# --------------------------------------------------------------------------- #


class TestValidatePathComponentHelper:
    def test_simple_valid_string(self):
        # Should not raise
        _validate_path_component("name", "EODHD")
        _validate_path_component("name", "TEST.US")
        _validate_path_component("name", "1m")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            _validate_path_component("exchange", "")

    def test_non_string_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            _validate_path_component("ticker", None)
        with pytest.raises(ValueError, match="non-empty"):
            _validate_path_component("ticker", 42)

    def test_dot_dot_raises(self):
        with pytest.raises(ValueError, match="path traversal"):
            _validate_path_component("ticker", "..")

    def test_single_dot_raises(self):
        with pytest.raises(ValueError, match="path traversal"):
            _validate_path_component("ticker", ".")

    def test_dotdot_substring_allowed(self):
        """Per Codex's narrower rule: foo..bar (substring) is fine since
        path separators are rejected separately.
        """
        # Should not raise
        _validate_path_component("ticker", "foo..bar")
        _validate_path_component("ticker", "..bar")
        _validate_path_component("ticker", "bar..")

    def test_forward_slash_raises(self):
        with pytest.raises(ValueError, match="path separators"):
            _validate_path_component("exchange", "foo/bar")

    def test_back_slash_raises(self):
        with pytest.raises(ValueError, match="path separators"):
            _validate_path_component("exchange", "foo\\bar")

    def test_null_byte_raises(self):
        with pytest.raises(ValueError, match="null bytes"):
            _validate_path_component("ticker", "foo\0bar")

    def test_leading_dot_allowed(self):
        # `.evil` is not a traversal; it's a legitimate hidden-file name
        _validate_path_component("ticker", ".evil")


class TestFactoryRejectsBadComponents:
    def test_exchange_traversal_raises(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        with pytest.raises(ValueError, match="exchange"):
            create_candle_iterator(
                exchange="..", ticker="TEST.US",
                base_timeframe="1m", aggregation_timeframes=["1m"],
                data_dir=str(tmp_path),
            )

    def test_ticker_separator_raises(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        with pytest.raises(ValueError, match="ticker"):
            create_candle_iterator(
                exchange="EODHD", ticker="foo/bar",
                base_timeframe="1m", aggregation_timeframes=["1m"],
                data_dir=str(tmp_path),
            )

    def test_base_timeframe_separator_raises(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        with pytest.raises(ValueError, match="base_timeframe"):
            create_candle_iterator(
                exchange="EODHD", ticker="TEST.US",
                base_timeframe="1m/etc", aggregation_timeframes=["1m"],
                data_dir=str(tmp_path),
            )

    def test_validation_runs_before_upper_case(self, tmp_path, monkeypatch):
        """Validation must run BEFORE exchange.upper() so an attempt like
        'foo/BAR' is rejected at the exchange (not after normalization).
        """
        _stub_sync(monkeypatch)
        with pytest.raises(ValueError, match="exchange"):
            create_candle_iterator(
                exchange="foo/bar", ticker="TEST.US",
                base_timeframe="1m", aggregation_timeframes=["1m"],
                data_dir=str(tmp_path),
            )

    def test_empty_ticker_raises(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        with pytest.raises(ValueError, match="ticker"):
            create_candle_iterator(
                exchange="EODHD", ticker="",
                base_timeframe="1m", aggregation_timeframes=["1m"],
                data_dir=str(tmp_path),
            )


# --------------------------------------------------------------------------- #
# P1-4: lineterminator normalization
# --------------------------------------------------------------------------- #


class TestLineTerminator:
    def test_append_rows_uses_lf_not_crlf(self, tmp_path):
        p = tmp_path / "out.csv"
        df = _mk_df([
            (1000, 1.0, 1.5, 2.0, 0.5, 10.0),
            (2000, 2.0, 2.5, 3.0, 1.5, 20.0),
        ])
        _append_rows(str(p), df, include_header=True)
        raw = p.read_bytes()
        # No \r\n anywhere
        assert b"\r\n" not in raw
        # \n present (line terminator)
        assert raw.count(b"\n") >= 2

    def test_partition_writer_uses_lf_not_crlf(self, tmp_path):
        p = tmp_path / "pw.csv"
        df = _mk_df([
            (1000, 1.0, 1.5, 2.0, 0.5, 10.0),
            (2000, 2.0, 2.5, 3.0, 1.5, 20.0),
        ])
        pw.write_batch(str(p), df)
        raw = p.read_bytes()
        assert b"\r\n" not in raw
        assert raw.count(b"\n") >= 2

    def test_partition_writer_append_preserves_lf(self, tmp_path):
        p = tmp_path / "pw.csv"
        # Manually create a file with LF endings
        p.write_text("timestamp,open,close,high,low,volume\n1000,1.0,1.5,2.0,0.5,10.0\n")
        df = _mk_df([(2000, 2.0, 2.5, 3.0, 1.5, 20.0)])
        pw.write_batch(str(p), df)
        raw = p.read_bytes()
        assert b"\r\n" not in raw
