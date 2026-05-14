"""Tests for create_candle_iterator validation and auto-insertion.

Phase P1-6 of the test-coverage plan: pin the four validation/normalization
paths in `create_candle_iterator` (lines 1971-1991).

Targets:
  - Invalid base TF -> ValueError.
  - Aggregation TF smaller than base -> ValueError.
  - Unparseable agg_timeframes -> ValueError "No valid aggregation timeframes".
  - Base TF auto-insertion when missing from parsed agg list.
"""
from __future__ import annotations

import pytest

from candle_iterator import candle_iterator as ci_mod
from candle_iterator.candle_iterator import create_candle_iterator


def _stub_sync(monkeypatch):
    monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)


def _mk_dir(tmp_path, base_tf):
    p = tmp_path / "EODHD" / "candles" / "TEST.US" / base_tf
    p.mkdir(parents=True)
    return p


# --------------------------------------------------------------------------- #
# Invalid base timeframe
# --------------------------------------------------------------------------- #


class TestInvalidBaseTimeframe:
    def test_unknown_base_tf_raises(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        with pytest.raises(ValueError, match="Invalid base timeframe"):
            create_candle_iterator(
                exchange="EODHD", ticker="TEST.US",
                base_timeframe="42x", aggregation_timeframes=["42x"],
                data_dir=str(tmp_path),
            )

    def test_empty_base_tf_raises(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        with pytest.raises(ValueError, match="Invalid base timeframe"):
            create_candle_iterator(
                exchange="EODHD", ticker="TEST.US",
                base_timeframe="", aggregation_timeframes=["1m"],
                data_dir=str(tmp_path),
            )


# --------------------------------------------------------------------------- #
# Aggregation smaller than base
# --------------------------------------------------------------------------- #


class TestAggregationSmallerThanBase:
    def test_agg_smaller_than_base_raises(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        _mk_dir(tmp_path, "1h")
        # base=1h (3.6M ms); agg includes 5m (300k ms) which is smaller
        with pytest.raises(ValueError, match="smaller than base"):
            create_candle_iterator(
                exchange="EODHD", ticker="TEST.US",
                base_timeframe="1h", aggregation_timeframes=["5m"],
                data_dir=str(tmp_path),
            )

    def test_agg_via_relation_smaller_than_base_raises(self, tmp_path, monkeypatch):
        """`<1h` expands to {1m, 5m, 15m, 30m}, all smaller than base=1h."""
        _stub_sync(monkeypatch)
        _mk_dir(tmp_path, "1h")
        with pytest.raises(ValueError, match="smaller than base"):
            create_candle_iterator(
                exchange="EODHD", ticker="TEST.US",
                base_timeframe="1h", aggregation_timeframes=["<1h"],
                data_dir=str(tmp_path),
            )


# --------------------------------------------------------------------------- #
# Unparseable aggregation tokens
# --------------------------------------------------------------------------- #


class TestEmptyParsedAgg:
    def test_all_unparseable_tokens_raises(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        with pytest.raises(ValueError, match="No valid aggregation timeframes"):
            create_candle_iterator(
                exchange="EODHD", ticker="TEST.US",
                base_timeframe="1m",
                aggregation_timeframes=["garbage", "blah"],
                data_dir=str(tmp_path),
            )


# --------------------------------------------------------------------------- #
# Base TF auto-insertion
# --------------------------------------------------------------------------- #


class TestBaseTfAutoInsertion:
    def test_base_inserted_when_missing_from_agg(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        _mk_dir(tmp_path, "1D")
        it = create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1D",
            aggregation_timeframes=["1W"],  # base=1D NOT in agg list
            data_dir=str(tmp_path),
        )
        # The manager should have base_tf=1D and 1W as higher TF
        assert it.manager.base_tf == "1D"
        higher_tf_names = [a.tf for a in it.manager.higher_aggs]
        assert higher_tf_names == ["1W"]

    def test_base_not_double_inserted_when_present(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        _mk_dir(tmp_path, "1D")
        it = create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1D",
            aggregation_timeframes=["1D", "1W"],
            data_dir=str(tmp_path),
        )
        # 1D is the base; higher_aggs should NOT include 1D
        higher_tf_names = [a.tf for a in it.manager.higher_aggs]
        assert "1D" not in higher_tf_names
        assert higher_tf_names == ["1W"]


# --------------------------------------------------------------------------- #
# Exchange normalization
# --------------------------------------------------------------------------- #


class TestExchangeNormalization:
    def test_exchange_uppercased(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        # Note: data dir uses uppercased exchange; we must create the upper-case path
        (tmp_path / "EODHD" / "candles" / "TEST.US" / "1m").mkdir(parents=True)
        it = create_candle_iterator(
            exchange="eodhd",  # lowercase
            ticker="TEST.US",
            base_timeframe="1m", aggregation_timeframes=["1m"],
            data_dir=str(tmp_path),
        )
        assert it.cfg.exchange == "EODHD"


# --------------------------------------------------------------------------- #
# Data dir validation (constructor enforces existing dir)
# --------------------------------------------------------------------------- #


class TestDataDirValidation:
    def test_missing_data_dir_raises_value_error(self, tmp_path, monkeypatch):
        _stub_sync(monkeypatch)
        # Do NOT create the data directory
        with pytest.raises(ValueError, match="Data directory not found"):
            create_candle_iterator(
                exchange="EODHD", ticker="TEST.US",
                base_timeframe="1m", aggregation_timeframes=["1m"],
                data_dir=str(tmp_path),
            )
