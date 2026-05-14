"""Tests for defensive contracts on CandleAggregator entry points.

Phase P2-1 of the test-coverage plan: pin that base/higher-only methods
reject the wrong aggregator type.

Targets:
  - on_base_csv_row: raises RuntimeError if called on a non-base agg.
  - on_base_candle_closed: raises RuntimeError if called on a base agg.
"""
from __future__ import annotations

import pytest

from candle_iterator.candle_iterator import AggregatorManager


class TestAggregatorContracts:
    def test_on_base_csv_row_raises_on_higher_agg(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        three_d = [a for a in mgr.higher_aggs if a.tf == "3D"][0]
        with pytest.raises(RuntimeError):
            three_d.on_base_csv_row(0, 1.0, 2.0, 0.5, 1.5, 10.0)

    def test_on_base_candle_closed_raises_on_base_agg(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"])
        with pytest.raises(RuntimeError):
            mgr.base_agg.on_base_candle_closed(0, 1.0, 2.0, 0.5, 1.5, 10.0)
