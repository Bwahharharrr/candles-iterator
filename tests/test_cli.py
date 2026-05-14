"""Tests for the CLI (`candle_iterator/cli.py`).

Phase P1-7 of the test-coverage plan: pin parse_args defaults, main()
normalization, run_cli exit codes, and KeyboardInterrupt handling.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from candle_iterator import cli
from candle_iterator.candle_iterator import CandleClosure


# --------------------------------------------------------------------------- #
# parse_args defaults
# --------------------------------------------------------------------------- #


class TestParseArgsDefaults:
    def test_required_args_only(self):
        args = cli.parse_args(["--exchange", "BITFINEX", "--ticker", "tBTCUSD"])
        assert args.exchange == "BITFINEX"
        assert args.ticker == "tBTCUSD"
        assert args.base_timeframe == "1m"  # default
        assert args.agg is None  # not provided
        assert args.start_date is None
        assert args.end_date is None
        assert args.data_dir == "~/.corky"
        assert args.verbose is False
        assert args.poll_interval is None

    def test_legacy_timeframe_alias(self):
        args = cli.parse_args([
            "--exchange", "BITFINEX", "--ticker", "tBTCUSD",
            "--timeframe", "1h",
        ])
        assert args.base_timeframe == "1h"

    def test_agg_alias(self):
        args = cli.parse_args([
            "--exchange", "X", "--ticker", "Y",
            "--aggregation-tfs", "5m", "1h",
        ])
        assert args.agg == ["5m", "1h"]

    def test_poll_interval_alias(self):
        args = cli.parse_args([
            "--exchange", "X", "--ticker", "Y",
            "--poll-interval-seconds", "5",
        ])
        assert args.poll_interval == 5

    def test_missing_required_exits(self):
        with pytest.raises(SystemExit):
            cli.parse_args([])

    def test_verbose_flag(self):
        args = cli.parse_args(["--exchange", "X", "--ticker", "Y", "-v"])
        assert args.verbose is True


# --------------------------------------------------------------------------- #
# main() normalization
# --------------------------------------------------------------------------- #


class TestMainNormalization:
    def test_main_uppercases_exchange_and_defaults_agg(self):
        with patch.object(cli, "run_cli", return_value=0) as mock_run:
            with pytest.raises(SystemExit) as exc_info:
                cli.main([
                    "--exchange", "bitfinex",  # lowercase
                    "--ticker", "tBTCUSD",
                ])
        assert exc_info.value.code == 0
        # run_cli was called with the normalized exchange and agg defaulted
        _, kwargs = mock_run.call_args
        assert kwargs["exchange"] == "BITFINEX"
        assert kwargs["agg_tokens"] == ["1m"]  # defaulted to base_timeframe


# --------------------------------------------------------------------------- #
# run_cli exit codes
# --------------------------------------------------------------------------- #


class TestRunCliExitCodes:
    def test_construction_failure_returns_exit_code_2(self):
        with patch.object(cli, "create_candle_iterator", side_effect=Exception("bad")):
            code = cli.run_cli(
                exchange="X", ticker="Y", base_timeframe="1m",
                agg_tokens=["1m"], start_date=None, end_date=None,
                data_dir="/tmp", verbose=False, poll_interval=None,
            )
        assert code == 2

    def test_iteration_failure_returns_exit_code_3(self):
        def bad_iter():
            yield CandleClosure(timestamp=100, closed_candles={}, open_candles={})
            raise RuntimeError("mid-stream failure")
        with patch.object(cli, "create_candle_iterator", return_value=bad_iter()):
            code = cli.run_cli(
                exchange="X", ticker="Y", base_timeframe="1m",
                agg_tokens=["1m"], start_date=None, end_date=None,
                data_dir="/tmp", verbose=False, poll_interval=None,
            )
        assert code == 3

    def test_successful_iteration_returns_exit_code_0(self):
        def good_iter():
            yield CandleClosure(timestamp=100, closed_candles={}, open_candles={})
            yield CandleClosure(timestamp=200, closed_candles={}, open_candles={})
        with patch.object(cli, "create_candle_iterator", return_value=good_iter()):
            code = cli.run_cli(
                exchange="X", ticker="Y", base_timeframe="1m",
                agg_tokens=["1m"], start_date=None, end_date=None,
                data_dir="/tmp", verbose=False, poll_interval=None,
            )
        assert code == 0

    def test_keyboard_interrupt_returns_exit_code_0(self):
        def interrupted_iter():
            yield CandleClosure(timestamp=100, closed_candles={}, open_candles={})
            raise KeyboardInterrupt
        with patch.object(cli, "create_candle_iterator", return_value=interrupted_iter()):
            code = cli.run_cli(
                exchange="X", ticker="Y", base_timeframe="1m",
                agg_tokens=["1m"], start_date=None, end_date=None,
                data_dir="/tmp", verbose=False, poll_interval=None,
            )
        # KeyboardInterrupt is handled gracefully and returns 0
        assert code == 0


# --------------------------------------------------------------------------- #
# One-line summaries
# --------------------------------------------------------------------------- #


class TestOneLineSummaries:
    def test_run_summary_contains_required_params(self):
        args = cli.parse_args([
            "--exchange", "BITFINEX", "--ticker", "tBTCUSD",
            "--timeframe", "1h", "--start", "2026-01-01",
        ])
        s = cli._one_line_run_summary(args)
        assert "BITFINEX" in s
        assert "tBTCUSD" in s
        assert "1h" in s
        assert "2026-01-01" in s

    def test_sync_summary_includes_exchange_ticker_timeframe(self):
        args = cli.parse_args([
            "--exchange", "BITFINEX", "--ticker", "tBTCUSD",
            "--timeframe", "1h",
        ])
        s = cli._one_line_sync_summary(args)
        assert "BITFINEX" in s
        assert "tBTCUSD" in s
        assert "1h" in s


# --------------------------------------------------------------------------- #
# print_closure does not raise on minimal CandleClosure
# --------------------------------------------------------------------------- #


class TestPrintClosure:
    def test_print_closure_on_minimal_object(self, capsys):
        cc = CandleClosure(timestamp=100, closed_candles={}, open_candles={})
        cli.print_closure(cc)  # should not raise
        captured = capsys.readouterr()
        # Some output is produced
        assert len(captured.out) > 0
