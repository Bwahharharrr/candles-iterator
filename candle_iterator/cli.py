#!/usr/bin/env python3
"""
Command-line interface for the candle_iterator package.

Parses args, normalizes them, prints compact colored one-liners,
then streams CandleClosure events from create_candle_iterator().
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional

# Public API
from . import create_candle_iterator  # re-exported in package __init__
from .candle_iterator import CandleClosure  # types for printing

# Reuse color/style constants from candle_iterator.py
from .candle_iterator import (
    Fore,
    Style,
    INFO,
    WARNING,
    ERROR,
    SUCCESS,
    COLOR_VAR,
    COLOR_TYPE,
    COLOR_DESC,
    COLOR_REQ,
    COLOR_FILE,
)

# -----------------------------
# Constants
# -----------------------------
DEFAULT_DATA_DIR: str = "~/.corky"
DEFAULT_BASE_TIMEFRAME: str = "1m"
MAX_PRINTED_CLOSURES: int = 5  # print a few examples, then a summary
SEP_BULLET: str = " · "        # visual separator for one-line summaries
EMPTY_DASH: str = "—"          # nice dash for absent params in summaries
DEFAULT_POLL_INTERVAL_SECS: Optional[int] = None  # None => derive from base TF
ENV_POLL_INTERVAL_KEY: str = "CANDLES_SYNC_POLL_INTERVAL_SECS"


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="candle-iterator",
        description=(
            f"{INFO} Candle Iterator — Multi-timeframe reader & aggregator {Style.RESET_ALL}\n\n"
            "Iterate candlestick data stored under ~/.corky (or a custom root), optionally aggregating\n"
            "a base timeframe into higher timeframes.\n\n"
            f"{INFO} Required:{Style.RESET_ALL} --exchange, --ticker\n"
            f"{INFO} Defaults:{Style.RESET_ALL} --base-timeframe=1m, --agg defaults to base timeframe if omitted\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Required
    parser.add_argument("--exchange", required=True, help="Exchange code (e.g., BITFINEX)")
    parser.add_argument("--ticker", required=True, help="Symbol/pair (e.g., tBTCUSD)")

    # Base TF (accept both legacy and new flag)
    parser.add_argument(
        "--base-timeframe",
        "--timeframe",
        dest="base_timeframe",
        default=DEFAULT_BASE_TIMEFRAME,
        help=f"Base timeframe, e.g. 1m, 1h, 1D (default: {DEFAULT_BASE_TIMEFRAME})",
    )

    # Aggregation TFs (optional; will default to base TF if omitted)
    parser.add_argument(
        "--agg",
        "--aggregation-tfs",
        dest="agg",
        nargs="+",
        required=False,
        help=(
            "Aggregation timeframes tokens. Accepts exact TFs (e.g., 5m 1h 4h) or relations "
            "(e.g., '>=1h', '<=4h', or '>=1h&<=4h'). Multiple tokens are unioned; compounds use '&' intersection."
        ),
    )

    # Poll interval (optional; purely informational announcement + continuous follow)
    parser.add_argument(
        "--poll-interval",
        "--poll-interval-seconds",
        dest="poll_interval",
        type=int,
        default=DEFAULT_POLL_INTERVAL_SECS,
        help=(
            "Polling interval in seconds to use after historical file iteration completes. "
            "Defaults to the base timeframe duration if omitted. "
            f"Can also be set via the {ENV_POLL_INTERVAL_KEY} environment variable."
        ),
    )

    # Date/paths and verbosity
    parser.add_argument("--start", dest="start_date", default=None, help="Start date 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM' (UTC)")
    parser.add_argument("--end", dest="end_date", default=None, help="End date 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM' (UTC)")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR, help=f"Root data directory (default: {DEFAULT_DATA_DIR})")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    parser.add_argument("--print-all", action="store_true", help="Print every CandleClosure (very chatty)")

    return parser.parse_args(argv)


def _fmt_bool(b: bool) -> str:
    return f"{Fore.GREEN}True{Style.RESET_ALL}" if b else f"{Fore.YELLOW}False{Style.RESET_ALL}"


def _one_line_run_summary(args: argparse.Namespace) -> str:
    """
    Returns a single, colored, detailed one-liner summarizing run parameters.
    Example target (colors applied):
      [INFO] BITFINEX · tBTCUSD · base=1h · agg=[1h] · start=2025-05-05 · end=— · data=~/.corky · verbose=False
    """
    agg_list = args.agg if args.agg is not None else [args.base_timeframe]
    agg_display = "[" + ", ".join(agg_list) + "]"
    start_display = args.start_date if args.start_date else EMPTY_DASH
    end_display = args.end_date if args.end_date else EMPTY_DASH
    poll_display = f"{args.poll_interval}s" if args.poll_interval else f"auto({args.base_timeframe})"

    parts = [
        f"{Fore.CYAN}{args.exchange}{Style.RESET_ALL}",
        f"{Fore.MAGENTA}{args.ticker}{Style.RESET_ALL}",
        f"{COLOR_VAR}base{Style.RESET_ALL}={COLOR_TYPE}{args.base_timeframe}{Style.RESET_ALL}",
        f"{COLOR_VAR}agg{Style.RESET_ALL}={COLOR_TYPE}{agg_display}{Style.RESET_ALL}",
        f"{COLOR_VAR}start{Style.RESET_ALL}={COLOR_TYPE}{start_display}{Style.RESET_ALL}",
        f"{COLOR_VAR}end{Style.RESET_ALL}={COLOR_TYPE}{end_display}{Style.RESET_ALL}",
        f"{COLOR_VAR}poll{Style.RESET_ALL}={COLOR_TYPE}{poll_display}{Style.RESET_ALL}",
        f"{COLOR_VAR}data{Style.RESET_ALL}={COLOR_TYPE}{args.data_dir}{Style.RESET_ALL}",
        f"{COLOR_VAR}verbose{Style.RESET_ALL}={_fmt_bool(args.verbose)}",
    ]
    return f"{INFO} " + SEP_BULLET.join(parts) + f"{Style.RESET_ALL}"


def _one_line_sync_summary(args: argparse.Namespace) -> str:
    """
    Returns a single, colored, detailed one-liner for sync, mirroring the user's sample:
      [INFO] Running synchronization with parameters: --exchange BITFINEX --ticker tBTCUSD [Timeframe internally set to '1h']
    """
    return (
        f"{INFO} Running synchronization with parameters:{Style.RESET_ALL} "
        f"--exchange {Fore.CYAN}{args.exchange}{Style.RESET_ALL} "
        f"--ticker {Fore.MAGENTA}{args.ticker}{Style.RESET_ALL} "
        f"{Fore.YELLOW}[Timeframe internally set to '{args.base_timeframe}']{Style.RESET_ALL}"
    )


def print_closure(cc: CandleClosure) -> None:
    """Pretty-print a single CandleClosure using its built-in helpers."""
    # Use the compact one-liner by default; CandleClosure.print() remains available for deep dives.
    cc.print()
    # print(cc.print())


def run_cli(
    exchange: str,
    ticker: str,
    base_timeframe: str,
    agg_tokens: List[str],
    start_date: Optional[str],
    end_date: Optional[str],
    data_dir: str,
    verbose: bool,
    print_all: bool,
    poll_interval: Optional[int],
) -> int:
    """
    Runs the iterator and prints a sample of closures plus a summary.
    Returns a process exit code.
    """
    try:
        iterator = create_candle_iterator(
            exchange=exchange,
            ticker=ticker,
            base_timeframe=base_timeframe,
            aggregation_timeframes=agg_tokens,
            start_date=start_date,
            end_date=end_date,
            data_dir=data_dir,
            verbose=verbose,
            poll_interval_seconds=poll_interval,
        )
    except Exception as exc:
        sys.stderr.write(f"{ERROR} Failed to create iterator: {exc}{Style.RESET_ALL}\n")
        return 2

    printed = 0
    total = 0
    last_closure: Optional[CandleClosure] = None

    try:
        for cc in iterator:
            total += 1
            print_closure(cc)
    except KeyboardInterrupt:
        sys.stderr.write(f"{WARNING} Interrupted by user.{Style.RESET_ALL}\n")
    except Exception as exc:
        sys.stderr.write(f"{ERROR} Iteration failed: {exc}{Style.RESET_ALL}\n")
        return 3

    # Summary
    if last_closure is not None:
        print(f"\n===== SUMMARY =====")
        print(f"Total closures: {total}")
        print("Last closure:")
        print_closure(last_closure)
        print(f"\n{SUCCESS} Done.{Style.RESET_ALL}\n")
    else:
        print(f"{WARNING} No closures produced (check date range / data availability).{Style.RESET_ALL}")

    return 0


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    # --- Normalization / defaults ---
    args.exchange = args.exchange.upper()       # always uppercase exchange
    if not args.agg:
        args.agg = [args.base_timeframe]        # default agg to base TF

    # --- Compact colored one-liners ---
    # 1) One-line run config
    print(_one_line_run_summary(args))
    # 2) One-line sync note (create_candle_iterator handles sync internally)
    print(_one_line_sync_summary(args))

    # --- Run ---
    exit_code = run_cli(
        exchange=args.exchange,
        ticker=args.ticker,
        base_timeframe=args.base_timeframe,
        agg_tokens=args.agg,
        start_date=args.start_date,
        end_date=args.end_date,
        data_dir=args.data_dir,
        verbose=args.verbose,
        print_all=args.print_all,
        poll_interval=args.poll_interval,
    )
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
