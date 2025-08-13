#!/usr/bin/env python3
"""
Example runner for candle_iterator that delegates to the package CLI.

This keeps the example aligned with your installed console script (`candle-iterator`)
and ensures a single source of truth for argument parsing, validation, and output.

The commented-out sync block below is intentionally preserved (debug notes).
"""

from __future__ import annotations

import sys
from candle_iterator.cli import main as cli_main

# ---------------------------------------------------------------------------
# PRESERVED DEBUG/REFERENCE (commented-out) — do not remove
# 
# from candles_sync.candles_sync import synchronize_candle_data
# 
# ok = synchronize_candle_data(
#     exchange=args.exchange,
#     ticker=args.ticker,
#     timeframe=args.timeframe,
#     end_date_str=args.end,
#     verbose=True
# )
# if not ok:
#     print(f"\n[ERROR] Synchronization failed for {tf}.\n")
#     sys.exit(1)
# ---------------------------------------------------------------------------


def main() -> None:
    # Pass through command-line args to the real CLI main
    cli_main(sys.argv[1:])


if __name__ == "__main__":
    main()
