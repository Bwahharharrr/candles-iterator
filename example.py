#!/usr/bin/env python3
"""
Example script demonstrating how to use the candle_iterator library.
This provides a command-line interface for the candle iterator functionality.
"""

import argparse
import sys
from datetime import datetime, timezone
from candle_iterator import create_candle_iterator, TIMEFRAMES
from candle_iterator.candle_iterator import Fore, Style, INFO, ERROR, COLOR_VAR, COLOR_TYPE, COLOR_REQ, COLOR_DESC, COLOR_FILE
from candles_sync.candles_sync import synchronize_candle_data


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description=f"""
{INFO} Candle Iterator CLI {Style.RESET_ALL}
Iterate through candlestick data from the .corky data directory.

{INFO} Required Parameters: {Style.RESET_ALL}
  {COLOR_VAR}--exchange{Style.RESET_ALL}        {COLOR_TYPE}(str){Style.RESET_ALL}    {COLOR_REQ} {COLOR_DESC}Exchange name{Style.RESET_ALL}
  {COLOR_VAR}--ticker{Style.RESET_ALL}          {COLOR_TYPE}(str){Style.RESET_ALL}    {COLOR_REQ} {COLOR_DESC}Trading pair{Style.RESET_ALL}
  {COLOR_VAR}--timeframe{Style.RESET_ALL}        {COLOR_TYPE}(str){Style.RESET_ALL}    {COLOR_REQ} {COLOR_DESC}Base timeframe (e.g., 1m, 1h, 1D){Style.RESET_ALL}
  {COLOR_VAR}--aggregation-tfs{Style.RESET_ALL}  {COLOR_TYPE}(str){Style.RESET_ALL}           {COLOR_DESC}Timeframes to aggregate (e.g., 1h 4h >=2h&<=12h){Style.RESET_ALL}
  {COLOR_VAR}--start{Style.RESET_ALL}            {COLOR_TYPE}(str){Style.RESET_ALL}           {COLOR_DESC}Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM){Style.RESET_ALL}
  {COLOR_VAR}--end{Style.RESET_ALL}              {COLOR_TYPE}(str){Style.RESET_ALL}           {COLOR_DESC}End date (YYYY-MM-DD or YYYY-MM-DD HH:MM){Style.RESET_ALL}
  {COLOR_VAR}--data-dir{Style.RESET_ALL}         {COLOR_TYPE}(str){Style.RESET_ALL}           {COLOR_DESC}Base directory for candle data (default: ~/.corky){Style.RESET_ALL}

{INFO} Example Usage: {Style.RESET_ALL}
  {COLOR_FILE}python example.py --exchange BITFINEX --ticker tBTCUSD --timeframe 1h{Style.RESET_ALL}
  {COLOR_FILE}python example.py --exchange BITFINEX --ticker tBTCUSD --timeframe 1h --aggregation-tfs 1h 4h 1D{Style.RESET_ALL}
  {COLOR_FILE}python example.py --exchange BITFINEX --ticker tBTCUSD --timeframe 1h --aggregation-tfs ">=2h&<=12h"{Style.RESET_ALL}
  {COLOR_FILE}python example.py --exchange BITFINEX --ticker tBTCUSD --timeframe 1h --start "2024-01-01" --end "2024-02-01"{Style.RESET_ALL}
""",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument("--exchange", required=True, help="Exchange name")
    parser.add_argument("--ticker", required=True, help="Trading pair")
    parser.add_argument("--timeframe", required=True, help="Base timeframe")
    parser.add_argument("--aggregation-tfs", nargs="+", default=None,
                      help="Timeframes to aggregate data for (e.g., 1h 4h >=2h&<=12h). Defaults to base timeframe if not specified.")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM)")
    parser.add_argument("--data-dir", default="~/.corky",
                      help="Base directory for candle data")
    parser.add_argument("--verbose", action="store_true",
                      help="Print detailed debug information")
    
    if len(sys.argv) == 1:
        print(f"\n{ERROR} No arguments provided! Please specify the required parameters.\n")
        parser.print_help()
        sys.exit(1)
        
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Print configuration summary
    print(f"\n{INFO} Running candle iterator with the following parameters:\n")
    print(f"  {COLOR_VAR}--exchange{Style.RESET_ALL}         {COLOR_TYPE}(str){Style.RESET_ALL}  {args.exchange}")
    print(f"  {COLOR_VAR}--ticker{Style.RESET_ALL}           {COLOR_TYPE}(str){Style.RESET_ALL}  {args.ticker}")
    print(f"  {COLOR_VAR}--timeframe{Style.RESET_ALL}        {COLOR_TYPE}(str){Style.RESET_ALL}  {args.timeframe}")
    
    aggregation_tfs = args.aggregation_tfs if args.aggregation_tfs is not None else [args.timeframe]
    print(f"  {COLOR_VAR}--aggregation-tfs{Style.RESET_ALL}  {COLOR_TYPE}(list){Style.RESET_ALL} {' '.join(aggregation_tfs)}")
    
    if args.start:
        print(f"  {COLOR_VAR}--start{Style.RESET_ALL}            {COLOR_TYPE}(str){Style.RESET_ALL}  {args.start}")
    if args.end:
        print(f"  {COLOR_VAR}--end{Style.RESET_ALL}              {COLOR_TYPE}(str){Style.RESET_ALL}  {args.end}")
    
    print(f"  {COLOR_VAR}--data-dir{Style.RESET_ALL}         {COLOR_TYPE}(str){Style.RESET_ALL}  {args.data_dir}")
    print(f"  {COLOR_VAR}--verbose{Style.RESET_ALL}          {COLOR_TYPE}(bool){Style.RESET_ALL} {args.verbose}")
    print(f"\n{INFO} Starting candle iterator...\n")

#'    ok = synchronize_candle_data(
#'        exchange=args.exchange,
#'        ticker=args.ticker,
#'        timeframe=args.timeframe,
#'        end_date_str=args.end,
#'        verbose=True
#'    )
#'    if not ok:
#'        print(f"\n{ERROR} Synchronization failed for {tf}.\n")
#'        sys.exit(1)

    try:
        for closure in create_candle_iterator(
            exchange=args.exchange,
            ticker=args.ticker,
            base_timeframe=args.timeframe,
            aggregation_timeframes=aggregation_tfs,
            start_date=args.start,
            end_date=args.end,
            data_dir=args.data_dir,
            verbose=args.verbose
        ):
            # Get current time in milliseconds to check if candles are closed
            closure.print()
            pass 

    except ValueError as e:
        print(f"{ERROR} {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
