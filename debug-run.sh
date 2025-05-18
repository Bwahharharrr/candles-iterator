#!/bin/bash

set -e

# —— Color definitions ——
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

EXCHANGE="BITFINEX"

usage() {
    echo -e "${YELLOW}Usage:${NC} $0 --ticker <TICKER> --timeframe <TIMEFRAME> [--aggregate] [--verbose]"
    echo -e "${YELLOW}Options:${NC}"
    echo -e "  ${GREEN}--ticker${NC}      Ticker symbol (required)"
    echo -e "  ${GREEN}--timeframe${NC}   Timeframe: 1m, 1h, or 1d (required)"
    echo -e "  ${GREEN}--aggregate${NC}   Add appropriate --aggregation-tfs flag"
    echo -e "  ${GREEN}--verbose${NC}     Pass --verbose to the Python script"
    echo -e "  ${GREEN}--help${NC}        Show this help message"
    exit 1
}

# If no arguments given, show help
if [ "$#" -eq 0 ]; then
    usage
fi

# Defaults
AGGREGATE=false
VERBOSE=false

# —— Parse arguments ——
while [[ $# -gt 0 ]]; do
    case "$1" in
        --ticker)
            if [[ -n $2 && $2 != --* ]]; then
                TICKER=$2
                shift 2
            else
                echo -e "${RED}Error:${NC} --ticker requires an argument"
                usage
            fi
            ;;
        --timeframe)
            if [[ -n $2 && $2 != --* ]]; then
                TIMEFRAME=$2
                shift 2
            else
                echo -e "${RED}Error:${NC} --timeframe requires an argument"
                usage
            fi
            ;;
        --aggregate)
            AGGREGATE=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --help)
            usage
            ;;
        *)
            echo -e "${RED}Error:${NC} Unknown option: $1"
            usage
            ;;
    esac
done

# —— Validate required flags ——
if [[ -z $TICKER ]]; then
    echo -e "${RED}Error:${NC} --ticker is required"
    usage
fi
if [[ -z $TIMEFRAME ]]; then
    echo -e "${RED}Error:${NC} --timeframe is required"
    usage
fi

# —— Validate timeframe value ——
if [[ "$TIMEFRAME" != "1m" && "$TIMEFRAME" != "1h" && "$TIMEFRAME" != "1d" ]]; then
    echo -e "${RED}Error:${NC} Invalid timeframe: $TIMEFRAME. Allowed: 1m, 1h, 1d"
    usage
fi

# —— Determine aggregation parameter if requested ——
if $AGGREGATE; then
    case "$TIMEFRAME" in
        1m) AGG_TFS="5m" ;;
        1h) AGG_TFS="2h" ;;
        1d) AGG_TFS="2D" ;;
    esac
fi

# —— Determine CSV filename ——
YEAR=$(date -u +%Y)
MONTH=$(date -u +%m)
DAY=$(date -u +%d)

case "$TIMEFRAME" in
    1m) FILENAME="$YEAR-$MONTH-$DAY.csv" ;;
    1h) FILENAME="$YEAR-$MONTH.csv" ;;
    1d) FILENAME="$YEAR.csv" ;;
esac

CSV_FILE="$HOME/.corky/$EXCHANGE/candles/$TICKER/$TIMEFRAME/$FILENAME"

# —— Build and run Python command ——
PYTHON_CMD=(python example.py --exchange "$EXCHANGE" --ticker "$TICKER" --timeframe "$TIMEFRAME")
if $AGGREGATE; then
    PYTHON_CMD+=(--aggregation-tfs "$AGG_TFS")
fi
if $VERBOSE; then
    PYTHON_CMD+=(--verbose)
fi

echo -e "${GREEN}Running:${NC} ${PYTHON_CMD[*]}"
"${PYTHON_CMD[@]}"

# —— Print UTC time and CSV tail ——
echo -e "\n\n${YELLOW}Output of date -u:${NC}"
date -u

echo -e "\n\n${YELLOW}Last 10 lines of CSV (${CSV_FILE}):${NC}"
if [ ! -f "$CSV_FILE" ]; then
    echo -e "${RED}Error:${NC} CSV file not found: $CSV_FILE"
    exit 1
fi

tail -n 10 "$CSV_FILE" | awk '{
    ts=$1/1000;
    cmd="date -u -d @" ts " +\"[%Y-%m-%d %H:%M:%S UTC]\"";
    cmd | getline dt;
    close(cmd);
    print dt, "->", $0
}'

exit 0
