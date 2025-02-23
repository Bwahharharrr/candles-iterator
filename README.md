# Candle Iterator 📊

A high-performance Python tool for analyzing financial candlestick data across multiple timeframes. Built for traders and analysts who need to process and analyze time-series market data efficiently.

## 🌟 Features

- **Multi-Exchange Support**: Process data from any exchange that follows the standard OHLCV format
- **Multi-Timeframe Analysis**: Analyze data across multiple timeframes simultaneously
- **Flexible Timeframe Expressions**: Support for expressions like ">=1h", "<=4h"
- **Memory Efficient**: Uses iterators to process large datasets without loading everything into memory
- **Dual Interface**: Both command-line and Python API available
- **Real-time Processing**: Process data as it becomes available

## 🚀 Installation

```bash
# From source
git clone https://github.com/yourusername/candle-iterator.git
cd candle-iterator
pip install -e .
```

## 💻 Usage

### Command Line Interface

```bash
# Basic usage
candle-iterator --exchange BITFINEX --ticker tBTCUSD --timeframe 1h

# Analyze specific date range
candle-iterator \
    --exchange BITFINEX \
    --ticker tBTCUSD \
    --timeframe 1h \
    --start "2024-02-17 00:00" \
    --end "2024-02-17 23:00"

# Custom analysis timeframes
candle-iterator \
    --exchange BITFINEX \
    --ticker tBTCUSD \
    --timeframe 1h \
    --analysis-tfs ">=1h" "<=12h" "1D"
```

### Python API

```python
from candle_iterator import analyze_candle_data

# Basic usage
for closure in analyze_candle_data(
    exchange="BITFINEX",
    ticker="tBTCUSD",
    base_timeframe="1h",
    analysis_timeframes=[">=1h"]
):
    hourly = closure.get_candle("1h")
    print(f"Hourly close at {hourly.datetime}: {hourly.close}")

# Advanced usage with multiple timeframes
for closure in analyze_candle_data(
    exchange="BITFINEX",
    ticker="tBTCUSD",
    base_timeframe="1h",
    analysis_timeframes=["1h", "4h", "1D"],
    start_date="2024-02-17 00:00",
    end_date="2024-02-17 23:00"
):
    print(f"\nClosure at: {closure.datetime}")
    for tf in closure.timeframes:
        candle = closure.get_candle(tf)
        print(f"{tf} close: {candle.close}")
```

## 📊 Data Structure

### CandleClosure
The main data structure returned by the iterator:

```python
class CandleClosure:
    """Group of candles closed at the same timestamp"""
    datetime: datetime    # Closure timestamp
    timeframes: List[str] # Available timeframes
    
    def get_candle(self, timeframe: str) -> Candle:
        """Get candle data for specific timeframe"""

class Candle:
    """Individual candle data"""
    timeframe: str    # Timeframe identifier
    timestamp: int    # Unix timestamp (ms)
    open: float      # Opening price
    high: float      # Highest price
    low: float       # Lowest price
    close: float     # Closing price
    volume: float    # Volume
    datetime: datetime  # Formatted timestamp
```

## ⏰ Supported Timeframes

### Base Timeframes
- Minutes: 1m, 5m, 15m, 30m
- Hours: 1h, 2h, 4h, 6h, 8h, 12h
- Days+: 1D, 3D, 1W, 1M

### Relational Expressions
- Greater than or equal: `>=1h`
- Less than or equal: `<=4h`
- Greater than: `>2h`
- Less than: `<12h`

## 📁 Data Directory Structure
The iterator will look for data in the following directory structure:
```
~/.corky/
└── {exchange}/
    └── candles/
        └── {ticker}/
            └── {timeframe}/
                └── YYYY-MM-DD.csv
```

### CSV Format
Each daily file should contain the following columns:
- timestamp (milliseconds)
- open
- high
- low
- close
- volume

## 🔧 Requirements

- Python ≥ 3.8
- Dependencies:
  - numpy
  - colorama

## 📝 License

MIT License - See LICENSE file for details

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Development Setup
1. Fork the repository
2. Create a virtual environment
3. Install development dependencies:
```bash
pip install -e ".[dev]"
```

### Running Tests
```bash
pytest tests/
```

## 📚 Documentation

For more detailed documentation, please visit our [Wiki](https://github.com/yourusername/candle-iterator/wiki)

## 🐛 Issues

Found a bug? Please [open an issue](https://github.com/yourusername/candle-iterator/issues)

## ✨ Acknowledgments

Special thanks to all contributors who have helped make this project better.