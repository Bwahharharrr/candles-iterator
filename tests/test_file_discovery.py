"""Tests for _list_csv_files and _file_day_range.

Phase P1-5 of the test-coverage plan. (_current_partition_basename is
covered in P0-7's test_tail_follow.py.)

Targets:
  - _list_csv_files()                 line 1743-1785
  - _file_day_range(filename)         line 1704-1741
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from candle_iterator.candle_iterator import (
    AggregatorManager,
    CandleIterator,
    Config,
)


DAY_MS = 86_400_000


def _utc_ms(year, month, day, hour=0, minute=0, second=0, ms=0):
    dt = datetime(year, month, day, hour, minute, second, ms * 1000,
                  tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _mk_iter(tmp_path, *, base_tf="1m", start_ts=None, end_ts=None):
    data_path = tmp_path / "EODHD" / "candles" / "TEST.US" / base_tf
    data_path.mkdir(parents=True)
    cfg = Config(
        exchange="EODHD", ticker="TEST.US", base_timeframe=base_tf,
        start_ts=start_ts, end_ts=end_ts, data_dir=str(tmp_path),
        verbose=False, poll_interval_seconds=None,
    )
    mgr = AggregatorManager(base_tf=base_tf, higher_tfs=[])
    return CandleIterator(cfg, mgr), data_path


# --------------------------------------------------------------------------- #
# A. _file_day_range
# --------------------------------------------------------------------------- #


class TestFileDayRange1m:
    def test_1m_daily_range(self, tmp_path):
        it, _ = _mk_iter(tmp_path, base_tf="1m")
        start, end = it._file_day_range("2026-05-14.csv")
        assert start == _utc_ms(2026, 5, 14, 0, 0, 0, 0)
        assert end == _utc_ms(2026, 5, 14, 23, 59, 59, 999)

    def test_1m_end_of_month(self, tmp_path):
        it, _ = _mk_iter(tmp_path, base_tf="1m")
        start, end = it._file_day_range("2026-01-31.csv")
        assert start == _utc_ms(2026, 1, 31, 0, 0, 0, 0)
        assert end == _utc_ms(2026, 1, 31, 23, 59, 59, 999)


class TestFileDayRange1h:
    def test_1h_monthly_range(self, tmp_path):
        it, _ = _mk_iter(tmp_path, base_tf="1h")
        start, end = it._file_day_range("2026-05.csv")
        assert start == _utc_ms(2026, 5, 1, 0, 0, 0, 0)
        # End: 2026-05-31 23:59:59.999 = 2026-06-01 00:00:00 - 1ms
        assert end == _utc_ms(2026, 5, 31, 23, 59, 59, 999)

    def test_1h_december_rolls_to_next_year(self, tmp_path):
        it, _ = _mk_iter(tmp_path, base_tf="1h")
        start, end = it._file_day_range("2026-12.csv")
        assert start == _utc_ms(2026, 12, 1, 0, 0, 0, 0)
        assert end == _utc_ms(2026, 12, 31, 23, 59, 59, 999)


class TestFileDayRange1D:
    def test_1D_yearly_range(self, tmp_path):
        it, _ = _mk_iter(tmp_path, base_tf="1D")
        start, end = it._file_day_range("2026.csv")
        assert start == _utc_ms(2026, 1, 1, 0, 0, 0, 0)
        assert end == _utc_ms(2026, 12, 31, 23, 59, 59, 999)


# --------------------------------------------------------------------------- #
# B. _list_csv_files
# --------------------------------------------------------------------------- #


class TestListCsvFiles1m:
    def test_no_files(self, tmp_path):
        it, _ = _mk_iter(tmp_path, base_tf="1m")
        assert it._list_csv_files() == []

    def test_three_files_sorted_ascending(self, tmp_path):
        it, data_path = _mk_iter(tmp_path, base_tf="1m")
        # Create files out-of-order
        for name in ["2026-05-16.csv", "2026-05-14.csv", "2026-05-15.csv"]:
            (data_path / name).write_text("")
        files = it._list_csv_files()
        assert [p.split("/")[-1] for p in files] == [
            "2026-05-14.csv", "2026-05-15.csv", "2026-05-16.csv"
        ]

    def test_ignores_non_matching_filenames(self, tmp_path):
        it, data_path = _mk_iter(tmp_path, base_tf="1m")
        (data_path / "2026-05-14.csv").write_text("")
        # Non-matching (for 1m's daily regex)
        (data_path / "junk.csv").write_text("")
        (data_path / "2026-05.csv").write_text("")  # month-only
        (data_path / "2026.csv").write_text("")    # year-only
        (data_path / "README.md").write_text("")
        files = it._list_csv_files()
        assert len(files) == 1
        assert files[0].endswith("2026-05-14.csv")

    def test_start_ts_filter_excludes_earlier_files(self, tmp_path):
        # start_ts = 2026-05-15 00:00:00 → files with end < start_ts excluded
        start_ts = _utc_ms(2026, 5, 15, 0, 0, 0, 0)
        it, data_path = _mk_iter(tmp_path, base_tf="1m", start_ts=start_ts)
        for name in ["2026-05-14.csv", "2026-05-15.csv", "2026-05-16.csv"]:
            (data_path / name).write_text("")
        files = it._list_csv_files()
        # 2026-05-14 ends at 23:59:59.999 of that day < 2026-05-15 00:00 → excluded
        names = [p.split("/")[-1] for p in files]
        assert "2026-05-14.csv" not in names
        assert "2026-05-15.csv" in names
        assert "2026-05-16.csv" in names

    def test_end_ts_filter_excludes_later_files(self, tmp_path):
        # end_ts = 2026-05-15 23:59:59.999 → files with start > end_ts excluded
        end_ts = _utc_ms(2026, 5, 15, 23, 59, 59, 999)
        it, data_path = _mk_iter(tmp_path, base_tf="1m", end_ts=end_ts)
        for name in ["2026-05-14.csv", "2026-05-15.csv", "2026-05-16.csv"]:
            (data_path / name).write_text("")
        files = it._list_csv_files()
        names = [p.split("/")[-1] for p in files]
        assert "2026-05-14.csv" in names
        assert "2026-05-15.csv" in names
        # 2026-05-16 starts at 00:00:00 > end_ts → excluded
        assert "2026-05-16.csv" not in names

    def test_window_filter_returns_overlap_only(self, tmp_path):
        start_ts = _utc_ms(2026, 5, 14, 0, 0, 0, 0)
        end_ts = _utc_ms(2026, 5, 15, 23, 59, 59, 999)
        it, data_path = _mk_iter(
            tmp_path, base_tf="1m", start_ts=start_ts, end_ts=end_ts
        )
        for name in ["2026-05-13.csv", "2026-05-14.csv",
                     "2026-05-15.csv", "2026-05-16.csv"]:
            (data_path / name).write_text("")
        files = it._list_csv_files()
        names = [p.split("/")[-1] for p in files]
        assert names == ["2026-05-14.csv", "2026-05-15.csv"]


class TestListCsvFiles1h:
    def test_1h_filters_monthly_files(self, tmp_path):
        it, data_path = _mk_iter(tmp_path, base_tf="1h")
        # Mix: valid monthly files + non-matching
        (data_path / "2026-05.csv").write_text("")
        (data_path / "2026-06.csv").write_text("")
        (data_path / "2026-05-14.csv").write_text("")  # daily; should be ignored
        (data_path / "2026.csv").write_text("")        # yearly; ignored
        files = it._list_csv_files()
        names = sorted(p.split("/")[-1] for p in files)
        assert names == ["2026-05.csv", "2026-06.csv"]


class TestListCsvFiles1D:
    def test_1D_filters_yearly_files(self, tmp_path):
        it, data_path = _mk_iter(tmp_path, base_tf="1D")
        (data_path / "2025.csv").write_text("")
        (data_path / "2026.csv").write_text("")
        (data_path / "2026-05.csv").write_text("")    # monthly; ignored
        (data_path / "2026-05-14.csv").write_text("")  # daily; ignored
        files = it._list_csv_files()
        names = sorted(p.split("/")[-1] for p in files)
        assert names == ["2025.csv", "2026.csv"]
