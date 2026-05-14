"""Tests for the fast-append CSV writer infrastructure.

Phase P0-8 of the test-coverage plan: pin the on-disk CSV persistence path
(`_read_last_row_timestamp`, `_truncate_last_line`, `_canonicalize_input_df`,
`_append_rows`, `_fast_write_partition_wrapper_factory`,
`_install_fast_append_writer`).

CSV layout pinned by `FAST_APPEND_HEADER = (timestamp, open, close, high, low, volume)`.
"""
from __future__ import annotations

import os
import sys
import types

import pandas as pd
import pytest

from candle_iterator import candle_iterator as ci_mod
from candle_iterator.candle_iterator import (
    FAST_APPEND_DISABLE_ENV,
    FAST_APPEND_HEADER,
    _append_rows,
    _canonicalize_input_df,
    _fast_write_partition_wrapper_factory,
    _install_fast_append_writer,
    _read_last_row_timestamp,
    _truncate_last_line,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _mk_df(rows):
    """rows: list of (ts, o, c, h, l, v)."""
    return pd.DataFrame(rows, columns=["timestamp", "open", "close", "high", "low", "volume"])


# --------------------------------------------------------------------------- #
# A. _read_last_row_timestamp
# --------------------------------------------------------------------------- #


class TestReadLastRowTimestamp:
    def test_non_existent_file_returns_none(self, tmp_path):
        assert _read_last_row_timestamp(str(tmp_path / "nope.csv")) is None

    def test_empty_file_returns_none(self, tmp_path):
        p = tmp_path / "empty.csv"
        p.write_text("")
        assert _read_last_row_timestamp(str(p)) is None

    def test_header_only_file_returns_none(self, tmp_path):
        p = tmp_path / "hdr.csv"
        p.write_text("timestamp,open,close,high,low,volume\n")
        assert _read_last_row_timestamp(str(p)) is None

    def test_single_data_row_returns_its_ts(self, tmp_path):
        p = tmp_path / "single.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5,2.0,0.5,10.0\n"
        )
        assert _read_last_row_timestamp(str(p)) == 1000

    def test_multiple_rows_returns_last_ts(self, tmp_path):
        p = tmp_path / "many.csv"
        content = "timestamp,open,close,high,low,volume\n"
        for ts in (1000, 2000, 3000, 4000):
            content += f"{ts},1.0,1.5,2.0,0.5,10.0\n"
        p.write_text(content)
        assert _read_last_row_timestamp(str(p)) == 4000

    def test_trailing_blank_line_walks_back(self, tmp_path):
        p = tmp_path / "blank.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5,2.0,0.5,10.0\n"
            "\n"
        )
        assert _read_last_row_timestamp(str(p)) == 1000

    def test_no_trailing_newline_reads_last_line(self, tmp_path):
        p = tmp_path / "no-nl.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5,2.0,0.5,10.0\n"
            "2000,1.0,1.5,2.0,0.5,10.0"  # no trailing newline
        )
        assert _read_last_row_timestamp(str(p)) == 2000

    def test_case_insensitive_header_check_with_data(self, tmp_path):
        p = tmp_path / "case.csv"
        p.write_text(
            "TIMESTAMP,OPEN,CLOSE,HIGH,LOW,VOLUME\n"
            "1000,1.0,1.5,2.0,0.5,10.0\n"
        )
        assert _read_last_row_timestamp(str(p)) == 1000

    def test_uppercase_header_only_returns_none(self, tmp_path):
        """Header-only files (uppercase variant) → None, just like the
        lowercase header-only case.
        """
        p = tmp_path / "hdr-upper.csv"
        p.write_text("TIMESTAMP,OPEN,CLOSE,HIGH,LOW,VOLUME\n")
        assert _read_last_row_timestamp(str(p)) is None

    def test_malformed_final_data_row_returns_none(self, tmp_path):
        """If the final line has a non-numeric first field, the walk-back
        skips it and continues. With only one malformed row + header, the
        result is None.
        """
        p = tmp_path / "bad.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "abc,1.0,1.5,2.0,0.5,10.0\n"
        )
        assert _read_last_row_timestamp(str(p)) is None

    def test_malformed_final_row_walks_back_to_prior_valid_ts(self, tmp_path):
        """If the final data row is malformed (non-numeric first field), the
        walk-back loop must skip it and return the PRIOR valid timestamp.
        """
        p = tmp_path / "trail-bad.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5,2.0,0.5,10.0\n"
            "2000,2.0,2.5,3.0,1.5,20.0\n"
            "garbage,9.9,9.5,9.0,8.5,99.0\n"
        )
        assert _read_last_row_timestamp(str(p)) == 2000


# --------------------------------------------------------------------------- #
# B. _truncate_last_line
# --------------------------------------------------------------------------- #


class TestTruncateLastLine:
    def test_trailing_newline_truncates_keeps_prior_newline(self, tmp_path):
        p = tmp_path / "tail.csv"
        p.write_text("a\nb\nc\n")
        ok = _truncate_last_line(str(p))
        assert ok is True
        # Expected: "a\nb\n"
        assert p.read_text() == "a\nb\n"

    def test_no_trailing_newline_truncates(self, tmp_path):
        p = tmp_path / "no-nl.csv"
        p.write_text("a\nb\nc")
        ok = _truncate_last_line(str(p))
        assert ok is True
        # Expected: "a\nb\n"
        assert p.read_text() == "a\nb\n"

    def test_single_line_no_newline_becomes_empty(self, tmp_path):
        p = tmp_path / "single.csv"
        p.write_text("only")
        _truncate_last_line(str(p))
        # File should become empty (size 0 or just the BOF position)
        assert p.read_text() == ""

    def test_non_existent_file_returns_false(self, tmp_path):
        ok = _truncate_last_line(str(tmp_path / "nope.csv"))
        assert ok is False

    def test_empty_file_returns_true(self, tmp_path):
        p = tmp_path / "empty.csv"
        p.write_text("")
        ok = _truncate_last_line(str(p))
        assert ok is True


# --------------------------------------------------------------------------- #
# C. _canonicalize_input_df
# --------------------------------------------------------------------------- #


class TestCanonicalizeInputDf:
    def test_canonical_columns_pass_through(self):
        df = pd.DataFrame({
            "timestamp": [3000, 1000, 2000],
            "open": [1.0, 2.0, 3.0],
            "close": [1.5, 2.5, 3.5],
            "high": [2.0, 3.0, 4.0],
            "low": [0.5, 1.5, 2.5],
            "volume": [10.0, 20.0, 30.0],
        })
        result = _canonicalize_input_df(df)
        assert result is not None
        # Sorted ascending by timestamp
        assert list(result["timestamp"]) == [1000, 2000, 3000]
        # Column order matches FAST_APPEND_HEADER
        assert list(result.columns) == list(FAST_APPEND_HEADER)

    def test_short_aliases_renamed(self):
        df = pd.DataFrame({
            "ts": [1000],
            "o": [1.0],
            "c": [1.5],
            "h": [2.0],
            "l": [0.5],
            "v": [10.0],
        })
        result = _canonicalize_input_df(df)
        assert result is not None
        assert list(result.columns) == list(FAST_APPEND_HEADER)
        assert result.iloc[0]["timestamp"] == 1000

    def test_mixed_case_column_names(self):
        df = pd.DataFrame({
            "Timestamp": [1000],
            "OPEN": [1.0],
            "Close": [1.5],
            "High": [2.0],
            "low": [0.5],
            "VOL": [10.0],
        })
        result = _canonicalize_input_df(df)
        assert result is not None
        assert list(result.columns) == list(FAST_APPEND_HEADER)

    def test_missing_required_column_returns_none(self):
        df = pd.DataFrame({
            "timestamp": [1000],
            "open": [1.0],
            # missing 'close'
            "high": [2.0],
            "low": [0.5],
            "volume": [10.0],
        })
        assert _canonicalize_input_df(df) is None

    def test_non_dataframe_returns_none(self):
        assert _canonicalize_input_df({"timestamp": [1]}) is None
        assert _canonicalize_input_df([1, 2, 3]) is None
        assert _canonicalize_input_df(None) is None

    def test_dedup_keeps_last_value_per_ts(self):
        df = pd.DataFrame({
            "timestamp": [1000, 1000, 2000],
            "open": [1.0, 99.0, 2.0],
            "close": [1.5, 99.5, 2.5],
            "high": [2.0, 99.9, 3.0],
            "low": [0.5, 99.1, 1.5],
            "volume": [10.0, 999.0, 20.0],
        })
        result = _canonicalize_input_df(df)
        assert len(result) == 2
        # The duplicate ts=1000 row should keep the LAST one (open=99.0)
        row_1000 = result[result["timestamp"] == 1000].iloc[0]
        assert row_1000["open"] == 99.0

    def test_nan_timestamps_cause_astype_failure_returns_none(self, capsys):
        """NaN in `timestamp` makes the `astype('int64')` call (line 238)
        raise BEFORE the dropna step (line 242). The exception is caught and
        the function returns None — pinning this existing behavior.
        """
        df = pd.DataFrame({
            "timestamp": [1000, None, 2000],
            "open": [1.0, 2.0, 3.0],
            "close": [1.5, 2.5, 3.5],
            "high": [2.0, 3.0, 4.0],
            "low": [0.5, 1.5, 2.5],
            "volume": [10.0, 20.0, 30.0],
        })
        assert _canonicalize_input_df(df) is None
        captured = capsys.readouterr()
        assert "_canonicalize_input_df failed" in captured.out

    def test_dtypes_enforced(self):
        df = pd.DataFrame({
            "timestamp": ["1000", "2000"],
            "open": ["1.0", "2.0"],
            "close": ["1.5", "2.5"],
            "high": ["2.0", "3.0"],
            "low": ["0.5", "1.5"],
            "volume": ["10.0", "20.0"],
        })
        result = _canonicalize_input_df(df)
        assert result is not None
        assert result["timestamp"].dtype == "int64"
        for col in ("open", "close", "high", "low", "volume"):
            assert result[col].dtype == "float64"

    def test_extra_columns_dropped(self):
        df = pd.DataFrame({
            "timestamp": [1000],
            "open": [1.0],
            "close": [1.5],
            "high": [2.0],
            "low": [0.5],
            "volume": [10.0],
            "EXTRA": ["junk"],
        })
        result = _canonicalize_input_df(df)
        assert result is not None
        assert "EXTRA" not in result.columns
        assert list(result.columns) == list(FAST_APPEND_HEADER)

    def test_all_nan_timestamps_returns_none(self):
        """All-NaN `timestamp` column: astype('int64') raises before dropna,
        function returns None via the except branch. Pinning current
        astype-before-dropna sequence.
        """
        df = pd.DataFrame({
            "timestamp": [None, None],
            "open": [1.0, 2.0],
            "close": [1.5, 2.5],
            "high": [2.0, 3.0],
            "low": [0.5, 1.5],
            "volume": [10.0, 20.0],
        })
        assert _canonicalize_input_df(df) is None

    def test_truly_empty_dataframe_returns_none(self):
        """An empty DataFrame (zero rows) with all required columns:
        canonicalization should detect this and return None.
        """
        df = pd.DataFrame({
            "timestamp": pd.array([], dtype="int64"),
            "open": pd.array([], dtype="float64"),
            "close": pd.array([], dtype="float64"),
            "high": pd.array([], dtype="float64"),
            "low": pd.array([], dtype="float64"),
            "volume": pd.array([], dtype="float64"),
        })
        assert _canonicalize_input_df(df) is None


# --------------------------------------------------------------------------- #
# D. _append_rows
# --------------------------------------------------------------------------- #


class TestAppendRows:
    def test_new_file_with_header(self, tmp_path):
        p = tmp_path / "new.csv"
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        _append_rows(str(p), df, include_header=True)
        content = p.read_text()
        assert content.startswith("timestamp,open,close,high,low,volume\r\n") or \
               content.startswith("timestamp,open,close,high,low,volume\n")
        assert "1000,1.0,1.5,2.0,0.5,10.0" in content

    def test_existing_file_append_no_header(self, tmp_path):
        p = tmp_path / "exist.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5,2.0,0.5,10.0\n"
        )
        df = _mk_df([(2000, 2.0, 2.5, 3.0, 1.5, 20.0)])
        _append_rows(str(p), df, include_header=False)
        text = p.read_text()
        # Only one header line
        assert text.count("timestamp,open,close,high,low,volume") == 1
        # Both rows present
        assert "1000," in text
        assert "2000," in text

    def test_auto_creates_parent_directory(self, tmp_path):
        p = tmp_path / "sub" / "deep" / "file.csv"
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        _append_rows(str(p), df, include_header=True)
        assert p.exists()

    def test_no_header_ensures_trailing_newline_before_append(self, tmp_path):
        """If the existing file lacks a trailing newline, _ensure_trailing_newline
        is called before appending.
        """
        p = tmp_path / "no-nl.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5,2.0,0.5,10.0"  # NO trailing newline
        )
        df = _mk_df([(2000, 2.0, 2.5, 3.0, 1.5, 20.0)])
        _append_rows(str(p), df, include_header=False)
        text = p.read_text()
        # New row appears on its own line, not concatenated to prior row
        assert "10.02000," not in text  # no concatenation
        assert "10.0\n2000," in text


# --------------------------------------------------------------------------- #
# E. _fast_write_partition_wrapper
# --------------------------------------------------------------------------- #


class TestFastAppendWrapper:
    def _make_wrapper(self, original_fn=None):
        if original_fn is None:
            calls = []
            def original(*args, **kwargs):
                calls.append((args, kwargs))
                return "original_return"
            original._calls = calls  # type: ignore[attr-defined]
        else:
            original = original_fn
        return _fast_write_partition_wrapper_factory(original), original

    def test_env_disable_delegates_immediately(self, tmp_path, monkeypatch):
        wrapper, original = self._make_wrapper()
        monkeypatch.setenv(FAST_APPEND_DISABLE_ENV, "1")
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        ret = wrapper(str(tmp_path / "x.csv"), df=df)
        assert ret == "original_return"
        assert len(original._calls) == 1

    def test_path_extraction_fails_delegates(self, tmp_path):
        wrapper, original = self._make_wrapper()
        # Neither path nor df: delegate
        wrapper()
        assert len(original._calls) == 1

    def test_canonicalize_returns_none_delegates(self, tmp_path):
        wrapper, original = self._make_wrapper()
        # DataFrame missing required column
        bad_df = pd.DataFrame({"timestamp": [1000], "open": [1.0]})
        wrapper(str(tmp_path / "x.csv"), df=bad_df)
        assert len(original._calls) == 1

    def test_new_file_writes_header_and_rows(self, tmp_path):
        wrapper, original = self._make_wrapper()
        p = str(tmp_path / "new.csv")
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        ret = wrapper(p, df=df)
        # Original NOT called — fast path took over
        assert original._calls == []
        # Returns None per the wrapper contract
        assert ret is None
        # File exists with header + row
        assert os.path.exists(p)
        text = open(p).read()
        assert "timestamp,open,close,high,low,volume" in text
        assert "1000," in text

    def test_append_only_strictly_greater_ts(self, tmp_path):
        wrapper, original = self._make_wrapper()
        p = tmp_path / "x.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5,2.0,0.5,10.0\n"
        )
        df = _mk_df([(2000, 2.0, 2.5, 3.0, 1.5, 20.0)])
        wrapper(str(p), df=df)
        assert original._calls == []
        # File now has both rows, only one header
        text = p.read_text()
        assert text.count("timestamp,open,close,high,low,volume") == 1
        assert "1000," in text
        assert "2000," in text

    def test_same_ts_truncate_last_then_append(self, tmp_path):
        wrapper, original = self._make_wrapper()
        p = tmp_path / "x.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5,2.0,0.5,10.0\n"
        )
        # Incoming has same first ts → replace last line
        df = _mk_df([(1000, 99.0, 99.5, 99.9, 99.1, 999.0)])
        wrapper(str(p), df=df)
        assert original._calls == []
        text = p.read_text()
        # The old "1000,1.0,..." line should be gone; new line present
        assert "1000,1.0,1.5,2.0,0.5,10.0" not in text
        assert "1000,99.0,99.5,99.9,99.1,999.0" in text

    def test_same_ts_replace_plus_future_rows_appended(self, tmp_path):
        """When incoming has first_ts == last_on_disk_ts AND additional newer
        rows, the wrapper truncates the last line and appends ALL incoming.
        """
        wrapper, original = self._make_wrapper()
        p = tmp_path / "x.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5,2.0,0.5,10.0\n"
        )
        df = _mk_df([
            (1000, 99.0, 99.5, 99.9, 99.1, 999.0),
            (2000, 2.0, 2.5, 3.0, 1.5, 20.0),
            (3000, 3.0, 3.5, 4.0, 2.5, 30.0),
        ])
        wrapper(str(p), df=df)
        assert original._calls == []
        text = p.read_text()
        assert "1000,1.0,1.5,2.0,0.5,10.0" not in text  # old line gone
        assert "1000,99.0,99.5,99.9,99.1,999.0" in text
        assert "2000,2.0,2.5,3.0,1.5,20.0" in text
        assert "3000,3.0,3.5,4.0,2.5,30.0" in text
        # Only one header
        assert text.count("timestamp,open,close,high,low,volume") == 1

    def test_existing_header_only_file_treated_as_new_write(self, tmp_path):
        """File with header only and no data rows → last_ts is None → the
        atomic partition_writer takes the "created" path which O_TRUNCs the
        file and writes a single header + rows. Improvement over the old
        wrapper which produced a duplicate header.
        """
        wrapper, original = self._make_wrapper()
        p = tmp_path / "x.csv"
        p.write_text("timestamp,open,close,high,low,volume\n")
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        wrapper(str(p), df=df)
        assert original._calls == []
        text = p.read_text()
        # Single header (the old quirk that produced two headers has been
        # fixed by partition_writer's O_TRUNC on the created path).
        assert text.count("timestamp,open,close,high,low,volume") == 1
        assert "1000," in text

    def test_overlap_backfill_leaves_file_unchanged(self, tmp_path):
        """When the wrapper delegates due to backfill, the file content must
        not have been mutated by the wrapper itself (the original_fn may or
        may not mutate; we only check the wrapper's own behavior).
        """
        wrapper, original = self._make_wrapper()
        p = tmp_path / "x.csv"
        original_content = (
            "timestamp,open,close,high,low,volume\n"
            "2000,2.0,2.5,3.0,1.5,20.0\n"
        )
        p.write_text(original_content)
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        wrapper(str(p), df=df)
        # Original was called; our fake original doesn't mutate the file
        assert len(original._calls) == 1
        # File unchanged because the fake original is a recorder
        assert p.read_text() == original_content

    def test_overlap_backfill_delegates(self, tmp_path):
        wrapper, original = self._make_wrapper()
        p = tmp_path / "x.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "2000,2.0,2.5,3.0,1.5,20.0\n"
        )
        # Incoming first ts < last on-disk ts → delegate via partition_writer
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        ret = wrapper(str(p), df=df)
        # Original was invoked (under the partition lock); wrapper returns None
        # consistently (mimicking what most write_partition impls return).
        assert len(original._calls) == 1
        assert ret is None

    def test_exception_in_wrapper_falls_back(self, tmp_path, monkeypatch, capsys):
        """Force _canonicalize_input_df to raise — wrapper should print and
        delegate to original.
        """
        wrapper, original = self._make_wrapper()
        def bad_canon(df):
            raise RuntimeError("boom")
        monkeypatch.setattr(ci_mod, "_canonicalize_input_df", bad_canon)
        df = _mk_df([(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        ret = wrapper(str(tmp_path / "x.csv"), df=df)
        assert len(original._calls) == 1
        assert ret == "original_return"
        captured = capsys.readouterr()
        assert "fast-append wrapper failed" in captured.out

    def test_wrapper_marked_installed(self):
        def original(*a, **k):
            return None
        wrapper = _fast_write_partition_wrapper_factory(original)
        assert getattr(wrapper, "_fast_append_installed", False) is True


# --------------------------------------------------------------------------- #
# F. _install_fast_append_writer
# --------------------------------------------------------------------------- #


class TestInstallFastAppendWriter:
    def _make_fake_cs(self):
        fake = types.ModuleType("candles_sync")
        def original(*a, **k):
            return "real_write"
        fake.write_partition = original
        return fake

    def test_install_wraps_write_partition(self, monkeypatch):
        fake = self._make_fake_cs()
        original_fn = fake.write_partition
        monkeypatch.setitem(sys.modules, "candles_sync", fake)
        _install_fast_append_writer()
        # write_partition should now be the wrapper, not the original
        assert fake.write_partition is not original_fn
        assert getattr(fake, "_fast_append_writer_installed", False) is True
        # Wrapper carries the install marker
        assert getattr(fake.write_partition, "_fast_append_installed", False) is True

    def test_install_is_idempotent(self, monkeypatch):
        fake = self._make_fake_cs()
        monkeypatch.setitem(sys.modules, "candles_sync", fake)
        _install_fast_append_writer()
        first_wrapper = fake.write_partition
        _install_fast_append_writer()
        # No re-wrap — same object
        assert fake.write_partition is first_wrapper

    def test_install_missing_write_partition_silent(self, monkeypatch):
        fake = types.ModuleType("candles_sync")
        # No write_partition attribute
        monkeypatch.setitem(sys.modules, "candles_sync", fake)
        # Should not raise
        _install_fast_append_writer()
        assert not hasattr(fake, "write_partition")
