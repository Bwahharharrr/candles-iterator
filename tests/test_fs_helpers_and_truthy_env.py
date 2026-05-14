"""Tests for small filesystem helpers and _truthy_env.

Combines phases P2-2 (small fs helpers) and P2-3 (_truthy_env boundaries).

Targets:
  - _ensure_parent_dir         line 78-81
  - _file_size                 line 84-88
  - _file_ends_with_newline    line 91-100
  - _ensure_trailing_newline   line 103-110
  - _extract_path_and_df       line 274-306
  - _truthy_env                line 71-75
"""
from __future__ import annotations

import os

import pandas as pd
import pytest

from candle_iterator.candle_iterator import (
    _ensure_parent_dir,
    _ensure_trailing_newline,
    _extract_path_and_df,
    _file_ends_with_newline,
    _file_size,
    _truthy_env,
)


# --------------------------------------------------------------------------- #
# _truthy_env
# --------------------------------------------------------------------------- #


class TestTruthyEnv:
    @pytest.mark.parametrize("val", ["1", "true", "yes", "on", "y", "t"])
    def test_lowercase_canonical_values_true(self, val):
        assert _truthy_env(val) is True

    @pytest.mark.parametrize("val", ["TRUE", "Yes", "ON", "Y", "T"])
    def test_mixed_case_true(self, val):
        assert _truthy_env(val) is True

    @pytest.mark.parametrize("val", ["  yes  ", "\t1\n", "  TRUE "])
    def test_whitespace_stripped(self, val):
        assert _truthy_env(val) is True

    @pytest.mark.parametrize("val", ["0", "false", "no", "off", "n", "f", "",
                                     "2", "yeah", "truly", "garbage"])
    def test_non_whitelisted_false(self, val):
        assert _truthy_env(val) is False

    def test_none_returns_false(self):
        assert _truthy_env(None) is False


# --------------------------------------------------------------------------- #
# _ensure_parent_dir
# --------------------------------------------------------------------------- #


class TestEnsureParentDir:
    def test_creates_nested_parent(self, tmp_path):
        target = tmp_path / "a" / "b" / "c" / "file.csv"
        _ensure_parent_dir(str(target))
        assert (tmp_path / "a" / "b" / "c").is_dir()

    def test_existing_parent_no_error(self, tmp_path):
        (tmp_path / "existing").mkdir()
        _ensure_parent_dir(str(tmp_path / "existing" / "file.csv"))

    def test_no_parent_segment_no_error(self, tmp_path):
        # Path with no directory part (relative bare filename)
        # The function should no-op silently.
        _ensure_parent_dir("plain.csv")


# --------------------------------------------------------------------------- #
# _file_size
# --------------------------------------------------------------------------- #


class TestFileSize:
    def test_existing_file_returns_size(self, tmp_path):
        p = tmp_path / "f.csv"
        p.write_text("hello world\n")
        assert _file_size(str(p)) == len("hello world\n")

    def test_nonexistent_returns_zero(self, tmp_path):
        assert _file_size(str(tmp_path / "nope.csv")) == 0

    def test_empty_file_returns_zero(self, tmp_path):
        p = tmp_path / "empty.csv"
        p.write_text("")
        assert _file_size(str(p)) == 0


# --------------------------------------------------------------------------- #
# _file_ends_with_newline
# --------------------------------------------------------------------------- #


class TestFileEndsWithNewline:
    def test_file_with_trailing_newline(self, tmp_path):
        p = tmp_path / "f.csv"
        p.write_text("abc\n")
        assert _file_ends_with_newline(str(p)) is True

    def test_file_without_trailing_newline(self, tmp_path):
        p = tmp_path / "f.csv"
        p.write_text("abc")
        assert _file_ends_with_newline(str(p)) is False

    def test_empty_file_returns_true(self, tmp_path):
        p = tmp_path / "empty.csv"
        p.write_text("")
        # Empty file has size 0; the function returns True (no need to append)
        assert _file_ends_with_newline(str(p)) is True

    def test_nonexistent_file_returns_true(self, tmp_path):
        # Defensive: nonexistent treated as "no need to append" → True
        assert _file_ends_with_newline(str(tmp_path / "nope.csv")) is True


# --------------------------------------------------------------------------- #
# _ensure_trailing_newline
# --------------------------------------------------------------------------- #


class TestEnsureTrailingNewline:
    def test_appends_newline_if_missing(self, tmp_path):
        p = tmp_path / "f.csv"
        p.write_text("abc")
        _ensure_trailing_newline(str(p))
        assert p.read_text() == "abc\n"

    def test_noop_if_already_has_newline(self, tmp_path):
        p = tmp_path / "f.csv"
        p.write_text("abc\n")
        _ensure_trailing_newline(str(p))
        assert p.read_text() == "abc\n"

    def test_noop_on_empty_file(self, tmp_path):
        p = tmp_path / "empty.csv"
        p.write_text("")
        _ensure_trailing_newline(str(p))
        assert p.read_text() == ""

    def test_noop_on_nonexistent(self, tmp_path):
        # Should not raise
        _ensure_trailing_newline(str(tmp_path / "nope.csv"))


# --------------------------------------------------------------------------- #
# _extract_path_and_df
# --------------------------------------------------------------------------- #


class TestExtractPathAndDf:
    def test_kwargs_path_and_df(self):
        df = pd.DataFrame({"a": [1]})
        path, found_df = _extract_path_and_df(
            args=(), kwargs={"path": "/tmp/x.csv", "df": df}
        )
        assert path == "/tmp/x.csv"
        assert found_df is df

    def test_kwargs_alternative_path_keys(self):
        path_keys = ["csv_path", "file_path", "filepath", "out_path",
                     "dst_path", "dst"]
        for key in path_keys:
            path, _ = _extract_path_and_df(args=(), kwargs={key: "/p.csv"})
            assert path == "/p.csv", f"failed for key={key}"

    def test_positional_path_string(self):
        path, _ = _extract_path_and_df(args=("/p.csv",), kwargs={})
        assert path == "/p.csv"

    def test_positional_df(self):
        df = pd.DataFrame({"a": [1]})
        path, found_df = _extract_path_and_df(args=("/p.csv", df), kwargs={})
        assert path == "/p.csv"
        assert found_df is df

    def test_kwargs_alternative_df_keys(self):
        df = pd.DataFrame({"a": [1]})
        for key in ["df", "frame", "data", "rows", "chunk"]:
            _, found_df = _extract_path_and_df(args=(), kwargs={key: df})
            assert found_df is df, f"failed for key={key}"

    def test_both_missing_returns_none_none(self):
        path, df = _extract_path_and_df(args=(), kwargs={})
        assert path is None
        assert df is None

    def test_non_csv_positional_string_ignored(self):
        """Positional string args without .csv suffix are not treated as path."""
        path, _ = _extract_path_and_df(args=("not_a_csv",), kwargs={})
        assert path is None
