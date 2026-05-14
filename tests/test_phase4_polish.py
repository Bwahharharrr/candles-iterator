"""Tests for Phase 4 of the file-robustness plan: polish.

Covers:
  P2-1: dropna-before-astype in _canonicalize_input_df.
  P2-2: os.listdir caching during rollover-gap window.
  P2-3: utf-8-sig encoding for historical reads.
  P2-4: opt-in candle-row validation in _truncate_last_line.
  P2-5: documented numeric-only invariant for tail parser (no behavioral test).
"""
from __future__ import annotations

import os
import time as _time_module
from datetime import datetime, timezone

import pandas as pd
import pytest

from candle_iterator import candle_iterator as ci_mod
from candle_iterator.candle_iterator import (
    AggregatorManager,
    CandleIterator,
    Config,
    _canonicalize_input_df,
    _truncate_last_line,
    _FALLBACK_CACHE_TTL_SECS,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _mk_iter(tmp_path, *, base_tf="1m", poll_interval=1):
    data_path = tmp_path / "EODHD" / "candles" / "TEST.US" / base_tf
    data_path.mkdir(parents=True)
    cfg = Config(
        exchange="EODHD", ticker="TEST.US", base_timeframe=base_tf,
        start_ts=None, end_ts=None, data_dir=str(tmp_path),
        verbose=False, poll_interval_seconds=poll_interval,
    )
    mgr = AggregatorManager(base_tf=base_tf, higher_tfs=[])
    return CandleIterator(cfg, mgr), data_path


def _frozen_dt_class(fixed: datetime):
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)
    return FrozenDateTime


def _write_partition(path, rows, with_header=True, encoding="utf-8"):
    lines = []
    if with_header:
        lines.append("timestamp,open,close,high,low,volume")
    for ts, o, c, h, l, v in rows:
        lines.append(f"{ts},{o},{c},{h},{l},{v}")
    path.write_text("\n".join(lines) + "\n", encoding=encoding)


# --------------------------------------------------------------------------- #
# P2-1: dropna-before-astype
# --------------------------------------------------------------------------- #


class TestCanonicalizeOrderingP21:
    def test_all_nan_timestamps_returns_none_gracefully(self, capsys):
        """No exception path triggered; clean empty-after-dropna return."""
        df = pd.DataFrame({
            "timestamp": [None, None],
            "open": [1.0, 2.0],
            "close": [1.5, 2.5],
            "high": [2.0, 3.0],
            "low": [0.5, 1.5],
            "volume": [10.0, 20.0],
        })
        assert _canonicalize_input_df(df) is None
        captured = capsys.readouterr()
        assert "_canonicalize_input_df failed" not in captured.out

    def test_mixed_nan_and_valid_keeps_valid(self):
        df = pd.DataFrame({
            "timestamp": [1000, None, 2000, None, 3000],
            "open": [1.0, 2.0, 3.0, 4.0, 5.0],
            "close": [1.5, 2.5, 3.5, 4.5, 5.5],
            "high": [2.0, 3.0, 4.0, 5.0, 6.0],
            "low": [0.5, 1.5, 2.5, 3.5, 4.5],
            "volume": [10.0, 20.0, 30.0, 40.0, 50.0],
        })
        result = _canonicalize_input_df(df)
        assert result is not None
        assert len(result) == 3
        assert list(result["timestamp"]) == [1000, 2000, 3000]
        # Types correctly enforced after the dropna
        assert result["timestamp"].dtype == "int64"

    def test_all_valid_unaffected(self):
        """Sanity: P2-1 reorder must not regress the clean-input path."""
        df = pd.DataFrame({
            "timestamp": [2000, 1000],
            "open": [2.0, 1.0],
            "close": [2.5, 1.5],
            "high": [3.0, 2.0],
            "low": [1.5, 0.5],
            "volume": [20.0, 10.0],
        })
        result = _canonicalize_input_df(df)
        assert result is not None
        assert list(result["timestamp"]) == [1000, 2000]  # sorted ascending


# --------------------------------------------------------------------------- #
# P2-2: os.listdir caching
# --------------------------------------------------------------------------- #


class TestFallbackCacheP22:
    def test_ttl_constant_value(self):
        # 30s is the agreed default
        assert _FALLBACK_CACHE_TTL_SECS == 30.0

    def test_fallback_cache_avoids_repeated_listdir(self, tmp_path, monkeypatch):
        """During the rollover-gap, _list_csv_files is called once and the
        result is cached for the TTL.
        """
        fixed = datetime(2026, 5, 15, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, dp = _mk_iter(tmp_path, base_tf="1m")
        # Create only the OLDER file (today's partition is absent — rollover gap)
        older = dp / "2026-05-14.csv"
        _write_partition(older, [(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])

        # Spy on _list_csv_files
        listdir_calls = [0]
        orig = it._list_csv_files
        def spy():
            listdir_calls[0] += 1
            return orig()
        it._list_csv_files = spy

        # First call: should listdir + cache
        r1 = it._ensure_tail_file_current()
        assert r1 == str(older)
        assert listdir_calls[0] == 1
        assert it._fallback_cache_path == str(older)

        # Second call (still in gap): should use cache, NOT re-listdir
        r2 = it._ensure_tail_file_current()
        assert r2 == str(older)
        assert listdir_calls[0] == 1

        # Third call: same
        r3 = it._ensure_tail_file_current()
        assert listdir_calls[0] == 1

    def test_cache_cleared_when_current_partition_appears(
        self, tmp_path, monkeypatch
    ):
        """When the expected current-partition file appears, the fallback
        cache is cleared so a future gap-window starts fresh.
        """
        fixed = datetime(2026, 5, 15, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, dp = _mk_iter(tmp_path, base_tf="1m")
        older = dp / "2026-05-14.csv"
        _write_partition(older, [(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        it._ensure_tail_file_current()
        assert it._fallback_cache_path == str(older)
        # Current partition appears
        current = dp / "2026-05-15.csv"
        _write_partition(current, [(86400000, 2.0, 2.5, 3.0, 1.5, 20.0)])
        result = it._ensure_tail_file_current()
        # Tail-follow switched to the current partition
        assert result == str(current)
        # Cache cleared
        assert it._fallback_cache_path is None
        assert it._fallback_cache_ts == 0.0

    def test_cache_expires_after_ttl(self, tmp_path, monkeypatch):
        fixed = datetime(2026, 5, 15, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, dp = _mk_iter(tmp_path, base_tf="1m")
        older = dp / "2026-05-14.csv"
        _write_partition(older, [(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])

        listdir_calls = [0]
        orig = it._list_csv_files
        def spy():
            listdir_calls[0] += 1
            return orig()
        it._list_csv_files = spy

        # First call: listdir + cache
        it._ensure_tail_file_current()
        assert listdir_calls[0] == 1
        # Force cache to be stale (pretend it was populated long ago)
        it._fallback_cache_ts -= _FALLBACK_CACHE_TTL_SECS + 1.0
        # Next call: TTL expired → listdir again
        it._ensure_tail_file_current()
        assert listdir_calls[0] == 2


# --------------------------------------------------------------------------- #
# P2-3: utf-8-sig encoding
# --------------------------------------------------------------------------- #


class TestUtf8SigEncodingP23:
    def test_historical_read_handles_bom(self, tmp_path, monkeypatch):
        """A CSV file with UTF-8 BOM at the start is read correctly by
        _load_candles_from_disk (the BOM is stripped via utf-8-sig).
        File must exist BEFORE iterator construction so csv_file_paths
        captures it.
        """
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        # Pre-create the data dir + BOM-prefixed CSV
        data_path = tmp_path / "EODHD" / "candles" / "TEST.US" / "1m"
        data_path.mkdir(parents=True)
        p = data_path / "2026-05-14.csv"
        _write_partition(
            p, [(60_000, 1.0, 1.5, 2.0, 0.5, 10.0)],
            encoding="utf-8-sig",
        )
        assert p.read_bytes().startswith(b"\xef\xbb\xbf")
        # NOW construct the iterator so csv_file_paths sees the file
        cfg = Config(
            exchange="EODHD", ticker="TEST.US", base_timeframe="1m",
            start_ts=None, end_ts=None, data_dir=str(tmp_path),
            verbose=False, poll_interval_seconds=None,
        )
        mgr = AggregatorManager(base_tf="1m", higher_tfs=[])
        it = CandleIterator(cfg, mgr)
        # Drive _load_candles_from_disk
        rows = list(it._load_candles_from_disk())
        # Header recognized (BOM stripped), data row parsed
        assert len(rows) == 1
        assert rows[0][0] == 60_000

    def test_historical_read_handles_no_bom(self, tmp_path, monkeypatch):
        """Sanity: utf-8-sig still reads plain utf-8 (no BOM) files."""
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        data_path = tmp_path / "EODHD" / "candles" / "TEST.US" / "1m"
        data_path.mkdir(parents=True)
        p = data_path / "2026-05-14.csv"
        _write_partition(
            p, [(60_000, 1.0, 1.5, 2.0, 0.5, 10.0)],
            encoding="utf-8",
        )
        assert not p.read_bytes().startswith(b"\xef\xbb\xbf")
        cfg = Config(
            exchange="EODHD", ticker="TEST.US", base_timeframe="1m",
            start_ts=None, end_ts=None, data_dir=str(tmp_path),
            verbose=False, poll_interval_seconds=None,
        )
        mgr = AggregatorManager(base_tf="1m", higher_tfs=[])
        it = CandleIterator(cfg, mgr)
        rows = list(it._load_candles_from_disk())
        assert len(rows) == 1


# --------------------------------------------------------------------------- #
# P2-4: opt-in candle-row validation in _truncate_last_line
# --------------------------------------------------------------------------- #


class TestTruncateLastLineValidationP24:
    def test_default_is_generic_truncate(self, tmp_path):
        """Backward compat: without validate_candle_row, the function works
        as a generic line-truncator (legacy P0-8 tests rely on this).
        """
        p = tmp_path / "x.txt"
        p.write_text("alpha\nbeta\ngamma\n")
        assert _truncate_last_line(str(p)) is True
        assert p.read_text() == "alpha\nbeta\n"

    def test_validation_passes_on_valid_candle_row(self, tmp_path):
        p = tmp_path / "x.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5,2.0,0.5,10.0\n"
            "2000,2.0,2.5,3.0,1.5,20.0\n"
        )
        ok = _truncate_last_line(str(p), validate_candle_row=True)
        assert ok is True
        text = p.read_text()
        assert "2000," not in text
        assert "1000," in text

    def test_validation_refuses_non_candle_last_line(self, tmp_path, capsys):
        p = tmp_path / "x.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5,2.0,0.5,10.0\n"
            "garbage,not,a,candle,row,here\n"
        )
        ok = _truncate_last_line(str(p), validate_candle_row=True)
        assert ok is False
        # File unchanged
        assert "garbage,not,a,candle,row,here" in p.read_text()
        # Warning logged
        captured = capsys.readouterr()
        assert "refusing to truncate" in captured.out

    def test_validation_refuses_fewer_than_6_columns(self, tmp_path):
        p = tmp_path / "x.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,1.0,1.5\n"  # 3 columns; would normally pass strict regex
        )
        ok = _truncate_last_line(str(p), validate_candle_row=True)
        assert ok is False

    def test_validation_refuses_non_numeric_field(self, tmp_path):
        p = tmp_path / "x.csv"
        p.write_text(
            "timestamp,open,close,high,low,volume\n"
            "1000,abc,1.5,2.0,0.5,10.0\n"
        )
        ok = _truncate_last_line(str(p), validate_candle_row=True)
        assert ok is False


# --------------------------------------------------------------------------- #
# P2-5: tail parser invariant comment (no behavioral test)
# --------------------------------------------------------------------------- #
# Per Codex's debate: docstring/comment presence does not need a behavioral
# test (source-grep tests are brittle).
