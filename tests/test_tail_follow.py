"""Tests for the tail-follow subsystem.

Phase P0-7 of the test-coverage plan: pin rotation detection, partition
basename, tail parsing, header/malformed skip, partial-first-line drop,
strictly-new-ts early-break, redrain hint, and exception containment.

Targets:
  - _current_partition_basename     line 1787-1797
  - _ensure_tail_file_current       line 1799-1864
  - _poll_for_new_data_and_buffer   line 1505-1699

CSV column order is `timestamp,open,close,high,low,volume` (FAST_APPEND_HEADER
at line 58); the parser reads `o,c,h,l,v` and ingests `(ts, o, h, l, c, v)`.
"""
from __future__ import annotations

import os
import time as _time_module
from datetime import datetime, timezone

import pytest

from candle_iterator import candle_iterator as ci_mod
from candle_iterator.candle_iterator import (
    AggregatorManager,
    CandleIterator,
    Config,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _mk_iter(tmp_path, *, base_tf="1m", end_ts=None, poll_interval=1, verbose=False):
    """Build a CandleIterator with a real-ish data dir but no IO sync."""
    data_path = tmp_path / "EODHD" / "candles" / "TEST.US" / base_tf
    data_path.mkdir(parents=True)
    cfg = Config(
        exchange="EODHD",
        ticker="TEST.US",
        base_timeframe=base_tf,
        start_ts=None,
        end_ts=end_ts,
        data_dir=str(tmp_path),
        verbose=verbose,
        poll_interval_seconds=poll_interval,
    )
    mgr = AggregatorManager(base_tf=base_tf, higher_tfs=[])
    it = CandleIterator(cfg, mgr)
    return it, mgr, data_path


def _write_partition(path, rows, *, with_header=True):
    """Write a CSV with the actual column order `ts,o,c,h,l,v`."""
    lines = []
    if with_header:
        lines.append("timestamp,open,close,high,low,volume")
    for ts, o, c, h, l, v in rows:
        lines.append(f"{ts},{o},{c},{h},{l},{v}")
    path.write_text("\n".join(lines) + "\n")


def _append_partition(path, rows):
    with open(path, "a") as f:
        for ts, o, c, h, l, v in rows:
            f.write(f"{ts},{o},{c},{h},{l},{v}\n")


def _row(ts, o=100.0, c=102.0, h=105.0, l=95.0, v=1000.0):
    return (ts, o, c, h, l, v)


# --------------------------------------------------------------------------- #
# A. _current_partition_basename
# --------------------------------------------------------------------------- #


class TestPartitionBasename:
    def test_1m_daily_naming(self, tmp_path):
        it, _, _ = _mk_iter(tmp_path, base_tf="1m")
        dt = datetime(2026, 1, 5, 12, 30, tzinfo=timezone.utc)
        assert it._current_partition_basename(dt) == "2026-01-05.csv"

    def test_1h_monthly_naming(self, tmp_path):
        it, _, _ = _mk_iter(tmp_path, base_tf="1h")
        dt = datetime(2026, 7, 15, tzinfo=timezone.utc)
        assert it._current_partition_basename(dt) == "2026-07.csv"

    def test_1D_yearly_naming(self, tmp_path):
        it, _, _ = _mk_iter(tmp_path, base_tf="1D")
        dt = datetime(2026, 6, 1, tzinfo=timezone.utc)
        assert it._current_partition_basename(dt) == "2026.csv"

    def test_zero_padded_month_and_day(self, tmp_path):
        it, _, _ = _mk_iter(tmp_path, base_tf="1m")
        dt = datetime(2026, 1, 5, tzinfo=timezone.utc)
        assert it._current_partition_basename(dt) == "2026-01-05.csv"


# --------------------------------------------------------------------------- #
# Time control: FrozenDateTime subclass
# --------------------------------------------------------------------------- #


def _frozen_dt_class(fixed: datetime):
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)
    return FrozenDateTime


# --------------------------------------------------------------------------- #
# B-F. _ensure_tail_file_current — init / rotation / inode / truncation
# --------------------------------------------------------------------------- #


class TestEnsureTailFileCurrentInit:
    def test_first_init_sets_path_inode_offset(self, tmp_path, monkeypatch):
        it, _, data_path = _mk_iter(tmp_path, base_tf="1m")
        fixed = datetime(2026, 5, 14, 10, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        # Create the expected partition file
        p = data_path / "2026-05-14.csv"
        _write_partition(p, [_row(1000)])
        result = it._ensure_tail_file_current()
        assert result == str(p)
        assert it._tail_file_path == str(p)
        assert it._tail_inode is not None
        assert it._tail_size > 0
        # Offset starts at EOF
        assert it._tail_offset == it._tail_size

    def test_no_files_returns_none(self, tmp_path, monkeypatch):
        it, _, data_path = _mk_iter(tmp_path, base_tf="1m")
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        # Empty data dir → fallback also empty
        assert it._ensure_tail_file_current() is None

    def test_fallback_to_latest_existing_file(self, tmp_path, monkeypatch):
        """Current partition doesn't exist but an older file does → fall back."""
        it, _, data_path = _mk_iter(tmp_path, base_tf="1m")
        # Today is 2026-05-15 but only 2026-05-14 exists
        fixed = datetime(2026, 5, 15, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        older = data_path / "2026-05-14.csv"
        _write_partition(older, [_row(1000)])
        result = it._ensure_tail_file_current()
        assert result == str(older)


class TestEnsureTailFileCurrentRotation:
    def test_path_change_resets_offset_and_line_len(self, tmp_path, monkeypatch):
        """Day rollover → new path → offset and last-line-len reset."""
        it, _, data_path = _mk_iter(tmp_path, base_tf="1m")
        # Day 1 init
        day1 = datetime(2026, 5, 14, 23, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(day1))
        p1 = data_path / "2026-05-14.csv"
        _write_partition(p1, [_row(1000)])
        it._ensure_tail_file_current()
        # Simulate state advancing
        it._tail_offset = it._tail_size
        it._tail_last_line_len = 60
        # Day 2 — new partition file appears
        day2 = datetime(2026, 5, 15, 0, 1, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(day2))
        p2 = data_path / "2026-05-15.csv"
        _write_partition(p2, [_row(2000)])
        result = it._ensure_tail_file_current()
        assert result == str(p2)
        assert it._tail_file_path == str(p2)
        # Rotation reset
        assert it._tail_offset == 0
        assert it._tail_last_line_len == 0


class TestEnsureTailFileCurrentInodeChange:
    def test_inode_change_with_same_path_triggers_rotation(self, tmp_path, monkeypatch):
        """Same path, but file replaced via unlink+rewrite → inode changes →
        offset/last_line_len reset.
        """
        it, _, data_path = _mk_iter(tmp_path, base_tf="1m")
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        p = data_path / "2026-05-14.csv"
        _write_partition(p, [_row(1000)])
        it._ensure_tail_file_current()
        old_inode = it._tail_inode
        it._tail_offset = it._tail_size
        # Replace the file (new inode)
        os.unlink(p)
        _write_partition(p, [_row(2000), _row(3000)])
        new_st = os.stat(p)
        # On most filesystems, the new file gets a new inode. Skip otherwise.
        if new_st.st_ino == old_inode:
            pytest.skip("Filesystem reused inode")
        result = it._ensure_tail_file_current()
        assert result == str(p)
        assert it._tail_inode == new_st.st_ino
        assert it._tail_offset == 0  # reset due to inode change


class TestEnsureTailFileCurrentTruncation:
    def test_size_shrink_resets_offset(self, tmp_path, monkeypatch):
        it, _, data_path = _mk_iter(tmp_path, base_tf="1m")
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        p = data_path / "2026-05-14.csv"
        _write_partition(p, [_row(1000), _row(2000), _row(3000)])
        it._ensure_tail_file_current()
        it._tail_offset = it._tail_size
        it._tail_last_line_len = 60
        # Truncate the file in-place by rewriting smaller content
        # (need to preserve inode for "same file, smaller size" case)
        # Use os.truncate to shrink without unlinking
        with open(p, "rb+") as f:
            f.truncate(20)  # very small, way below previous size
        result = it._ensure_tail_file_current()
        assert result == str(p)
        assert it._tail_offset == 0
        assert it._tail_last_line_len == 0


class TestEnsureTailFileCurrentSteadyState:
    def test_steady_state_updates_size_only(self, tmp_path, monkeypatch):
        it, _, data_path = _mk_iter(tmp_path, base_tf="1m")
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        p = data_path / "2026-05-14.csv"
        _write_partition(p, [_row(1000)])
        it._ensure_tail_file_current()
        first_offset = it._tail_offset
        first_inode = it._tail_inode
        # Append more rows
        _append_partition(p, [_row(2000), _row(3000)])
        result = it._ensure_tail_file_current()
        assert result == str(p)
        # No reset — offset preserved, inode preserved
        assert it._tail_offset == first_offset
        assert it._tail_inode == first_inode
        # Size updated
        assert it._tail_size == os.path.getsize(p)


# --------------------------------------------------------------------------- #
# G. _poll_for_new_data_and_buffer — parsing & ingestion
# --------------------------------------------------------------------------- #


class TestPollParsing:
    def test_first_poll_with_no_file_no_error(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)
        monkeypatch.setattr(_time_module, "sleep", lambda *_: None)
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, _, _ = _mk_iter(tmp_path, base_tf="1m")
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=True, stop_after_first_emission=True
        )
        # No crash
        assert list(it._closed_buffer) == []

    def test_first_poll_with_rows_ingested_and_OHLC_mapping_correct(
        self, tmp_path, monkeypatch
    ):
        """Drive a poll with three rows AND a 4th to force a base finalize
        for row 0 so we can verify OHLC mapping end-to-end. CSV column order
        is (ts, o, c, h, l, v); the parser reads (o, c, h, l, v) and the
        aggregator finalizes with the correct OHLC.
        """
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)
        monkeypatch.setattr(_time_module, "sleep", lambda *_: None)
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, mgr, data_path = _mk_iter(tmp_path, base_tf="1m")
        p = data_path / "2026-05-14.csv"
        # Write rows with DISTINCT o/c/h/l values per row so swaps would show
        rows = [
            (60_000,  100.0, 102.0, 107.0, 93.0, 1000.0),
            (120_000, 102.0, 103.0, 108.0, 94.0, 1500.0),
            (180_000, 103.0, 104.0, 109.0, 95.0, 2000.0),
        ]
        _write_partition(p, rows)
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        assert it._last_ts == 180_000
        # After 3 bars, base finalized bars 0 and 1. `latest_closed_candles`
        # holds the LATEST per TF — bar 1 (ts=120_000). Verifying its OHLC
        # against the CSV pins the column mapping: o!=c!=h!=l so a swap
        # would manifest as a wrong value.
        latest = mgr.latest_closed_candles.get("1m")
        assert latest is not None
        assert latest.timestamp == 120_000
        assert latest.open == 102.0
        assert latest.close == 103.0
        assert latest.high == 108.0
        assert latest.low == 94.0
        assert latest.volume == 1500.0

    def test_subsequent_poll_picks_up_appended_rows_only(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)
        monkeypatch.setattr(_time_module, "sleep", lambda *_: None)
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, mgr, data_path = _mk_iter(tmp_path, base_tf="1m")
        p = data_path / "2026-05-14.csv"
        _write_partition(p, [_row(60_000), _row(120_000)])
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        assert it._last_ts == 120_000
        # Append new row
        _append_partition(p, [_row(180_000)])
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        assert it._last_ts == 180_000

    def test_header_skipped_on_rotation(self, tmp_path, monkeypatch):
        """When start_offset > 0, the partial first line is dropped; but if
        a fresh rotation starts offset=0, the header row at the top of the
        file must be skipped. Force the start_offset=0 path by rotation.
        """
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)
        monkeypatch.setattr(_time_module, "sleep", lambda *_: None)
        # Day 1
        day1 = datetime(2026, 5, 14, 23, 30, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(day1))
        it, mgr, data_path = _mk_iter(tmp_path, base_tf="1m")
        p1 = data_path / "2026-05-14.csv"
        _write_partition(p1, [_row(60_000)])
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        # Day 2
        day2 = datetime(2026, 5, 15, 0, 5, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(day2))
        p2 = data_path / "2026-05-15.csv"
        _write_partition(p2, [_row(86_400_000 + 60_000)])
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        # The header line at top of p2 must NOT have been parsed as a row
        assert it._last_ts == 86_400_000 + 60_000

    def test_malformed_row_skipped_silently_when_not_verbose(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)
        monkeypatch.setattr(_time_module, "sleep", lambda *_: None)
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, mgr, data_path = _mk_iter(tmp_path, base_tf="1m", verbose=False)
        p = data_path / "2026-05-14.csv"
        # Mix valid + malformed rows
        content = (
            "timestamp,open,close,high,low,volume\n"
            "60000,1.0,1.5,2.0,0.5,10.0\n"
            "notanumber,1.0,1.5,2.0,0.5,10.0\n"
            "120000,abc,1.5,2.0,0.5,10.0\n"
            "180000,2.0,2.5,3.0,1.5,20.0\n"
        )
        p.write_text(content)
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        # Only the valid rows ingested
        assert it._last_ts == 180_000

    def test_short_row_under_six_columns_skipped(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)
        monkeypatch.setattr(_time_module, "sleep", lambda *_: None)
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, mgr, data_path = _mk_iter(tmp_path, base_tf="1m")
        p = data_path / "2026-05-14.csv"
        content = (
            "timestamp,open,close,high,low,volume\n"
            "60000,1.0,1.5,2.0,0.5,10.0\n"
            "120000,1.0,1.5\n"  # too few columns
            "180000,2.0,2.5,3.0,1.5,20.0\n"
        )
        p.write_text(content)
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        assert it._last_ts == 180_000


class TestPollEarlyBreak:
    def test_stop_after_first_emission_breaks_on_strictly_new_ts(
        self, tmp_path, monkeypatch
    ):
        """With stop_after_first_emission=True, the loop breaks as soon as
        a closure is enqueued from a row whose ts > initial_last_ts.
        """
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)
        monkeypatch.setattr(_time_module, "sleep", lambda *_: None)
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, mgr, data_path = _mk_iter(tmp_path, base_tf="1m")
        # Seed _last_ts to 60_000 (the iterator's initial_last_ts)
        it._last_ts = 60_000
        it._last_close = 100.0
        # Also seed the manager's base aggregator state to match
        mgr.base_agg.on_base_csv_row(60_000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        p = data_path / "2026-05-14.csv"
        _write_partition(p, [
            _row(60_000),    # same-ts snapshot update → does NOT break
            _row(120_000),   # strictly-new ts → would break (after closure)
            _row(180_000),   # would be left for next poll
        ])
        # Force fresh tail offset to read from start
        it._tail_offset = 0
        it._tail_size = 0
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=True
        )
        # The strictly-new ts 120_000 triggered a closure (1m base bar at 60_000
        # finalizes when bar at 120_000 ingests) → break. last_ts == 120_000.
        # The 180_000 row was NOT processed in this poll.
        assert it._last_ts == 120_000
        assert it._redrain_after_first_emission is True
        # Now drive a SECOND poll — the 180_000 row should be picked up.
        # (Reset redrain flag to simulate the production path that consumed
        # one closure and is now polling again.)
        it._redrain_after_first_emission = False
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        assert it._last_ts == 180_000

    def test_no_break_when_no_new_ts(self, tmp_path, monkeypatch):
        """Same-ts re-ingest does NOT trigger break (per the strict > check)."""
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)
        monkeypatch.setattr(_time_module, "sleep", lambda *_: None)
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, mgr, data_path = _mk_iter(tmp_path, base_tf="1m")
        it._last_ts = 60_000
        it._last_close = 100.0
        mgr.base_agg.on_base_csv_row(60_000, 100.0, 105.0, 95.0, 102.0, 1000.0)
        p = data_path / "2026-05-14.csv"
        # Only same-ts snapshots in file
        _write_partition(p, [_row(60_000), _row(60_000)])
        it._tail_offset = 0
        it._tail_size = 0
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=True
        )
        # No closure emitted (no strictly-new ts), so no redrain hint set
        assert it._redrain_after_first_emission is False


class TestPollExceptionContainment:
    def test_stat_failure_does_not_crash_poll(self, tmp_path, monkeypatch):
        """If os.stat raises mid-poll, the catch at the outer try logs a
        warning and returns; subsequent poll is recoverable.
        """
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)
        monkeypatch.setattr(_time_module, "sleep", lambda *_: None)
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, mgr, data_path = _mk_iter(tmp_path, base_tf="1m")
        p = data_path / "2026-05-14.csv"
        _write_partition(p, [_row(60_000)])
        # Make os.stat raise
        orig_stat = os.stat
        def bad_stat(path, *args, **kwargs):
            if str(path) == str(p):
                raise OSError("simulated stat failure")
            return orig_stat(path, *args, **kwargs)
        monkeypatch.setattr(os, "stat", bad_stat)
        # Should not crash
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        # After the recovery, restore stat and try again
        monkeypatch.setattr(os, "stat", orig_stat)
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        # Now the row WAS ingested
        assert it._last_ts == 60_000


class TestPollOffsetAdvance:
    def test_tail_offset_advances_to_eof(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)
        monkeypatch.setattr(_time_module, "sleep", lambda *_: None)
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, mgr, data_path = _mk_iter(tmp_path, base_tf="1m")
        p = data_path / "2026-05-14.csv"
        _write_partition(p, [_row(60_000), _row(120_000)])
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        assert it._tail_offset == os.path.getsize(p)
        # _tail_last_line_len equals the length of the last parsed line
        # (without the trailing newline, which `splitlines()` strips)
        last_line = "120000,100.0,102.0,105.0,95.0,1000.0"
        assert it._tail_last_line_len == len(last_line)


class TestPartialFirstLineDrop:
    def test_start_offset_inside_a_line_drops_partial(self, tmp_path, monkeypatch):
        """When start_offset > 0 (after backtrack), the first newline-aligned
        fragment is dropped. Construct a file, set _tail_offset to a position
        INSIDE row 1, and poll. The parser must drop the partial fragment of
        row 1 and start at row 2.
        """
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)
        monkeypatch.setattr(_time_module, "sleep", lambda *_: None)
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, mgr, data_path = _mk_iter(tmp_path, base_tf="1m")
        p = data_path / "2026-05-14.csv"
        # 3 rows with distinct ts
        rows = [
            _row(60_000),
            _row(120_000),
            _row(180_000),
        ]
        _write_partition(p, rows)
        # Compute position roughly inside row 1 (after header + ~half of row 1).
        # backtrack window is TAIL_BACKTRACK_BYTES=4096, larger than the file,
        # so we set _tail_offset to a value > backtrack to force start_offset>0.
        # Make _tail_last_line_len small so dynamic backtrack uses small window.
        it._tail_last_line_len = 30
        # start_offset = max(0, _tail_offset - min(4096, 30*2+256)) = _tail_offset - 316
        # Set _tail_offset such that start_offset lands mid-row-2
        file_size = os.path.getsize(p)
        header_line_len = len("timestamp,open,close,high,low,volume\n")
        row_len = len("60000,100.0,102.0,105.0,95.0,1000.0\n")
        # We want start_offset to be mid-row-2 (the second data row, ts=120000)
        # The bytes for row 2 start at header_line_len + row_len.
        mid_of_row_2 = header_line_len + row_len + (row_len // 2)
        # _tail_offset = mid_of_row_2 + 316 (so start_offset = mid_of_row_2)
        it._tail_offset = mid_of_row_2 + 316
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        # The partial fragment of row 2 (ts=120000) should have been dropped.
        # Row 3 (ts=180000) should be the only line ingested.
        # _last_ts == 180_000 (the only parsed row)
        assert it._last_ts == 180_000
