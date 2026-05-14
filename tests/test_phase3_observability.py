"""Tests for Phase 3 of the file-robustness plan: observability.

Covers:
  P1-5: rotation tuple stores ctime_ns for diagnostics (NOT a rotation trigger).
  P1-7: narrow OSError handling in helper functions with logging on suspicious
        errno values.
  P1-8: tail health counter — mid-file header OR N consecutive malformed rows
        forces a full resync (offset → 0, log reason, reset counter).
"""
from __future__ import annotations

import errno
import os
import time as _time_module
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from candle_iterator import candle_iterator as ci_mod
from candle_iterator.candle_iterator import (
    AggregatorManager,
    CandleIterator,
    Config,
    _file_ends_with_newline,
    _file_size,
    _ensure_trailing_newline,
    _TAIL_MALFORMED_RESYNC_THRESHOLD,
)


def _mk_iter(tmp_path, *, base_tf="1m", end_ts=None, poll_interval=1, verbose=False):
    data_path = tmp_path / "EODHD" / "candles" / "TEST.US" / base_tf
    data_path.mkdir(parents=True)
    cfg = Config(
        exchange="EODHD", ticker="TEST.US", base_timeframe=base_tf,
        start_ts=None, end_ts=end_ts, data_dir=str(tmp_path),
        verbose=verbose, poll_interval_seconds=poll_interval,
    )
    mgr = AggregatorManager(base_tf=base_tf, higher_tfs=[])
    return CandleIterator(cfg, mgr), data_path


def _frozen_dt_class(fixed: datetime):
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)
    return FrozenDateTime


def _write_partition(path, rows, with_header=True):
    lines = []
    if with_header:
        lines.append("timestamp,open,close,high,low,volume")
    for ts, o, c, h, l, v in rows:
        lines.append(f"{ts},{o},{c},{h},{l},{v}")
    path.write_text("\n".join(lines) + "\n")


# --------------------------------------------------------------------------- #
# P1-5: rotation tuple stores ctime_ns
# --------------------------------------------------------------------------- #


class TestRotationCtimeDiagnostic:
    def test_init_records_ctime_ns(self, tmp_path, monkeypatch):
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, dp = _mk_iter(tmp_path, base_tf="1m")
        p = dp / "2026-05-14.csv"
        _write_partition(p, [(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        it._ensure_tail_file_current()
        assert it._tail_ctime_ns is not None
        # Should match the file's actual ctime_ns
        st = os.stat(p)
        expected_ctime_ns = getattr(st, "st_ctime_ns", int(st.st_ctime * 1_000_000_000))
        assert it._tail_ctime_ns == expected_ctime_ns

    def test_normal_append_updates_ctime_but_no_rotation(
        self, tmp_path, monkeypatch, capsys
    ):
        """Appending to a file changes its ctime_ns. The tail-follow MUST NOT
        treat this as rotation (offset preserved). The diagnostic field
        updates as the file grows.
        """
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, dp = _mk_iter(tmp_path, base_tf="1m")
        p = dp / "2026-05-14.csv"
        _write_partition(p, [(1000, 1.0, 1.5, 2.0, 0.5, 10.0)])
        it._ensure_tail_file_current()
        first_offset = it._tail_offset
        first_ctime = it._tail_ctime_ns
        # Append (which bumps ctime_ns)
        time_module = __import__("time")
        time_module.sleep(0.01)  # Ensure ctime difference
        with open(p, "a") as f:
            f.write("2000,2.0,2.5,3.0,1.5,20.0\n")
        # Call ensure-tail again
        it._ensure_tail_file_current()
        # Offset NOT reset (ctime change is not a rotation trigger)
        assert it._tail_offset == first_offset
        # ctime_ns updated for diagnostics
        assert it._tail_ctime_ns >= first_ctime
        # No rotation log
        captured = capsys.readouterr()
        assert "rotation detected" not in captured.out


# --------------------------------------------------------------------------- #
# P1-7: narrow OSError handling
# --------------------------------------------------------------------------- #


class TestNarrowExceptionHandling:
    def test_file_size_missing_file_silent_zero(self, tmp_path, capsys):
        assert _file_size(str(tmp_path / "nope.csv")) == 0
        captured = capsys.readouterr()
        # No warning for normal ENOENT
        assert "_file_size" not in captured.out

    def test_file_size_permission_denied_logs(self, tmp_path, capsys, monkeypatch):
        """If os.path.getsize raises EACCES, _file_size logs a warning and
        returns 0 (behavior preserved).
        """
        def deny(path):
            raise PermissionError(errno.EACCES, "denied")
        monkeypatch.setattr(ci_mod.os.path, "getsize", deny)
        result = _file_size("/whatever")
        assert result == 0
        captured = capsys.readouterr()
        assert "_file_size" in captured.out
        assert "errno=" in captured.out

    def test_file_size_does_not_swallow_keyboard_interrupt(self, monkeypatch):
        """Narrowing to OSError means KeyboardInterrupt is NOT caught — it
        propagates as expected.
        """
        def interrupt(path):
            raise KeyboardInterrupt
        monkeypatch.setattr(ci_mod.os.path, "getsize", interrupt)
        with pytest.raises(KeyboardInterrupt):
            _file_size("/whatever")

    def test_file_ends_with_newline_permission_denied_logs(
        self, tmp_path, capsys, monkeypatch
    ):
        def deny(path):
            raise PermissionError(errno.EACCES, "denied")
        monkeypatch.setattr(ci_mod.os.path, "getsize", deny)
        assert _file_ends_with_newline("/whatever") is True
        captured = capsys.readouterr()
        assert "_file_ends_with_newline" in captured.out

    def test_ensure_trailing_newline_logs_on_eacces(self, tmp_path, capsys, monkeypatch):
        """Patching the module's `open` to raise EACCES exercises the
        OSError branch with errno=EACCES → logged warning, no raise.
        """
        p = tmp_path / "x.csv"
        p.write_text("data")  # no trailing newline
        def deny(*a, **k):
            raise PermissionError(errno.EACCES, "denied")
        # `open` is a builtin used as a bare name inside the module; setattr
        # with raising=False adds it to the module namespace, which shadows
        # the builtin for subsequent calls in that module.
        monkeypatch.setattr(ci_mod, "open", deny, raising=False)
        _ensure_trailing_newline(str(p))  # should not raise
        captured = capsys.readouterr()
        assert "_ensure_trailing_newline" in captured.out


# --------------------------------------------------------------------------- #
# P1-8: tail health counter
# --------------------------------------------------------------------------- #


class TestTailHealthCounter:
    def _setup(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)
        monkeypatch.setattr(_time_module, "sleep", lambda *_: None)
        fixed = datetime(2026, 5, 14, tzinfo=timezone.utc)
        monkeypatch.setattr(ci_mod, "datetime", _frozen_dt_class(fixed))
        it, dp = _mk_iter(tmp_path, base_tf="1m")
        return it, dp

    def test_threshold_constant_value(self):
        assert _TAIL_MALFORMED_RESYNC_THRESHOLD == 10

    def test_few_malformed_rows_no_resync(self, tmp_path, monkeypatch, capsys):
        it, dp = self._setup(tmp_path, monkeypatch)
        p = dp / "2026-05-14.csv"
        # 5 malformed rows + a valid one
        content = "timestamp,open,close,high,low,volume\n"
        for i in range(5):
            content += f"garbage,row,number,{i},nope,bad\n"
        content += "60000,1.0,1.5,2.0,0.5,10.0\n"
        p.write_text(content)
        # Force tail to read from 0
        it._tail_offset = 0
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        # No force-resync warning
        captured = capsys.readouterr()
        assert "force-resync" not in captured.out
        # Valid row was ingested
        assert it._last_ts == 60_000
        # Counter reset after the valid row
        assert it._tail_malformed_count == 0

    def test_threshold_malformed_rows_triggers_resync(
        self, tmp_path, monkeypatch, capsys
    ):
        it, dp = self._setup(tmp_path, monkeypatch)
        p = dp / "2026-05-14.csv"
        # Threshold many malformed rows, NO valid rows
        content = "timestamp,open,close,high,low,volume\n"
        for i in range(_TAIL_MALFORMED_RESYNC_THRESHOLD + 2):
            content += f"garbage,row,{i},bad,data,nope\n"
        p.write_text(content)
        it._tail_offset = 0
        # Pre-populate something to verify offset reset
        it._tail_offset = len(content.encode("utf-8"))
        it._tail_last_line_len = 50
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        captured = capsys.readouterr()
        assert "force-resync" in captured.out
        assert "malformed_threshold" in captured.out
        # Reset state
        assert it._tail_offset == 0
        assert it._tail_last_line_len == 0
        assert it._tail_malformed_count == 0

    def test_mid_file_header_triggers_resync(self, tmp_path, monkeypatch, capsys):
        """A header line at any non-zero file offset triggers force-resync.
        Construction: pad the file so the corrupting header sits past byte 316
        (the small-backtrack threshold) AND the _tail_offset is at EOF so
        start_offset > 0 lands well after the legitimate header.
        """
        it, dp = self._setup(tmp_path, monkeypatch)
        p = dp / "2026-05-14.csv"
        # Build a large enough file that start_offset > 0 after backtrack
        lines = ["timestamp,open,close,high,low,volume"]
        # Many valid rows to push the corrupting header far into the file
        for i in range(50):
            ts = 60_000 + i * 60_000
            lines.append(f"{ts},1.0,1.5,2.0,0.5,10.0")
        # Corrupting mid-file header (just before EOF)
        lines.append("timestamp,open,close,high,low,volume")
        # One more valid row after the corrupting header
        lines.append("9999999999,9.0,9.5,10.0,8.5,99.0")
        content = "\n".join(lines) + "\n"
        p.write_text(content)

        # Position the tail so start_offset > 0 and the corrupting header is
        # inside the read window. _tail_last_line_len kept small so the
        # dynamic backtrack is also small (~316 bytes).
        file_size = len(content.encode("utf-8"))
        it._tail_offset = file_size
        it._tail_last_line_len = 30  # → backtrack = min(4096, 30*2+256) = 316

        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        captured = capsys.readouterr()
        assert "force-resync" in captured.out
        assert "mid_file_header" in captured.out
        # Reset state
        assert it._tail_offset == 0
        assert it._tail_malformed_count == 0

    def test_valid_row_resets_malformed_counter(self, tmp_path, monkeypatch):
        it, dp = self._setup(tmp_path, monkeypatch)
        p = dp / "2026-05-14.csv"
        content = "timestamp,open,close,high,low,volume\n"
        # 3 malformed rows, then a valid row, then a malformed
        for i in range(3):
            content += f"bad,row,here,{i},nope,nope\n"
        content += "60000,1.0,1.5,2.0,0.5,10.0\n"
        content += "bad,row,here,99,nope,nope\n"
        p.write_text(content)
        it._tail_offset = 0
        it._poll_for_new_data_and_buffer(
            sleep_first=False, resync=False, stop_after_first_emission=False
        )
        # After the valid row, counter went back to 0; the FINAL malformed
        # row bumps it to 1 (well below threshold).
        assert it._tail_malformed_count == 1
