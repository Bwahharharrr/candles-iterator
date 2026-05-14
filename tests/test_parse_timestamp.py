"""Tests for parse_timestamp.

Phase P0-2 of the test-coverage plan: pin the date/time parsing contract,
especially the end-of-day fix in commit 2836cda that previously returned
next-day-midnight (silently including row 0 of the next day in finite-range
runs).

Target: candle_iterator/candle_iterator.py:1191-1206.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from candle_iterator.candle_iterator import parse_timestamp


def _utc_ms(year, month, day, hour=0, minute=0, second=0, ms=0) -> int:
    """Helper: build a UTC ms timestamp for a known calendar point."""
    dt = datetime(year, month, day, hour, minute, second, ms * 1000, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


# --------------------------------------------------------------------------- #
# None / empty input
# --------------------------------------------------------------------------- #


class TestNoneInput:
    def test_none_returns_none(self):
        assert parse_timestamp(None) is None

    def test_empty_string_returns_none(self):
        assert parse_timestamp("") is None

    def test_none_input_is_start_argument_ignored(self):
        """`is_start` is irrelevant when input is None."""
        assert parse_timestamp(None, is_start=True) is None
        assert parse_timestamp(None, is_start=False) is None


# --------------------------------------------------------------------------- #
# Date-only (YYYY-MM-DD) — the core contract
# --------------------------------------------------------------------------- #


class TestDateOnly:
    def test_start_is_midnight_utc(self):
        result = parse_timestamp("2026-01-15", is_start=True)
        assert result == _utc_ms(2026, 1, 15, 0, 0, 0, 0)

    def test_end_is_one_ms_before_next_midnight(self):
        """The audit fix (commit 2836cda): end-of-day must cover 23:59:59.999,
        NOT roll over into the next day's first millisecond.
        """
        result = parse_timestamp("2026-01-15", is_start=False)
        # Triangulation 1: derive via datetime arithmetic (mirrors implementation)
        next_midnight = _utc_ms(2026, 1, 16, 0, 0, 0, 0)
        assert result == next_midnight - 1
        # Triangulation 2: hand-computed expected literal for 2026-01-15 23:59:59.999 UTC
        # 2026-01-15 00:00:00 UTC = 1768435200000 ms (verified via standalone calc)
        # plus 86399999 ms (=24*3600*1000 - 1) = 1768521599999
        assert result == 1768521599999

    def test_default_is_start_true(self):
        """Omitting `is_start` must default to start-of-day, NOT end."""
        result = parse_timestamp("2026-01-15")
        midnight = _utc_ms(2026, 1, 15)
        assert result == midnight

    def test_end_specifically_returns_23_59_59_999(self):
        """Pinned end-of-day arithmetic precision: parsed value matches the
        UTC datetime built for 23:59:59.999.
        """
        result = parse_timestamp("2026-01-15", is_start=False)
        expected = _utc_ms(2026, 1, 15, 23, 59, 59, 999)
        assert result == expected

    def test_returns_int_not_float(self):
        result = parse_timestamp("2026-01-15", is_start=True)
        assert isinstance(result, int)


# --------------------------------------------------------------------------- #
# Date-only — month/year/leap boundaries
# --------------------------------------------------------------------------- #


class TestDateOnlyBoundaries:
    def test_end_of_december(self):
        result = parse_timestamp("2026-12-31", is_start=False)
        assert result == _utc_ms(2026, 12, 31, 23, 59, 59, 999)

    def test_start_of_january(self):
        result = parse_timestamp("2026-01-01", is_start=True)
        assert result == _utc_ms(2026, 1, 1)

    def test_end_of_february_non_leap(self):
        result = parse_timestamp("2026-02-28", is_start=False)
        assert result == _utc_ms(2026, 2, 28, 23, 59, 59, 999)

    def test_end_of_february_leap_year(self):
        """2024 is a leap year — Feb has 29 days."""
        result = parse_timestamp("2024-02-29", is_start=False)
        assert result == _utc_ms(2024, 2, 29, 23, 59, 59, 999)

    def test_non_leap_year_feb_29_raises(self):
        """2026 is NOT a leap year — strptime must reject Feb 29."""
        with pytest.raises(ValueError):
            parse_timestamp("2026-02-29")


# --------------------------------------------------------------------------- #
# UTC purity (no local-zone bleed)
# --------------------------------------------------------------------------- #


class TestUTCIsolation:
    def test_us_dst_start_date_is_pure_utc(self):
        """2026-03-08 is when US DST begins. Confirm parse_timestamp returns
        the true UTC midnight, not a local-zone shifted value.
        """
        result = parse_timestamp("2026-03-08", is_start=True)
        assert result == _utc_ms(2026, 3, 8, 0, 0, 0, 0)

    def test_end_of_dst_date_is_pure_utc(self):
        """End-of-day on a DST start date must still be 23:59:59.999 UTC."""
        result = parse_timestamp("2026-03-08", is_start=False)
        assert result == _utc_ms(2026, 3, 8, 23, 59, 59, 999)


# --------------------------------------------------------------------------- #
# Date+time (YYYY-MM-DD HH:MM) — `is_start` is irrelevant here
# --------------------------------------------------------------------------- #


class TestDateTime:
    def test_minute_precision_start(self):
        result = parse_timestamp("2026-01-15 14:30", is_start=True)
        assert result == _utc_ms(2026, 1, 15, 14, 30, 0, 0)

    def test_minute_precision_end(self):
        """For HH:MM input the implementation IGNORES `is_start` (line 1203
        path doesn't apply the end-of-day shift). Pin this invariant.
        """
        result = parse_timestamp("2026-01-15 14:30", is_start=False)
        assert result == _utc_ms(2026, 1, 15, 14, 30, 0, 0)

    def test_is_start_does_not_shift_hhmm_value(self):
        """Same input with is_start=True vs is_start=False must be equal."""
        a = parse_timestamp("2026-06-15 09:00", is_start=True)
        b = parse_timestamp("2026-06-15 09:00", is_start=False)
        assert a == b

    def test_midnight_hhmm(self):
        """Explicit midnight via HH:MM equals date-only start of same day."""
        a = parse_timestamp("2026-01-15 00:00")
        b = parse_timestamp("2026-01-15", is_start=True)
        assert a == b


# --------------------------------------------------------------------------- #
# Invalid input — must raise ValueError with input echoed
# --------------------------------------------------------------------------- #


class TestInvalidFormat:
    @pytest.mark.parametrize(
        "bad",
        [
            "15-01-2026",            # wrong field order
            "2026/01/15",            # wrong separator
            "not-a-date",            # arbitrary 10-char garbage
            "abcdefghij",            # 10 chars but unparseable
            "2026-13-01",            # invalid month
            "2026-01-32",            # invalid day
            "2026-01-15T14:30",      # ISO-ish with T separator
            "2026-01-15Z",           # ISO-ish suffix
            "2026-01-15 14:30:00",   # seconds precision unsupported
            "2026-01-15 14:30.000",      # millisecond precision unsupported
            "2026-01-15 14:30:00.000",   # seconds+ms precision unsupported
            "2026-01-15 14",             # missing minute and colon
            "2026-01-15 ",           # trailing whitespace, no time
            " 2026-01-15",           # leading whitespace
            "2026",                  # year only
        ],
    )
    def test_invalid_format_raises(self, bad):
        with pytest.raises(ValueError) as excinfo:
            parse_timestamp(bad)
        # The implementation re-raises a custom message that ALWAYS includes
        # the offending input — pin both pieces (substring AND prefix).
        msg = str(excinfo.value)
        assert "Invalid date format" in msg
        assert bad in msg

    def test_error_message_includes_input(self):
        with pytest.raises(ValueError, match=r"Invalid date format: garbage"):
            parse_timestamp("garbage")
