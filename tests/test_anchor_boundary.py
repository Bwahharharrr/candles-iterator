"""Tests for the anchor_offset_ms / anchor_offsets boundary-alignment feature.

Phase 3 of the Monday-week project: verify that
  - the legacy default (offset=0) behaves identically to the prior epoch-aligned
    `(ts // tf_ms) * tf_ms` formula,
  - non-zero offsets shift the boundary by the configured amount,
  - the helper is robust to magnitude/sign of the offset (modulo tf_ms),
  - AggregatorManager threads per-TF offsets to the right aggregator.

These tests intentionally exercise only `_align_to_boundary` and the manager
wiring. Full closed-candle ingestion through the boundary-jump path is left to
the P4 integration phase.
"""
from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

import pytest

from candle_iterator import create_candle_iterator
from candle_iterator.candle_iterator import (
    AggregatorManager,
    CandleAggregator,
    TIMEFRAMES,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS

THU_OFFSET_MS = 0                    # legacy: 1970-01-01 was Thursday
MON_OFFSET_MS = -3 * DAY_MS          # Monday anchor (Thu - 3 = Mon)
FRI_OFFSET_MS = 1 * DAY_MS           # Friday anchor (Thu + 1 = Fri)


def _ts(year: int, month: int, day: int) -> int:
    """UTC midnight ms timestamp for a date."""
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000)


def _legacy_floor(ts: int, tf_ms: int) -> int:
    """The exact prior formula we replaced — used for regression equivalence."""
    return (ts // tf_ms) * tf_ms


def _mk_1w_agg(offset_ms: int = 0) -> CandleAggregator:
    """Build a standalone 1W CandleAggregator for unit testing (no manager)."""
    return CandleAggregator(
        timeframe="1W",
        manager=None,
        is_base=False,
        sub_factor=7,
        anchor_offset_ms=offset_ms,
    )


# --------------------------------------------------------------------------- #
# Legacy default (offset=0) — Thursday alignment, must match prior formula bit-for-bit
# --------------------------------------------------------------------------- #

class TestAlignToBoundaryLegacy:
    """offset=0 must produce identical results to (ts // tf_ms) * tf_ms."""

    def test_wed_aligns_to_prior_thursday(self):
        agg = _mk_1w_agg(0)
        # Wed 2026-04-15 -> Thu 2026-04-09
        wed = _ts(2026, 4, 15)
        expected_thu = _ts(2026, 4, 9)
        assert agg._align_to_boundary(wed) == expected_thu

    def test_thursday_aligns_to_self(self):
        agg = _mk_1w_agg(0)
        thu = _ts(2026, 4, 9)
        assert agg._align_to_boundary(thu) == thu

    def test_one_minute_before_next_thursday_still_prior_week(self):
        agg = _mk_1w_agg(0)
        # Wed 2026-04-15 23:59 -> still in window starting Thu 04-09
        almost = _ts(2026, 4, 16) - 60_000
        assert agg._align_to_boundary(almost) == _ts(2026, 4, 9)

    def test_year_boundary_2025_to_2026(self):
        agg = _mk_1w_agg(0)
        # Random day in Jan 2026
        jan_5_2026 = _ts(2026, 1, 5)  # Monday
        # Legacy formula expectation
        expected = _legacy_floor(jan_5_2026, WEEK_MS)
        assert agg._align_to_boundary(jan_5_2026) == expected
        # Sanity: result is on Thursday
        dt = datetime.fromtimestamp(expected / 1000, tz=timezone.utc)
        assert dt.weekday() == 3  # Thursday

    def test_zero_offset_matches_legacy_formula_across_full_year(self):
        """Bit-for-bit regression vs the prior formula for every Mon of 2025."""
        agg = _mk_1w_agg(0)
        d = datetime(2025, 1, 6, tzinfo=timezone.utc)  # First Mon of 2025
        end = datetime(2025, 12, 31, tzinfo=timezone.utc)
        while d <= end:
            ts = int(d.timestamp() * 1000)
            assert agg._align_to_boundary(ts) == _legacy_floor(ts, WEEK_MS), (
                f"Legacy regression failed at {d}: "
                f"_align={agg._align_to_boundary(ts)} vs legacy={_legacy_floor(ts, WEEK_MS)}"
            )
            d += timedelta(days=7)


# --------------------------------------------------------------------------- #
# Monday anchor (offset = -3 days)
# --------------------------------------------------------------------------- #

class TestAlignToBoundaryMondayOffset:
    """anchor_offset_ms=-259_200_000 should produce Monday-aligned weekly boundaries."""

    def test_wed_aligns_to_prior_monday(self):
        agg = _mk_1w_agg(MON_OFFSET_MS)
        wed = _ts(2026, 4, 15)
        expected_mon = _ts(2026, 4, 13)
        assert agg._align_to_boundary(wed) == expected_mon

    def test_monday_aligns_to_self(self):
        agg = _mk_1w_agg(MON_OFFSET_MS)
        mon = _ts(2026, 4, 13)
        assert agg._align_to_boundary(mon) == mon

    def test_sunday_aligns_to_prior_monday(self):
        agg = _mk_1w_agg(MON_OFFSET_MS)
        sun = _ts(2026, 4, 19)
        assert agg._align_to_boundary(sun) == _ts(2026, 4, 13)

    def test_next_monday_starts_new_week(self):
        agg = _mk_1w_agg(MON_OFFSET_MS)
        next_mon = _ts(2026, 4, 20)
        assert agg._align_to_boundary(next_mon) == next_mon

    def test_iso_week_ground_truth_every_monday_2025(self):
        """For every Monday in 2025, `_align_to_boundary(monday)` must equal that Monday."""
        agg = _mk_1w_agg(MON_OFFSET_MS)
        d = datetime(2025, 1, 6, tzinfo=timezone.utc)  # First Mon of 2025
        end = datetime(2025, 12, 31, tzinfo=timezone.utc)
        while d <= end:
            assert d.weekday() == 0, "fixture sanity: d must be a Monday"
            ts = int(d.timestamp() * 1000)
            assert agg._align_to_boundary(ts) == ts
            d += timedelta(days=7)

    def test_year_boundary_week_spans_dec_into_jan(self):
        """Mon-anchored week of Mon 2025-12-29 contains Sun 2026-01-04."""
        agg = _mk_1w_agg(MON_OFFSET_MS)
        mon_dec_29_2025 = _ts(2025, 12, 29)
        sun_jan_4_2026 = _ts(2026, 1, 4)
        wed_dec_31_2025 = _ts(2025, 12, 31)
        for ts in (mon_dec_29_2025, wed_dec_31_2025, sun_jan_4_2026):
            assert agg._align_to_boundary(ts) == mon_dec_29_2025

    def test_all_results_fall_on_monday_for_random_dates(self):
        """Property check: any input maps to a Monday boundary."""
        agg = _mk_1w_agg(MON_OFFSET_MS)
        d = datetime(2024, 6, 1, tzinfo=timezone.utc)
        end = datetime(2026, 6, 1, tzinfo=timezone.utc)
        while d <= end:
            ts = int(d.timestamp() * 1000)
            boundary = agg._align_to_boundary(ts)
            bdt = datetime.fromtimestamp(boundary / 1000, tz=timezone.utc)
            assert bdt.weekday() == 0, f"Boundary for {d} fell on {bdt} (weekday {bdt.weekday()})"
            d += timedelta(days=1)


# --------------------------------------------------------------------------- #
# Generic offset properties (sign / magnitude / modulo)
# --------------------------------------------------------------------------- #

class TestAlignToBoundaryGenericOffset:
    """Offset is reduced modulo tf_ms; offsets one period apart must produce identical
    boundaries; positive vs negative offsets should both work."""

    def test_friday_positive_offset(self):
        """Offset +1 day from Thu = Friday anchor. Verify boundary lands on a Friday."""
        agg = _mk_1w_agg(FRI_OFFSET_MS)
        wed = _ts(2026, 4, 15)
        boundary = agg._align_to_boundary(wed)
        bdt = datetime.fromtimestamp(boundary / 1000, tz=timezone.utc)
        assert bdt.weekday() == 4, f"Expected Friday, got {bdt}"

    def test_offset_minus_one_week_equivalent_to_zero(self):
        """offset = -tf_ms should produce identical boundary to offset = 0."""
        agg0 = _mk_1w_agg(0)
        agg_neg = _mk_1w_agg(-WEEK_MS)
        for ts in (_ts(2026, 1, 1), _ts(2026, 4, 15), _ts(2025, 12, 31)):
            assert agg0._align_to_boundary(ts) == agg_neg._align_to_boundary(ts)

    def test_offset_plus_one_week_equivalent_to_zero(self):
        """offset = +tf_ms should also produce identical boundary to offset = 0."""
        agg0 = _mk_1w_agg(0)
        agg_pos = _mk_1w_agg(WEEK_MS)
        for ts in (_ts(2026, 1, 1), _ts(2026, 4, 15), _ts(2025, 12, 31)):
            assert agg0._align_to_boundary(ts) == agg_pos._align_to_boundary(ts)

    def test_advance_by_tf_ms_preserves_alignment(self):
        """Starting from any aligned boundary B, B + tf_ms must also be aligned
        (it must equal `_align_to_boundary(B + tf_ms)`).
        """
        for offset in (0, MON_OFFSET_MS, FRI_OFFSET_MS):
            agg = _mk_1w_agg(offset)
            b = agg._align_to_boundary(_ts(2026, 4, 15))
            assert agg._align_to_boundary(b + WEEK_MS) == b + WEEK_MS


# --------------------------------------------------------------------------- #
# AggregatorManager threading
# --------------------------------------------------------------------------- #

class TestAggregatorManagerThreading:
    """AggregatorManager must thread per-TF offsets into the correct CandleAggregator."""

    def test_default_no_offsets_dict_uses_zero(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"])
        w_agg = [a for a in mgr.higher_aggs if a.tf == "1W"][0]
        assert w_agg.anchor_offset_ms == 0
        assert mgr.base_agg.anchor_offset_ms == 0

    def test_mon_offset_applied_to_1W_only(self):
        mgr = AggregatorManager(
            base_tf="1D",
            higher_tfs=["3D", "1W"],
            anchor_offsets={"1W": MON_OFFSET_MS},
        )
        for agg in mgr.higher_aggs:
            if agg.tf == "1W":
                assert agg.anchor_offset_ms == MON_OFFSET_MS
            else:
                assert agg.anchor_offset_ms == 0
        assert mgr.base_agg.anchor_offset_ms == 0

    def test_offset_applied_to_base_too_if_specified(self):
        mgr = AggregatorManager(
            base_tf="1D",
            higher_tfs=["1W"],
            anchor_offsets={"1D": DAY_MS, "1W": MON_OFFSET_MS},
        )
        assert mgr.base_agg.anchor_offset_ms == DAY_MS
        w_agg = [a for a in mgr.higher_aggs if a.tf == "1W"][0]
        assert w_agg.anchor_offset_ms == MON_OFFSET_MS

    def test_offsets_stored_on_manager_for_introspection(self):
        """The manager keeps a copy of the offsets dict for downstream inspection."""
        mgr = AggregatorManager(
            base_tf="1D",
            higher_tfs=["1W"],
            anchor_offsets={"1W": MON_OFFSET_MS},
        )
        assert mgr.anchor_offsets == {"1W": MON_OFFSET_MS}


# --------------------------------------------------------------------------- #
# create_candle_iterator factory passthrough
# --------------------------------------------------------------------------- #

class TestCreateCandleIteratorPassthrough:
    """The factory must accept anchor_offsets and pass it down."""

    def test_signature_accepts_anchor_offsets_kwarg(self):
        sig = inspect.signature(create_candle_iterator)
        assert "anchor_offsets" in sig.parameters
        # Default is None
        assert sig.parameters["anchor_offsets"].default is None

    def test_factory_threads_offsets_into_manager(self, tmp_path, monkeypatch):
        """Construct the iterator under a stub corky dir and inspect the manager's
        offsets. We stub synchronize_candle_data so no network/IO happens, and
        pre-create the data dir so CandleIterator's existence check passes.
        """
        from candle_iterator import candle_iterator as ci_mod

        # Stub synchronize_candle_data so the iterator never tries to fetch
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)

        # The CandleIterator constructor verifies the data path exists.
        data_path = tmp_path / "EODHD" / "candles" / "TEST.US" / "1D"
        data_path.mkdir(parents=True)

        it = ci_mod.create_candle_iterator(
            exchange="EODHD",
            ticker="TEST.US",
            base_timeframe="1D",
            aggregation_timeframes=["1D", "1W"],
            data_dir=str(tmp_path),
            anchor_offsets={"1W": MON_OFFSET_MS},
        )

        # The iterator object built internally has a manager. Reach in via the
        # public Config/cfg path. CandleIterator stores .cfg with .ticker etc;
        # the actual AggregatorManager is constructed inside the factory and
        # held by the CandleIterator instance.
        mgr = getattr(it, "manager", None) or getattr(getattr(it, "cfg", None), "manager", None)
        if mgr is None:
            # Fallback: walk attributes
            for name in dir(it):
                obj = getattr(it, name, None)
                if isinstance(obj, AggregatorManager):
                    mgr = obj
                    break
        assert mgr is not None, "Could not locate AggregatorManager on iterator object"
        w_agg = [a for a in mgr.higher_aggs if a.tf == "1W"][0]
        assert w_agg.anchor_offset_ms == MON_OFFSET_MS
