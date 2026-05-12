"""Tests for the force_close_final flag.

Phase F2 of the same-day-signal project: verify that
  - the default (force_close_final=False) preserves the prior "do not
    force-close the base TF on wall-clock time" behaviour,
  - force_close_final=True moves the base aggregator's last open candle into
    closed_candles via a real record_closure event (not just the final
    partial snapshot),
  - the force-close is idempotent under repeated flush(),
  - no _seed_next_open is performed (no phantom next bar),
  - higher-TF propagation through record_closure still works correctly,
  - the flag combines cleanly with anchor_offsets.

These tests drive the AggregatorManager via base_agg.on_base_csv_row(), which
is the public ingestion seam. The CSV reader is intentionally not exercised
here; that integration is covered by the existing test_anchor_boundary.py
factory test plus higher-level integration tests in quant-buffers.
"""
from __future__ import annotations

import inspect
from datetime import datetime, timezone

import pytest

from candle_iterator import create_candle_iterator
from candle_iterator.candle_iterator import AggregatorManager, CandleAggregator


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

DAY_MS = 86_400_000
MON_APR_13 = int(datetime(2026, 4, 13, tzinfo=timezone.utc).timestamp() * 1000)
MON_OFFSET_MS = -3 * DAY_MS


def _feed(mgr: AggregatorManager, bars) -> None:
    """Push (ts, o, h, l, c, v) tuples into the base aggregator."""
    for ts, o, h, l, c, v in bars:
        mgr.base_agg.on_base_csv_row(ts, o, h, l, c, v)


def _make_bars(start_ts: int, n: int) -> list[tuple]:
    """Generate n consecutive daily bars."""
    return [(start_ts + i * DAY_MS, 100.0, 102.0, 99.0, 101.0, 1000.0) for i in range(n)]


# --------------------------------------------------------------------------- #
# Flag wiring on AggregatorManager
# --------------------------------------------------------------------------- #

class TestAggregatorManagerFlag:
    def test_default_false_when_omitted(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"])
        assert mgr.force_close_final is False

    def test_explicit_true_stores_True(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"], force_close_final=True)
        assert mgr.force_close_final is True


# --------------------------------------------------------------------------- #
# create_candle_iterator factory passthrough
# --------------------------------------------------------------------------- #

class TestCreateCandleIteratorPassthrough:
    def test_signature_accepts_force_close_final(self):
        sig = inspect.signature(create_candle_iterator)
        assert "force_close_final" in sig.parameters
        assert sig.parameters["force_close_final"].default is False

    def test_factory_threads_force_close_final_into_manager(self, tmp_path, monkeypatch):
        """Construct the iterator with the flag set; confirm the AggregatorManager
        on the iterator has force_close_final=True.
        """
        from candle_iterator import candle_iterator as ci_mod

        # Stub sync so the iterator never hits the network
        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)

        data_path = tmp_path / "EODHD" / "candles" / "TEST.US" / "1D"
        data_path.mkdir(parents=True)

        it = ci_mod.create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1D", aggregation_timeframes=["1D", "1W"],
            data_dir=str(tmp_path),
            force_close_final=True,
        )
        assert it.manager.force_close_final is True

    def test_factory_default_force_close_final_is_False(self, tmp_path, monkeypatch):
        from candle_iterator import candle_iterator as ci_mod

        monkeypatch.setattr(ci_mod, "synchronize_candle_data", lambda **kw: True)
        data_path = tmp_path / "EODHD" / "candles" / "TEST.US" / "1D"
        data_path.mkdir(parents=True)

        it = ci_mod.create_candle_iterator(
            exchange="EODHD", ticker="TEST.US",
            base_timeframe="1D", aggregation_timeframes=["1D"],
            data_dir=str(tmp_path),
        )
        assert it.manager.force_close_final is False


# --------------------------------------------------------------------------- #
# Default (legacy) behaviour: force_close_final=False
# --------------------------------------------------------------------------- #

class TestFlushDefault:
    def test_empty_flush_returns_empty_list(self):
        """Pinned contract per Codex round-2: a flush with no data returns []."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"])
        assert mgr.flush() == []

    def test_single_bar_stays_in_open_candles(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"])
        _feed(mgr, _make_bars(MON_APR_13, 1))
        result = mgr.flush()
        # At least the final partial snapshot is emitted
        assert len(result) >= 1
        # The last closure is the partial with is_final=True
        final = result[-1]
        assert final.is_final is True
        # 1D is in open_candles, NOT closed_candles
        assert "1D" in final.open_candles
        assert "1D" not in final.closed_candles

    def test_three_bars_pending_emit_then_last_is_partial(self):
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"])
        _feed(mgr, _make_bars(MON_APR_13, 3))
        result = mgr.flush()
        # The final closure is a partial with the last bar in open_candles
        final = result[-1]
        assert final.is_final is True
        assert "1D" in final.open_candles


# --------------------------------------------------------------------------- #
# force_close_final=True behaviour
# --------------------------------------------------------------------------- #

class TestFlushForceClose:
    def test_single_bar_moves_to_closed_candles(self):
        """The last (and only) bar must be in closed_candles, not open_candles."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"], force_close_final=True)
        _feed(mgr, _make_bars(MON_APR_13, 1))
        result = mgr.flush()
        assert len(result) >= 1
        # A closure containing 1D in closed_candles must exist
        had_closed_1d = any("1D" in c.closed_candles for c in result)
        assert had_closed_1d, (
            f"Expected 1D in closed_candles of some closure; got "
            f"{[list(c.closed_candles) for c in result]}"
        )
        # 1D must NOT appear in any closure's open_candles
        for c in result:
            assert "1D" not in c.open_candles, (
                f"1D should not be in open_candles after force-close; "
                f"found in closure with is_final={c.is_final}"
            )

    def test_force_closed_candle_has_correct_OHLCV(self):
        """Verify the force-closed bar carries the data we fed."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"], force_close_final=True)
        # Distinctive OHLCV to spot it
        _feed(mgr, [(MON_APR_13, 100.0, 102.0, 99.0, 101.0, 1000.0)])
        result = mgr.flush()
        closed_1d = [c for c in result if "1D" in c.closed_candles]
        assert len(closed_1d) >= 1
        candle = closed_1d[0].closed_candles["1D"]
        assert candle.timestamp == MON_APR_13
        assert candle.open == 100.0
        assert candle.close == 101.0
        assert candle.high == 102.0
        assert candle.low == 99.0
        assert candle.volume == 1000.0

    def test_no_seed_next_open_after_force_close_one_bar(self):
        """After force-close with a single bar fed (no natural transitions),
        no seed should exist. This is the trivial baseline."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"], force_close_final=True)
        _feed(mgr, _make_bars(MON_APR_13, 1))
        mgr.flush()
        assert mgr.base_agg._seed_next_open_ts is None
        assert mgr.base_agg._seed_next_open_price is None

    def test_no_seed_next_open_after_force_close_multiple_bars(self):
        """Regression: with 2+ bars, natural transitions inside _consume_subcandle
        call _seed_next_open on every bar boundary. The force-close MUST clear
        the pre-existing seed so the base TF doesn't reappear in open_candles
        via the seeded-preview path of get_current_open_candle().
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"], force_close_final=True)
        _feed(mgr, _make_bars(MON_APR_13, 3))
        # Before flush, seed IS set from natural transitions
        assert mgr.base_agg._seed_next_open_ts is not None, (
            "Sanity: natural bar transitions should set the seed"
        )
        mgr.flush()
        assert mgr.base_agg._seed_next_open_ts is None, (
            "force-close must clear the seed even when natural transitions set it"
        )
        assert mgr.base_agg._seed_next_open_price is None

    def test_force_close_removes_base_tf_from_open_preview(self):
        """After force-close with multiple bars, calling _current_open_candles()
        directly must NOT return 1D — the bar is now in closed_candles only.
        Asserts the pre-condition (base TF IS in the preview before flush) so
        the post-condition is a true removal, not vacuous absence.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"], force_close_final=True)
        _feed(mgr, _make_bars(MON_APR_13, 3))
        # Pre-condition: bar 3 is in the open preview before flush
        open_before = mgr._current_open_candles()
        assert "1D" in open_before, (
            f"Sanity: 1D must be present in the open preview before flush "
            f"(got {list(open_before)})"
        )
        mgr.flush()
        # Post-condition: 1D is no longer in the open preview
        open_now = mgr._current_open_candles()
        assert "1D" not in open_now, (
            f"After force-close + cleanup, 1D must not appear in open snapshot; "
            f"got {list(open_now)}"
        )

    def test_repeated_flush_is_idempotent(self):
        """Second flush() must NOT re-emit the same base closure."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"], force_close_final=True)
        _feed(mgr, _make_bars(MON_APR_13, 1))
        first = mgr.flush()
        second = mgr.flush()
        first_1d = [c for c in first if "1D" in c.closed_candles]
        second_1d = [c for c in second if "1D" in c.closed_candles]
        assert len(first_1d) >= 1, "first flush must close the base bar"
        assert len(second_1d) == 0, (
            f"second flush must NOT re-close the base bar; got {len(second_1d)} "
            f"1D closures: {second_1d}"
        )

    def test_force_close_with_no_data_is_noop(self):
        """Empty manager + force_close_final=True + flush() => empty list."""
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"], force_close_final=True)
        # No bars fed
        assert mgr.flush() == []

    def test_final_partial_still_emitted_and_excludes_base_tf(self):
        """Two-in-one: (1) the final partial snapshot (is_final=True) is still
        emitted when force_close_final is True — the force-close does NOT
        suppress the partial-snapshot mechanism; (2) the partial's open_candles
        does NOT contain the base TF because it moved to closed_candles. The
        partial's closed_candles MAY contain the base TF (because the partial
        is built from a copy of latest_closed_candles); that is acceptable for
        downstream consumers which only read from closed_candles.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["1W"], force_close_final=True)
        _feed(mgr, _make_bars(MON_APR_13, 1))
        result = mgr.flush()
        partials = [c for c in result if c.is_final is True]
        # (1) is_final=True closure must still be emitted (1W remains open)
        assert len(partials) >= 1, (
            f"Force-close must not suppress the final partial snapshot; got "
            f"{len(partials)} partials in {result}"
        )
        # (2) Force-closed base TF must not be in any partial's open_candles
        for p in partials:
            assert "1D" not in p.open_candles, (
                f"Force-closed base TF should not be in any partial snapshot's "
                f"open_candles; found {list(p.open_candles)}"
            )


# --------------------------------------------------------------------------- #
# Higher-TF propagation
# --------------------------------------------------------------------------- #

class TestForceCloseHigherTfPropagation:
    """Force-closing the base bar must propagate to higher TFs via
    record_closure -> on_base_candle_closed.
    """

    def test_higher_tf_completes_sub_factor_via_force_close(self):
        """For base=1D + higher=3D (sub_factor=3): feed 3 daily bars and
        force-close the last one. The 3D aggregator's sub_count goes
        2 (bars 1,2 closed via normal ingestion) -> 3 (bar 3 force-closed).
        Threshold reached -> 3D finalizes.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"], force_close_final=True)
        _feed(mgr, _make_bars(MON_APR_13, 3))
        result = mgr.flush()
        had_3d = any("3D" in c.closed_candles for c in result)
        assert had_3d, (
            f"Expected 3D in closed_candles after force-close completes its "
            f"sub_factor; got "
            f"{[list(c.closed_candles) for c in result]}"
        )

    def test_higher_tf_partial_state_not_force_closed(self):
        """Feed only 2 daily bars. 3D's sub_count goes to 2 (below threshold=3)
        after force-close. 3D should NOT be in closed_candles.
        """
        mgr = AggregatorManager(base_tf="1D", higher_tfs=["3D"], force_close_final=True)
        _feed(mgr, _make_bars(MON_APR_13, 2))
        result = mgr.flush()
        had_3d = any("3D" in c.closed_candles for c in result)
        assert not had_3d, (
            "3D should NOT be force-closed when sub_factor isn't met by the "
            "force-closed base bar"
        )


# --------------------------------------------------------------------------- #
# Cross-feature: force_close_final + anchor_offsets
# --------------------------------------------------------------------------- #

class TestForceCloseWithAnchorOffset:
    """The two opt-in features must compose cleanly."""

    def test_force_close_combined_with_mon_anchor(self):
        mgr = AggregatorManager(
            base_tf="1D",
            higher_tfs=["1W"],
            anchor_offsets={"1W": MON_OFFSET_MS},
            force_close_final=True,
        )
        # Feed exactly Monday's daily bar (a clean Mon-aligned 1W boundary)
        _feed(mgr, _make_bars(MON_APR_13, 1))
        result = mgr.flush()
        # 1D must be force-closed
        had_1d_closed = any("1D" in c.closed_candles for c in result)
        assert had_1d_closed, "1D should be force-closed under combined flags"
        # 1W aggregator's current_boundary should still align to Monday
        w_agg = [a for a in mgr.higher_aggs if a.tf == "1W"][0]
        assert w_agg.current_boundary == MON_APR_13, (
            f"1W boundary should align to Monday {MON_APR_13}; "
            f"got {w_agg.current_boundary}"
        )
