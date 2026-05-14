"""Tests for parse_aggregation_timeframes / parse_single_relation_or_exact.

Phase P0-1 of the test-coverage plan: pin the CLI/--agg token parser semantics
so regressions in `<`, `<=`, `>`, `>=`, `&` intersection, dedup, sort, or
warning emission cannot land silently.

Targets in `candle_iterator/candle_iterator.py`:
  - parse_aggregation_timeframes()        line 417-433
  - parse_single_relation_or_exact()      line 436-462
  - TIMEFRAMES                            line 396-414
  - _RELATION_RE                          line 61
"""
from __future__ import annotations

import pytest

from candle_iterator.candle_iterator import (
    TIMEFRAMES,
    parse_aggregation_timeframes,
    parse_single_relation_or_exact,
)


# --------------------------------------------------------------------------- #
# parse_single_relation_or_exact — exact-TF passthrough
# --------------------------------------------------------------------------- #


class TestParseSingleExact:
    @pytest.mark.parametrize("tf", list(TIMEFRAMES.keys()))
    def test_every_known_tf_round_trips(self, tf):
        """Every key in TIMEFRAMES must round-trip as an exact token."""
        assert parse_single_relation_or_exact(tf) == {tf}

    def test_leading_and_trailing_whitespace_stripped(self):
        assert parse_single_relation_or_exact("  1h  ") == {"1h"}


# --------------------------------------------------------------------------- #
# parse_single_relation_or_exact — relational operators
# --------------------------------------------------------------------------- #


class TestParseSingleRelations:
    def test_strict_less_than_excludes_boundary(self):
        result = parse_single_relation_or_exact("<1h")
        assert result == {"1m", "5m", "15m", "30m"}

    def test_less_than_or_equal_includes_boundary(self):
        result = parse_single_relation_or_exact("<=1h")
        assert result == {"1m", "5m", "15m", "30m", "1h"}

    def test_strict_greater_than_excludes_boundary(self):
        result = parse_single_relation_or_exact(">1W")
        # Only 14D is larger than 1W (1W=604.8M, 14D=1209.6M)
        assert result == {"14D"}

    def test_greater_than_or_equal_includes_boundary(self):
        result = parse_single_relation_or_exact(">=1W")
        assert result == {"1W", "14D"}

    def test_less_than_smallest_returns_empty(self):
        assert parse_single_relation_or_exact("<1m") == set()

    def test_greater_than_largest_returns_empty(self):
        assert parse_single_relation_or_exact(">14D") == set()

    def test_less_than_or_equal_largest_returns_all(self):
        result = parse_single_relation_or_exact("<=14D")
        assert result == set(TIMEFRAMES.keys())

    def test_greater_than_or_equal_smallest_returns_all(self):
        result = parse_single_relation_or_exact(">=1m")
        assert result == set(TIMEFRAMES.keys())


# --------------------------------------------------------------------------- #
# parse_single_relation_or_exact — unknown TF and malformed expressions
# --------------------------------------------------------------------------- #


class TestParseSingleUnknownAndMalformed:
    def test_regex_match_with_unknown_tf_warns_unknown(self, capsys):
        """`>=2W` matches `_RELATION_RE` (2W passes [0-9]+[mhDWhd]+) but 2W is
        not a TIMEFRAMES key → 'Unknown timeframe' branch."""
        result = parse_single_relation_or_exact(">=2W")
        assert result == set()
        captured = capsys.readouterr()
        assert "Unknown timeframe" in captured.out
        assert "Could not parse" not in captured.out

    def test_lowercase_d_relation_warns_unknown_not_malformed(self, capsys):
        """`>=1d` matches the regex (lowercase d is in [mhDWhd]) but `1d` is
        not a TIMEFRAMES key (which uses uppercase D). Should hit the unknown
        branch, NOT the malformed branch."""
        result = parse_single_relation_or_exact(">=1d")
        assert result == set()
        captured = capsys.readouterr()
        assert "Unknown timeframe" in captured.out
        assert "Could not parse" not in captured.out

    def test_exact_unknown_token_warns_malformed(self, capsys):
        """`2W` (no operator) is not in TIMEFRAMES and doesn't match the
        relation regex → 'Could not parse' branch."""
        result = parse_single_relation_or_exact("2W")
        assert result == set()
        captured = capsys.readouterr()
        assert "Could not parse" in captured.out
        assert "Unknown timeframe" not in captured.out

    @pytest.mark.parametrize(
        "expr",
        ["garbage", "=1h", "1z", ">1z", ">=1h<=4h", ""],
    )
    def test_malformed_expressions_return_empty_with_warning(self, expr, capsys):
        result = parse_single_relation_or_exact(expr)
        assert result == set()
        captured = capsys.readouterr()
        assert "Could not parse" in captured.out

    def test_operator_only_no_tf_is_malformed(self, capsys):
        """`>=` without a TF should fall through to malformed."""
        result = parse_single_relation_or_exact(">=")
        assert result == set()
        captured = capsys.readouterr()
        assert "Could not parse" in captured.out


# --------------------------------------------------------------------------- #
# parse_aggregation_timeframes — multi-token union, dedup, sort
# --------------------------------------------------------------------------- #


class TestParseAggregationUnion:
    def test_single_exact_token(self):
        assert parse_aggregation_timeframes(["1h"]) == ["1h"]

    def test_two_disjoint_exact_tokens_unioned_sorted(self):
        """Sort is by tf_ms ascending. '1D'(86.4M) > '1h'(3.6M)."""
        assert parse_aggregation_timeframes(["1D", "1h"]) == ["1h", "1D"]

    def test_three_disjoint_exact_tokens(self):
        """Reverse alphabetical input must come out tf_ms-ascending."""
        result = parse_aggregation_timeframes(["1D", "1h", "5m"])
        assert result == ["5m", "1h", "1D"]

    def test_duplicate_exact_tokens_deduped(self):
        assert parse_aggregation_timeframes(["1h", "1h"]) == ["1h"]

    def test_duplicate_via_relation_overlap_deduped(self):
        """`<=1h` includes 1h; explicit `1h` token must not double-add it."""
        result = parse_aggregation_timeframes(["<=1h", "1h"])
        assert result == ["1m", "5m", "15m", "30m", "1h"]
        # Cardinality must equal unique set size
        assert len(result) == len(set(result))

    def test_relation_token_expands_then_sorts(self):
        assert parse_aggregation_timeframes(["<=1h"]) == [
            "1m", "5m", "15m", "30m", "1h"
        ]

    def test_empty_token_list_returns_empty_list(self):
        assert parse_aggregation_timeframes([]) == []

    def test_all_malformed_tokens_return_empty_list(self, capsys):
        result = parse_aggregation_timeframes(["garbage", "blah"])
        assert result == []
        captured = capsys.readouterr()
        assert captured.out.count("Could not parse") == 2

    def test_mixed_valid_and_invalid_returns_only_valid(self, capsys):
        result = parse_aggregation_timeframes(["1h", "garbage", "5m"])
        assert result == ["5m", "1h"]
        captured = capsys.readouterr()
        assert "Could not parse" in captured.out

    def test_exact_token_with_outer_whitespace_at_cli_layer(self):
        """`parse_aggregation_timeframes` defers to per-token strip via the
        single-parser; verify the CLI layer tolerates `'  1h  '` directly."""
        assert parse_aggregation_timeframes(["  1h  "]) == ["1h"]


# --------------------------------------------------------------------------- #
# parse_aggregation_timeframes — `&` intersection
# --------------------------------------------------------------------------- #


class TestParseAggregationIntersection:
    def test_closed_interval(self):
        """`>=1h&<=4h` → {1h, 2h, 3h, 4h} sorted."""
        assert parse_aggregation_timeframes([">=1h&<=4h"]) == [
            "1h", "2h", "3h", "4h"
        ]

    def test_open_interval(self):
        """`>1h&<4h` → {2h, 3h} sorted (boundaries excluded)."""
        assert parse_aggregation_timeframes([">1h&<4h"]) == ["2h", "3h"]

    def test_reversed_operand_order_yields_same_result(self):
        """Intersection is commutative — `<=4h&>=1h` == `>=1h&<=4h`."""
        a = parse_aggregation_timeframes([">=1h&<=4h"])
        b = parse_aggregation_timeframes(["<=4h&>=1h"])
        assert a == b

    def test_unsatisfiable_intersection_is_empty(self):
        """`>4h&<1h` has no overlap → []."""
        assert parse_aggregation_timeframes([">4h&<1h"]) == []

    def test_disjoint_exact_intersection_is_empty(self):
        assert parse_aggregation_timeframes(["1h&2h"]) == []

    def test_exact_plus_relation_intersection(self):
        """`1h&<=4h` → {1h} (1h is in <=4h)."""
        assert parse_aggregation_timeframes(["1h&<=4h"]) == ["1h"]

    def test_exact_plus_disjoint_relation(self):
        """`1h&<30m` → {} (1h not in <30m)."""
        assert parse_aggregation_timeframes(["1h&<30m"]) == []

    def test_intersection_with_garbage_side_is_empty(self, capsys):
        """Empty side from garbage drops the intersection to {}."""
        result = parse_aggregation_timeframes([">=1h&garbage"])
        assert result == []
        captured = capsys.readouterr()
        assert "Could not parse" in captured.out

    def test_intersection_with_empty_trailing_side(self, capsys):
        """`>=1h&` → second side is empty string → malformed warning → {}."""
        result = parse_aggregation_timeframes([">=1h&"])
        assert result == []
        captured = capsys.readouterr()
        assert "Could not parse" in captured.out

    def test_intersection_with_inner_whitespace(self):
        """Whitespace around the `&` parts is tolerated via per-part strip()."""
        assert parse_aggregation_timeframes(["  >=1h  &  <=4h  "]) == [
            "1h", "2h", "3h", "4h"
        ]


# --------------------------------------------------------------------------- #
# Sort invariant
# --------------------------------------------------------------------------- #


class TestParseAggregationOrdering:
    def test_result_is_strictly_sorted_by_tf_ms(self):
        """For any permutation of input, the result must be tf_ms-ascending."""
        for tokens in (
            ["1D", "1h", "5m", "15m", "1m"],
            ["1m", "1D", "1h", "15m", "5m"],
            ["5m", "1h", "1D", "1m", "15m"],
        ):
            result = parse_aggregation_timeframes(tokens)
            ms_seq = [TIMEFRAMES[tf] for tf in result]
            assert ms_seq == sorted(ms_seq), (
                f"Input {tokens} produced unsorted result {result}"
            )

    def test_full_universe_via_ge_smallest_is_sorted(self):
        """`>=1m` expands to every TF; verify the full sort."""
        result = parse_aggregation_timeframes([">=1m"])
        expected = sorted(TIMEFRAMES.keys(), key=lambda k: TIMEFRAMES[k])
        assert result == expected
