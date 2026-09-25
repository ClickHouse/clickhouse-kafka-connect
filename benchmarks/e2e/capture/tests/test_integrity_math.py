#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Unit tests for the integrity math (20_insert_integrity.sql semantics).

Run: python3 -m pytest benchmarks/e2e/capture/tests/test_integrity_math.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from integrity_math import chaos_verdict, compute  # noqa: E402


# ClickBench hits realistic shape: source has duplicate WatchIDs, so
# rows_expected > unique_expected on a perfectly correct load.
SRC_ROWS = 99_997_497.0
SRC_UNIQ = 88_051_954.0


def test_perfect_load_passes_even_with_source_duplicate_watchids():
    # Target mirrors the source exactly: every source row landed once. The
    # source's own duplicate WatchIDs mean rows != unique on the target too — the
    # WHOLE POINT: this must still be integrity_ok, and duplicate_rows must be 0
    # (NOT rows_delivered - unique_delivered = ~12M).
    r = compute(SRC_ROWS, SRC_ROWS, SRC_UNIQ, SRC_UNIQ)
    assert r.duplicate_rows == 0.0
    assert r.integrity_ok is True


def test_retry_resent_committed_work_fails_on_extra_rows():
    # A retry re-sent a committed batch the server did not dedup: extra rows land
    # but no new unique WatchIDs. duplicate_rows > 0, integrity FAILS.
    delivered = SRC_ROWS + 50_000
    r = compute(delivered, SRC_ROWS, SRC_UNIQ, SRC_UNIQ)
    assert r.duplicate_rows == 50_000.0
    assert r.integrity_ok is False


def test_rows_lost_fails_on_negative_delta():
    # Fewer rows delivered than expected: rows were lost. duplicate_rows < 0,
    # integrity FAILS.
    delivered = SRC_ROWS - 1_000
    r = compute(delivered, SRC_ROWS, SRC_UNIQ - 1_000, SRC_UNIQ)
    assert r.duplicate_rows == -1_000.0
    assert r.integrity_ok is False


def test_row_count_matches_but_unique_short_fails():
    # rows_delivered == rows_expected but unique_delivered < unique_expected
    # (some distinct WatchIDs missing, replaced by dupes of others). Count-only
    # would pass; the AND on uniqExact catches it. integrity FAILS.
    r = compute(SRC_ROWS, SRC_ROWS, SRC_UNIQ - 5, SRC_UNIQ)
    assert r.duplicate_rows == 0.0
    assert r.integrity_ok is False


def test_zero_source_constants_fails():
    # Missing SOURCE_ROWS_EXPECTED / SOURCE_UNIQUE_EXPECTED => 0/0 ground truth.
    # A non-empty target then mismatches and integrity FAILS (never silently OK).
    r = compute(SRC_ROWS, 0.0, SRC_UNIQ, 0.0)
    assert r.integrity_ok is False


# --------------------------------------------------------------------------- #
# chaos_verdict — IC-6 oracle (chaos test #771 §5). Pure, no I/O.
# Verdicts: PASS | MISMATCH | UNVERIFIED_FAULT_NOT_OBSERVED.
# A dup-bearing source (U < N) is used throughout so every fixture would
# FALSE-FAIL under the BANNED count()-uniqExact() family (=N-U=50) and only
# passes under the corrected duplicate_rows = rows_delivered - rows_expected.
# --------------------------------------------------------------------------- #
DUP_N = 1_000.0   # delivered/expected rows (source contains duplicate WatchIDs)
DUP_U = 950.0     # distinct WatchIDs — U < N by construction


def test_chaos_verdict_exact_match_is_pass():
    v = chaos_verdict(DUP_N, DUP_N, DUP_U, DUP_U, dlq_depth=0, fault_observed=1)
    assert v.verdict == "PASS"
    assert v.duplicate_rows == 0.0
    assert v.loss == 0.0


def test_chaos_verdict_dup_bearing_source_passes_under_corrected_formula():
    # The banned count()-uniqExact() on the target would be 1000-950=50 and
    # false-fail this perfect load; the corrected formula yields duplicate_rows=0.
    v = chaos_verdict(DUP_N, DUP_N, DUP_U, DUP_U, dlq_depth=0, fault_observed=1)
    assert v.duplicate_rows == 0.0
    assert v.verdict == "PASS"


def test_chaos_verdict_loss_is_mismatch():
    v = chaos_verdict(DUP_N - 10, DUP_N, DUP_U - 10, DUP_U,
                      dlq_depth=0, fault_observed=1)
    assert v.loss == 10.0
    assert v.verdict == "MISMATCH"
    assert "loss" in v.reason.lower()


def test_chaos_verdict_duplicates_is_mismatch():
    v = chaos_verdict(DUP_N + 10, DUP_N, DUP_U, DUP_U,
                      dlq_depth=0, fault_observed=1)
    assert v.duplicate_rows == 10.0
    assert v.verdict == "MISMATCH"


def test_chaos_verdict_uniqueness_violation_on_dup_bearing_source_is_mismatch():
    # rows match but a distinct WatchID is missing (replaced by a dupe). Count
    # alone would pass; the uniqueness assertion catches it. duplicate_rows=0
    # proves this is NOT the banned family (which would read 55 here).
    v = chaos_verdict(DUP_N, DUP_N, DUP_U - 5, DUP_U,
                      dlq_depth=0, fault_observed=1)
    assert v.duplicate_rows == 0.0
    assert v.verdict == "MISMATCH"
    assert "uniq" in v.reason.lower()


def test_chaos_verdict_dlq_depth_positive_is_mismatch():
    v = chaos_verdict(DUP_N, DUP_N, DUP_U, DUP_U, dlq_depth=3, fault_observed=1)
    assert v.verdict == "MISMATCH"
    assert "dlq" in v.reason.lower()


def test_chaos_verdict_fault_not_observed_is_unverified():
    # Clean integrity but the fault left no observed effect => never PASS.
    v = chaos_verdict(DUP_N, DUP_N, DUP_U, DUP_U, dlq_depth=0, fault_observed=0)
    assert v.verdict == "UNVERIFIED_FAULT_NOT_OBSERVED"


def test_chaos_verdict_mismatch_wins_over_fault_not_observed():
    # A real mismatch is a mismatch regardless of fault observation.
    v = chaos_verdict(DUP_N + 10, DUP_N, DUP_U, DUP_U,
                      dlq_depth=0, fault_observed=0)
    assert v.verdict == "MISMATCH"


def test_chaos_verdict_double_delivery_negative_control_is_mismatch():
    # Negative control: every row delivered twice (no dedup). duplicate_rows=N>0.
    v = chaos_verdict(2 * DUP_N, DUP_N, DUP_U, DUP_U,
                      dlq_depth=0, fault_observed=1)
    assert v.duplicate_rows == DUP_N
    assert v.verdict == "MISMATCH"


# --------------------------------------------------------------------------- #
# The banned count()-uniqExact() family (ExactlyOnceTest.java:255 total-uniqueTotal)
# must be ABSENT from the oracle code — asserted by grepping the source.
# --------------------------------------------------------------------------- #
def _code_without_comments_and_docstrings(src: str) -> str:
    """Return the source with #-comments and triple-quoted docstrings removed,
    so a prose warning that NAMES the banned formula (to forbid it) does not
    trip the guard — only real code and SQL strings are searched."""
    import io
    import tokenize

    kept = []
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.COMMENT:
            continue
        if tok.type == tokenize.STRING and (
                tok.string.startswith(('"""', "'''", 'r"""', "r'''"))):
            continue  # docstring / block comment
        kept.append(tok.string)
    return " ".join(kept)


def test_banned_count_minus_uniqexact_formula_absent_from_new_code():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    banned = re.compile(
        r"count\s*\(\s*\)\s*[-−]\s*uniqExact"      # count() - uniqExact(...)
        r"|uniqExact\s*\([^)]*\)\s*[-−]\s*count"   # uniqExact(...) - count()
        r"|total\s*[-−]\s*uniqueTotal",            # the ExactlyOnceTest form
        re.IGNORECASE,
    )
    for fn in ("integrity_math.py", "check_integrity.py"):
        with open(os.path.join(base, fn), encoding="utf-8") as fh:
            code = _code_without_comments_and_docstrings(fh.read())
        assert not banned.search(code), f"BANNED count()-uniqExact() family in {fn}"


if __name__ == "__main__":
    sys.exit(__import__("pytest").main([__file__, "-v"]))
