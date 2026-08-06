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
"""Pure-Python mirror of the integrity math in 20_insert_integrity.sql.

The verdict is computed server-side in SQL at capture time; this module exists so
the semantics (contract §2.1 + §3, principal directive on duplicate WatchIDs) can
be unit-tested WITHOUT a live ClickHouse. It MUST stay in lock-step with the SQL:

  duplicate_rows = rows_delivered - rows_expected            (target-vs-SOURCE delta)
  integrity_ok   = (rows_delivered == rows_expected)
                   AND (unique_delivered == unique_expected)

CRITICAL: duplicate_rows is NOT count()-uniqExact() on the target — the ClickBench
hits SOURCE legitimately contains duplicate WatchIDs, so that formula
false-positives every run.
"""
from dataclasses import dataclass


@dataclass
class IntegrityResult:
    rows_delivered: float
    rows_expected: float
    unique_delivered: float
    unique_expected: float
    duplicate_rows: float
    integrity_ok: bool


def compute(rows_delivered: float, rows_expected: float,
            unique_delivered: float, unique_expected: float) -> IntegrityResult:
    duplicate_rows = rows_delivered - rows_expected
    integrity_ok = (rows_delivered == rows_expected
                    and unique_delivered == unique_expected)
    return IntegrityResult(
        rows_delivered=rows_delivered,
        rows_expected=rows_expected,
        unique_delivered=unique_delivered,
        unique_expected=unique_expected,
        duplicate_rows=duplicate_rows,
        integrity_ok=integrity_ok,
    )


# --------------------------------------------------------------------------- #
# Chaos test (#771) oracle verdict — IC-6, spec §5. Pure, no I/O.
# --------------------------------------------------------------------------- #
PASS = "PASS"
MISMATCH = "MISMATCH"
UNVERIFIED_FAULT_NOT_OBSERVED = "UNVERIFIED_FAULT_NOT_OBSERVED"


@dataclass
class ChaosVerdict:
    """The IC-6 result object. Field order == the emitted JSON key order."""
    rows_delivered: float
    rows_expected: float
    unique_delivered: float
    unique_expected: float
    duplicate_rows: float
    loss: float
    dlq_depth: float
    verdict: str
    reason: str


def chaos_verdict(rows_delivered: float, rows_expected: float,
                  unique_delivered: float, unique_expected: float,
                  dlq_depth: float, fault_observed) -> ChaosVerdict:
    """Classify one settled chaos run into PASS | MISMATCH |
    UNVERIFIED_FAULT_NOT_OBSERVED.

    Formula law (spec §5, inherited verbatim; the count()-uniqExact() family on
    the TARGET is BANNED — it false-positives on the dup-bearing source):

        duplicate_rows = rows_delivered - rows_expected     # target-vs-SOURCE
        loss           = rows_expected  - rows_delivered

    MISMATCH iff any of: loss (rows_delivered < rows_expected), duplicate_rows
    != 0 (rows_delivered > rows_expected), a uniqueness violation
    (unique_delivered != unique_expected), or dlq_depth > 0 (§3.6b — records
    landed nowhere). A MISMATCH is a MISMATCH regardless of fault observation.
    Otherwise, clean integrity with NO observed fault effect (fault_observed
    falsey) is UNVERIFIED_FAULT_NOT_OBSERVED (spec §5 fault-took-effect rule,
    never a silent PASS); only clean integrity WITH an observed fault is PASS.
    """
    duplicate_rows = rows_delivered - rows_expected
    loss = rows_expected - rows_delivered

    reasons = []
    if rows_delivered < rows_expected:
        reasons.append(
            f"loss={loss} (rows_delivered {rows_delivered} < "
            f"rows_expected {rows_expected})")
    elif rows_delivered > rows_expected:
        reasons.append(
            f"duplicate_rows={duplicate_rows} (rows_delivered "
            f"{rows_delivered} > rows_expected {rows_expected}; must be 0)")
    if unique_delivered != unique_expected:
        reasons.append(
            f"uniqueness violation (unique_delivered {unique_delivered} != "
            f"unique_expected {unique_expected})")
    if dlq_depth > 0:
        reasons.append(f"dlq_depth={dlq_depth} (must be 0)")

    if reasons:
        verdict = MISMATCH
        reason = "; ".join(reasons)
    elif not fault_observed:
        verdict = UNVERIFIED_FAULT_NOT_OBSERVED
        reason = ("integrity clean but no fault effect was observed "
                  "(fault_observed=0) — cannot be attributed to a survived fault")
    else:
        verdict = PASS
        reason = "zero loss, zero duplication, uniqueness intact, DLQ empty"

    return ChaosVerdict(
        rows_delivered=rows_delivered,
        rows_expected=rows_expected,
        unique_delivered=unique_delivered,
        unique_expected=unique_expected,
        duplicate_rows=duplicate_rows,
        loss=loss,
        dlq_depth=dlq_depth,
        verdict=verdict,
        reason=reason,
    )
