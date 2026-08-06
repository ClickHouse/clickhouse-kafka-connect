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
