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
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from integrity_math import compute  # noqa: E402


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


if __name__ == "__main__":
    sys.exit(__import__("pytest").main([__file__, "-v"]))
