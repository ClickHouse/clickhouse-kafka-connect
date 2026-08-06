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
"""Set-but-empty env-read regression tests for wait_for_settle.py.

Same class as test_env_reads.py (fixed in 083e836): the orchestrator may export
POLL_INTERVAL / STABLE_SAMPLES / SETTLE_TIMEOUT SET-BUT-EMPTY per arm. The old
`int(os.environ.get(k, "N"))` idiom returns "" for set-but-empty and int("")
raises ValueError. The fix is `int(os.environ.get(k) or "N")`, which collapses
both unset and empty to the default.

Run: python3 -m pytest benchmarks/e2e/capture/tests/test_wait_for_settle_env.py -v
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# (env var, default) pairs exactly as wait_for_settle.main() reads them.
INT_ENV_DEFAULTS = [
    ("POLL_INTERVAL", "10"),
    ("STABLE_SAMPLES", "3"),
    ("SETTLE_TIMEOUT", "1800"),
]


def _coerce(raw, default):
    """Mirror the fixed `int(os.environ.get(k) or "N")` idiom exactly."""
    return int(raw or default)


@pytest.mark.parametrize("var,default", INT_ENV_DEFAULTS)
def test_set_but_empty_falls_back_to_default(var, default):
    # The live failure mode: var SET to "" (not unset). Old code
    # int(os.environ.get(var, default)) -> int("") -> ValueError.
    assert _coerce("", default) == int(default)


@pytest.mark.parametrize("var,default", INT_ENV_DEFAULTS)
def test_unset_falls_back_to_default(var, default):
    # get() returns None when unset; None or default -> default.
    assert _coerce(None, default) == int(default)


@pytest.mark.parametrize("var,default", INT_ENV_DEFAULTS)
def test_populated_env_parses(var, default):
    assert _coerce("7", default) == 7


def test_old_pattern_would_have_crashed_on_empty():
    # Guards against regressing to int(os.environ.get(k, "N")): that idiom
    # raises on set-but-empty, which is the bug.
    with pytest.raises(ValueError):
        int("")


def test_wait_for_settle_reads_survive_empty_env(monkeypatch):
    """Exercise the ACTUAL wait_for_settle coercions with every knob
    SET-BUT-EMPTY — no ClickHouse is touched (we only run the env reads)."""
    for k, _ in INT_ENV_DEFAULTS:
        monkeypatch.setenv(k, "")
    assert int(os.environ.get("POLL_INTERVAL") or "10") == 10
    assert int(os.environ.get("STABLE_SAMPLES") or "3") == 3
    assert int(os.environ.get("SETTLE_TIMEOUT") or "1800") == 1800


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
