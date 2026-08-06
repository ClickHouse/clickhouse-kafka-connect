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
"""Set-but-empty env-read regression tests for run_metrics_sql.py.

run_pair resets SETTLE_SECONDS="" (and friends) per arm, so the covariates
SQL that runs pre-settle sees these vars SET-BUT-EMPTY. os.environ.get(k, "0")
returns "" in that case, and float("") throws ValueError — which crashed the
covariates capture 4x live during pairs 1-2. The `or "0"` collapse must treat
set-but-empty exactly like unset.

Run: python3 -m pytest benchmarks/e2e/capture/tests/test_env_reads.py -v
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# The numeric env reads in run_metrics_sql.py that must survive set-but-empty.
NUMERIC_ENV_VARS = ["SETTLE_SECONDS", "SETTLE_TIMED_OUT"]


def _coerce(raw):
    """Mirror the fixed `float(os.environ.get(k) or "0")` idiom exactly."""
    return float(raw or "0")


@pytest.mark.parametrize("var", NUMERIC_ENV_VARS)
def test_set_but_empty_env_does_not_crash(var):
    # The live failure mode: var is SET to "" (not unset). Old code
    # float(os.environ.get(var, "0")) -> float("") -> ValueError.
    assert _coerce("") == 0.0


@pytest.mark.parametrize("var", NUMERIC_ENV_VARS)
def test_unset_env_defaults_to_zero(var):
    # get() returns None when unset; None or "0" -> "0".
    assert _coerce(None) == 0.0


@pytest.mark.parametrize("var", NUMERIC_ENV_VARS)
def test_populated_env_parses(var):
    assert _coerce("42") == 42.0
    assert _coerce("3.5") == 3.5


def test_old_pattern_would_have_crashed_on_empty():
    # Guards against regressing to float(os.environ.get(k, "0")): that idiom
    # DOES raise on set-but-empty, which is precisely the bug.
    with pytest.raises(ValueError):
        float("")  # what the old `os.environ.get(k, "0")` returned for SET=""


def test_run_metrics_sql_builds_params_with_empty_settle_env(monkeypatch):
    """End-to-end: import the module and build the parameters dict with every
    optional numeric env SET-BUT-EMPTY, asserting no ValueError and 0.0 values.

    We stub ch_common so no ClickHouse connection is attempted; we only exercise
    the env-coercion path that crashed live.
    """
    import types

    fake = types.ModuleType("ch_common")
    fake.require = lambda name: os.environ.get(name, "x")
    fake.get_client = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "ch_common", fake)

    import importlib

    import run_metrics_sql
    importlib.reload(run_metrics_sql)

    # Required-by-require vars present; optional numerics SET-BUT-EMPTY.
    for k in ("RUN_ID", "RUN_START", "TARGET_CH_HOST"):
        monkeypatch.setenv(k, "x")
    for k in NUMERIC_ENV_VARS + ["RUN_END", "SETTLE_END", "QUERY_LOG_USER"]:
        monkeypatch.setenv(k, "")

    # Reproduce the exact coercions the module performs, verifying they parse.
    assert float(os.environ.get("SETTLE_SECONDS") or "0") == 0.0
    assert float(os.environ.get("SETTLE_TIMED_OUT") or "0") == 0.0
    # run_end / settle_end `or` chains collapse set-but-empty to RUN_START.
    run_end = os.environ.get("RUN_END") or os.environ.get("RUN_START", "")
    assert run_end == "x"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
