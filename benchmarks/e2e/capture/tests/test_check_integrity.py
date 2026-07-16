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
"""Exit-code semantics for check_integrity.py (pair-4 crash-class fix).

The pair-4 incident: check_integrity.py CRASHED on a transient clickhouse_connect
read-timeout during the connection-handshake SELECT, run_pair.sh read that as a
TIER 1 INTEGRITY MISMATCH, and a PERFECT run went false-red. The fix gives the
checker three DISTINCT exit codes so an infra hiccup can never masquerade as a
data mismatch:

  0  ran, verdict OK
  1  RAN and MISMATCHED (the only code that may fail a run)
  3  CHECK_ERROR (any infra/connection/query exception — could not verify),
     conceded only AFTER retries with backoff.

These tests stub ch_common.get_client with a fake client (no ClickHouse, no
network) and drive check_integrity.main() through each path, asserting the exit
CODE and the stderr wording.

Run: python3 -m pytest benchmarks/e2e/capture/tests/test_check_integrity.py -v
"""
import importlib
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class _FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class _FakeClient:
    """Returns a preset rows list from .query(); or raises a preset exception."""

    def __init__(self, rows=None, raise_exc=None):
        self._rows = rows or []
        self._raise = raise_exc
        self.query_calls = 0

    def query(self, *a, **k):
        self.query_calls += 1
        if self._raise is not None:
            raise self._raise
        return _FakeResult(self._rows)


def _load_check_integrity(monkeypatch, *, rows=None, raise_on=None,
                          fail_first_n_clients=0):
    """Import check_integrity with a stubbed ch_common.

    fail_first_n_clients: raise OperationalError-like exception from the FIRST n
    get_client() acquisitions, then hand back a working client (models a
    transient staging stall that clears on retry).
    """
    state = {"client_calls": 0}

    fake = types.ModuleType("ch_common")
    fake.require = lambda name: os.environ.get(name, "run-x")

    class _OperationalError(Exception):
        pass

    fake.OperationalError = _OperationalError

    def _get_client(*a, **k):
        state["client_calls"] += 1
        if state["client_calls"] <= fail_first_n_clients:
            raise _OperationalError("read timeout on SELECT version()")
        if raise_on == "query":
            return _FakeClient(raise_exc=_OperationalError("read timeout"))
        return _FakeClient(rows=rows)

    fake.get_client = _get_client
    monkeypatch.setitem(sys.modules, "ch_common", fake)

    import check_integrity
    importlib.reload(check_integrity)
    check_integrity._STATE = state  # expose for assertions
    return check_integrity


def _run_main(mod):
    """Run main(); return (exit_code)."""
    with pytest.raises(SystemExit) as ei:
        mod.main()
    code = ei.value.code
    return 0 if code is None else code


@pytest.fixture(autouse=True)
def _fast_backoff(monkeypatch):
    # Never actually sleep during retry tests.
    monkeypatch.setenv("CH_INTEGRITY_BACKOFFS", "0,0,0")
    monkeypatch.setenv("RUN_ID", "run-x")


# --------------------------------------------------------------------------- #
# exit 0: ran, verdict OK
# --------------------------------------------------------------------------- #
def test_exit_0_when_integrity_ok(monkeypatch, capsys):
    rows = [("integrity_ok", 1.0), ("rows_delivered", 10.0),
            ("rows_expected", 10.0), ("duplicate_rows", 0.0)]
    mod = _load_check_integrity(monkeypatch, rows=rows)
    assert _run_main(mod) == 0
    assert "integrity OK" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# exit 1: RAN and MISMATCHED — the ONLY code that may fail a run
# --------------------------------------------------------------------------- #
def test_exit_1_when_integrity_mismatch(monkeypatch, capsys):
    rows = [("integrity_ok", 0.0), ("rows_delivered", 9.0),
            ("rows_expected", 10.0), ("duplicate_rows", 1.0)]
    mod = _load_check_integrity(monkeypatch, rows=rows)
    assert _run_main(mod) == 1
    assert "MISMATCH" in capsys.readouterr().err


def test_exit_1_when_metrics_missing(monkeypatch, capsys):
    # No integrity_ok row at all -> RAN, but read an empty verdict -> mismatch.
    mod = _load_check_integrity(monkeypatch, rows=[])
    assert _run_main(mod) == 1
    err = capsys.readouterr().err
    assert "MISMATCH" in err and "no integrity metrics" in err


# --------------------------------------------------------------------------- #
# exit 3: CHECK_ERROR — infra/connection/query failure, could not verify.
# This is the exact pair-4 crash class; it must NEVER be exit 1.
# --------------------------------------------------------------------------- #
def test_exit_3_when_client_never_connects(monkeypatch, capsys):
    # Every attempt's get_client raises (persistent stall) -> concede exit 3.
    mod = _load_check_integrity(monkeypatch, fail_first_n_clients=99)
    assert _run_main(mod) == 3
    err = capsys.readouterr().err
    assert "CHECK_ERROR" in err
    assert "NOT a data mismatch" in err
    # It must have RETRIED (default 3 attempts) before conceding.
    assert mod._STATE["client_calls"] == 3


def test_exit_3_when_query_raises(monkeypatch, capsys):
    mod = _load_check_integrity(monkeypatch, raise_on="query", rows=[])
    assert _run_main(mod) == 3
    assert "CHECK_ERROR" in capsys.readouterr().err


def test_check_error_is_not_exit_1(monkeypatch):
    # Regression guard for the pair-4 root cause: an infra failure must never
    # exit 1 (which would fail a perfect run).
    mod = _load_check_integrity(monkeypatch, fail_first_n_clients=99)
    assert _run_main(mod) == 3  # explicitly NOT 1


# --------------------------------------------------------------------------- #
# retry-then-succeed: a transient stall that clears mid-retry ends OK (exit 0),
# proving the retry path actually recovers rather than just deferring failure.
# --------------------------------------------------------------------------- #
def test_transient_then_ok_recovers_to_exit_0(monkeypatch, capsys):
    rows = [("integrity_ok", 1.0), ("rows_delivered", 10.0),
            ("rows_expected", 10.0), ("duplicate_rows", 0.0)]
    # First acquisition raises; second succeeds and reads a clean verdict.
    mod = _load_check_integrity(monkeypatch, rows=rows, fail_first_n_clients=1)
    assert _run_main(mod) == 0
    out = capsys.readouterr()
    assert "integrity OK" in out.out
    assert "retrying" in out.err            # it DID retry
    assert mod._STATE["client_calls"] == 2  # failed once, then succeeded


def test_attempts_env_override(monkeypatch):
    monkeypatch.setenv("CH_INTEGRITY_ATTEMPTS", "5")
    mod = _load_check_integrity(monkeypatch, fail_first_n_clients=99)
    assert _run_main(mod) == 3
    assert mod._STATE["client_calls"] == 5  # honored the override


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
