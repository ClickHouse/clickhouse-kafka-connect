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


# =========================================================================== #
# --direct mode (chaos test #771, IC-6): read the self-hosted target directly
# (count(), uniqExact(WatchID) SETTINGS select_sequential_consistency=1),
# reuse the retry/backoff envelope, emit the IC-6 JSON, preserve exit 0/1/3.
# A transient undercount that resolves on re-read => CHECK_ERROR, never MISMATCH.
# The perf.metrics read-back mode (main()) and its tests above are UNTOUCHED.
# =========================================================================== #
def _load_check_integrity_direct(monkeypatch, *, reads=None,
                                  raise_client=False, raise_query=False):
    """Import check_integrity with a stubbed ch_common for --direct mode.

    reads: list of (count, uniq) tuples returned by SUCCESSIVE direct reads
    (one per get_client acquisition, i.e. one per _read_target_direct call);
    the last entry is reused if more reads occur than tuples supplied.
    """
    reads = list(reads) if reads is not None else [(10.0, 10.0)]
    state = {"client_calls": 0, "read_idx": 0}

    fake = types.ModuleType("ch_common")
    fake.require = lambda name: os.environ.get(name, "")

    class _OperationalError(Exception):
        pass

    fake.OperationalError = _OperationalError

    def _get_client(*a, **k):
        state["client_calls"] += 1
        state["last_client_kwargs"] = dict(k)
        if raise_client:
            raise _OperationalError("connect stall on SELECT version()")
        if raise_query:
            return _FakeClient(raise_exc=_OperationalError("read timeout"))
        idx = min(state["read_idx"], len(reads) - 1)
        state["read_idx"] += 1
        return _FakeClient(rows=[reads[idx]])  # result_rows == [(count, uniq)]

    fake.get_client = _get_client
    monkeypatch.setitem(sys.modules, "ch_common", fake)

    import check_integrity
    importlib.reload(check_integrity)
    check_integrity._STATE = state
    return check_integrity


def _run_direct(mod):
    with pytest.raises(SystemExit) as ei:
        mod.main_direct()
    code = ei.value.code
    return 0 if code is None else code


def _set_direct_env(monkeypatch, rows_expected, unique_expected,
                    dlq_depth=0, fault_observed=1):
    monkeypatch.setenv("TARGET_CH_HOST", "ch-chaos.kafka-bench.svc")
    monkeypatch.setenv("TARGET_CH_USER", "default")
    monkeypatch.setenv("TARGET_CH_PASSWORD", "")
    monkeypatch.setenv("CH_DATABASE", "clickbench")
    monkeypatch.setenv("CH_TABLE", "hits_chaos")
    monkeypatch.setenv("ROWS_EXPECTED", str(rows_expected))
    monkeypatch.setenv("SOURCE_UNIQUE_EXPECTED", str(unique_expected))
    monkeypatch.setenv("DLQ_DEPTH", str(dlq_depth))
    monkeypatch.setenv("FAULT_OBSERVED", str(fault_observed))


# dup-bearing source everywhere: N=1000 rows, U=950 distinct WatchIDs. The
# banned count()-uniqExact()=50 would false-fail a perfect load.
_N, _U = 1000.0, 950.0


def test_direct_exact_match_is_exit_0(monkeypatch, capsys):
    _set_direct_env(monkeypatch, _N, _U)
    mod = _load_check_integrity_direct(monkeypatch, reads=[(_N, _U)])
    assert _run_direct(mod) == 0
    out = capsys.readouterr().out
    assert '"verdict": "PASS"' in out
    assert '"duplicate_rows": 0.0' in out  # NOT 50 -> corrected formula


def test_direct_loss_is_exit_1(monkeypatch, capsys):
    _set_direct_env(monkeypatch, _N, _U)
    # STABLE undercount across both reads => real loss, not a lagging replica.
    mod = _load_check_integrity_direct(
        monkeypatch, reads=[(990.0, 945.0), (990.0, 945.0)])
    assert _run_direct(mod) == 1
    assert '"verdict": "MISMATCH"' in capsys.readouterr().out


def test_direct_duplicates_is_exit_1(monkeypatch, capsys):
    _set_direct_env(monkeypatch, _N, _U)
    mod = _load_check_integrity_direct(monkeypatch, reads=[(1050.0, _U)])
    assert _run_direct(mod) == 1
    out = capsys.readouterr().out
    assert '"verdict": "MISMATCH"' in out
    assert '"duplicate_rows": 50.0' in out


def test_direct_uniqueness_violation_on_dup_bearing_source_is_exit_1(monkeypatch, capsys):
    # rows match but a distinct WatchID missing. duplicate_rows=0 proves the
    # corrected formula: the banned one would read 55 here and mis-attribute.
    _set_direct_env(monkeypatch, _N, _U)
    mod = _load_check_integrity_direct(monkeypatch, reads=[(_N, 945.0)])
    assert _run_direct(mod) == 1
    out = capsys.readouterr().out
    assert '"verdict": "MISMATCH"' in out
    assert '"duplicate_rows": 0.0' in out


def test_direct_dlq_depth_positive_is_exit_1(monkeypatch, capsys):
    _set_direct_env(monkeypatch, _N, _U, dlq_depth=4)
    mod = _load_check_integrity_direct(monkeypatch, reads=[(_N, _U)])
    assert _run_direct(mod) == 1
    out = capsys.readouterr().out
    assert '"verdict": "MISMATCH"' in out
    assert "dlq" in out.lower()


def test_direct_fault_not_observed_is_exit_3(monkeypatch, capsys):
    # Clean integrity but no observed fault effect => unverified, never PASS.
    _set_direct_env(monkeypatch, _N, _U, fault_observed=0)
    mod = _load_check_integrity_direct(monkeypatch, reads=[(_N, _U)])
    assert _run_direct(mod) == 3
    assert '"verdict": "UNVERIFIED_FAULT_NOT_OBSERVED"' in capsys.readouterr().out


def test_direct_client_raising_is_check_error_exit_3(monkeypatch, capsys):
    _set_direct_env(monkeypatch, _N, _U)
    mod = _load_check_integrity_direct(monkeypatch, raise_client=True)
    assert _run_direct(mod) == 3
    err = capsys.readouterr().err
    assert "CHECK_ERROR" in err
    assert mod._STATE["client_calls"] == 3  # retried before conceding


def test_direct_query_raising_is_check_error_exit_3(monkeypatch, capsys):
    _set_direct_env(monkeypatch, _N, _U)
    mod = _load_check_integrity_direct(monkeypatch, raise_query=True)
    assert _run_direct(mod) == 3
    assert "CHECK_ERROR" in capsys.readouterr().err


def test_direct_undercount_then_correct_is_check_error_not_mismatch(monkeypatch, capsys):
    # First read lags (990 < 1000 expected); the re-read resolves to the full
    # count (1000). An unstable count is a lagging replica => CHECK_ERROR (3),
    # NEVER MISMATCH (1). This is the replica-consistency rule (§5).
    _set_direct_env(monkeypatch, _N, _U)
    mod = _load_check_integrity_direct(
        monkeypatch, reads=[(990.0, 945.0), (_N, _U)])
    assert _run_direct(mod) == 3  # explicitly NOT 1
    assert "CHECK_ERROR" in capsys.readouterr().err


def test_direct_double_delivery_negative_control_is_exit_1(monkeypatch, capsys):
    # Every row delivered twice (no dedup) => duplicate_rows = N > 0 => MISMATCH.
    _set_direct_env(monkeypatch, _N, _U)
    mod = _load_check_integrity_direct(monkeypatch, reads=[(2 * _N, _U)])
    assert _run_direct(mod) == 1
    out = capsys.readouterr().out
    assert '"verdict": "MISMATCH"' in out
    assert '"duplicate_rows": 1000.0' in out


# --------------------------------------------------------------------------- #
# #771 wiring: the --direct oracle must reach the in-cluster self-hosted CH
# (plaintext 8123, no TLS — IC-2), NOT the pair's Cloud 8443/TLS default that
# ch_common.get_client bakes in. chaos_run.sh exports TARGET_CH_PORT=8123 +
# TARGET_CH_SECURE=false; the direct read MUST thread them into get_client. The
# stubbed ch_common records the kwargs it was called with.
# --------------------------------------------------------------------------- #
def test_direct_threads_self_hosted_port_and_no_tls(monkeypatch):
    _set_direct_env(monkeypatch, _N, _U)
    monkeypatch.setenv("TARGET_CH_PORT", "8123")
    monkeypatch.setenv("TARGET_CH_SECURE", "false")
    mod = _load_check_integrity_direct(monkeypatch, reads=[(_N, _U)])
    assert _run_direct(mod) == 0
    kw = mod._STATE["last_client_kwargs"]
    assert kw["port"] == 8123, "oracle must use the in-cluster port 8123, not 8443"
    assert kw["secure"] is False, "oracle must not use TLS against in-cluster CH"


def test_direct_defaults_to_cloud_port_and_tls_when_unset(monkeypatch):
    # An unset TARGET_CH_PORT / TARGET_CH_SECURE keeps get_client's pair default
    # (8443 + TLS) — the change is additive and cannot regress the pair.
    _set_direct_env(monkeypatch, _N, _U)
    monkeypatch.delenv("TARGET_CH_PORT", raising=False)
    monkeypatch.delenv("TARGET_CH_SECURE", raising=False)
    mod = _load_check_integrity_direct(monkeypatch, reads=[(_N, _U)])
    assert _run_direct(mod) == 0
    kw = mod._STATE["last_client_kwargs"]
    assert kw["port"] == 8443
    assert kw["secure"] is True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
