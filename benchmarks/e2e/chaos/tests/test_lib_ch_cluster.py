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
"""Offline unit tests for the CH-cluster lifecycle library (task T8 / spec §2, §8).

No cluster, no creds, no network. A PATH-stubbed `kubectl` (and `envsubst`)
records every invocation and returns canned 4lw `mntr`/`ruok` and status output,
driven by STUB_* env vars, so every lifecycle function can be exercised offline.

Covered (fail-then-pass on every error path):
  * lib_ch_cluster.sh sources cleanly under `set -u`, syntax-clean, sources
    lib_bench.sh (inherits log/warn/die), defines exactly the T8 function set,
    and sourcing executes NO phase (zero kubectl/envsubst invocations)
  * deploy_ch_cluster waits for BOTH 3/3 readiness AND keeper quorum (1 leader +
    2 followers) before returning; readiness gates the quorum probe; IC-8 renders
    CHAOS_CH_IMAGE from CHAOS_CH_VERSION
  * teardown_ch_cluster deletes STS/Services/ConfigMap/PVCs and fails loud if the
    STS survives (§4 rule 4 / §9.8)
  * resolve_keeper_leader echoes the `leader` pod from mixed mntr outputs and
    errors on 0 or 2 leaders (C5's target; the T9 interface)
  * keeper_map_reset drops the exactly-once KeeperMap `zkDatabase` table (§2.4)
  * keeper_map_smoke CREATE/DROPs a KeeperMap table (acceptance §9.2 probe)
  * apply_chaos_ddl applies the hits_chaos DDL from the ConfigMap on each replica
  * await_replica_sync drains the system.replicas queue (SYSTEM SYNC REPLICA)
  * read_ch_image_digest reads back .status.containerStatuses[0].imageID (IC-8)
"""
import os
import subprocess
import sys
from pathlib import Path

import pytest

# tests -> chaos
CHAOS_DIR = Path(__file__).resolve().parents[1]
LIB = CHAOS_DIR / "lib_ch_cluster.sh"

# The T8 function set (spec §8) — this list IS the interface T9/T11 consume.
T8_FUNCTIONS = [
    "deploy_ch_cluster",
    "teardown_ch_cluster",
    "resolve_keeper_leader",
    "keeper_map_reset",
    "keeper_map_smoke",
    "apply_chaos_ddl",
    "await_replica_sync",
    "read_ch_image_digest",
]

# A single parametric kubectl stub. It records every call to $KUBECTL_CALLS and
# returns canned output selected by the args + STUB_* env vars, so the lifecycle
# functions run entirely offline.
KUBECTL_STUB = r'''#!/usr/bin/env bash
echo "kubectl $*" >> "$KUBECTL_CALLS"
args="$*"
pod_idx=""
for i in 0 1 2; do
  case "$args" in *"ch-chaos-$i"*) pod_idx="$i" ;; esac
done
case "$args" in
  *"get statefulset"*readyReplicas*)
    printf '%s' "${STUB_READY_REPLICAS:-3}"; exit 0 ;;
  *"get statefulset"*"-o name"*)
    printf '%s' "${STUB_STS_AFTER_DELETE:-}"; exit 0 ;;
  *"get pvc"*"-o name"*)
    printf '%s' "${STUB_PVC_AFTER_DELETE:-}"; exit 0 ;;
  *exec*ruok*)
    printf '%s' "${STUB_RUOK:-imok}"; exit 0 ;;
  *exec*mntr*)
    IFS=',' read -r -a _st <<< "${STUB_KEEPER_STATES:-leader,follower,follower}"
    idx="${pod_idx:-0}"
    state="${_st[$idx]:-}"
    [ -n "$state" ] && printf 'zk_server_state\t%s\n' "$state"
    exit 0 ;;
  *"get configmap"*hits_chaos*)
    printf '%s' "${STUB_DDL-CREATE DATABASE IF NOT EXISTS clickbench; CREATE TABLE IF NOT EXISTS clickbench.hits_chaos (WatchID BIGINT) ENGINE=ReplicatedMergeTree;}"
    exit 0 ;;
  *system.replicas*)
    printf '%s' "${STUB_REPLICA_QUEUE:-0}"; exit "${STUB_CH_QUERY_RC:-0}" ;;
  *"SYSTEM SYNC REPLICA"*)
    exit "${STUB_CH_QUERY_RC:-0}" ;;
  *"clickhouse-client -n"*)
    # piped multi-query DDL apply (exec -i): drain stdin like the real client.
    cat >/dev/null 2>&1; exit "${STUB_CH_QUERY_RC:-0}" ;;
  *clickhouse-client*)
    printf '%s' "${STUB_CH_QUERY_OUT:-}"; exit "${STUB_CH_QUERY_RC:-0}" ;;
  *containerStatuses*)
    printf '%s' "${STUB_IMAGE_ID-clickhouse/clickhouse-server@sha256:abc}"; exit 0 ;;
  *apply*)
    # `kubectl apply -f -` reads the manifest on stdin: drain it so the upstream
    # `envsubst` in the pipe never takes SIGPIPE (which pipefail would surface).
    cat >/dev/null 2>&1; exit "${STUB_APPLY_RC:-0}" ;;
  *delete*)
    exit 0 ;;
  *)
    exit 0 ;;
esac
'''

# envsubst stub: record the rendered CHAOS_CH_IMAGE (IC-8), pass stdin through.
ENVSUBST_STUB = r'''#!/usr/bin/env bash
echo "CHAOS_CH_IMAGE=${CHAOS_CH_IMAGE:-}" >> "$ENVSUBST_MARKER"
cat
'''


def _make_bin(tmp_path):
    """Create the PATH stub dir; return (bindir, calls_file, envsubst_marker)."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    (bindir / "kubectl").write_text(KUBECTL_STUB)
    (bindir / "kubectl").chmod(0o755)
    (bindir / "envsubst").write_text(ENVSUBST_STUB)
    (bindir / "envsubst").chmod(0o755)
    calls = tmp_path / "kubectl.calls"
    marker = tmp_path / "envsubst.marker"
    return bindir, calls, marker


def _run(tmp_path, snippet, extra_env=None):
    """Source lib_ch_cluster.sh in a PATH-stubbed shell and run `snippet`.

    Returns (CompletedProcess, calls_text)."""
    bindir, calls, marker = _make_bin(tmp_path)
    env = dict(os.environ)
    env["PATH"] = f"{bindir}:{env['PATH']}"
    env["KUBECTL_CALLS"] = str(calls)
    env["ENVSUBST_MARKER"] = str(marker)
    # Fast timeouts so the fail paths do not burn wall-clock.
    env.setdefault("CH_POLL_INTERVAL", "1")
    env.setdefault("CH_READY_TIMEOUT", "2")
    env.setdefault("CH_QUORUM_TIMEOUT", "2")
    env.setdefault("CH_REPLICA_SYNC_TIMEOUT", "2")
    env["BENCH_LOG_PREFIX"] = "chaos_run"
    if extra_env:
        env.update(extra_env)
    r = subprocess.run(
        ["bash", "-c", f'set -uo pipefail; source "{LIB}"; {snippet}'],
        capture_output=True, text=True, env=env)
    calls_text = calls.read_text() if calls.exists() else ""
    return r, calls_text


def _marker(tmp_path):
    m = tmp_path / "envsubst.marker"
    return m.read_text() if m.exists() else ""


# --------------------------------------------------------------------------- #
# sourcing / structure
# --------------------------------------------------------------------------- #
def test_lib_exists():
    assert LIB.is_file(), "lib_ch_cluster.sh must exist"


def test_lib_syntax_ok():
    r = subprocess.run(["bash", "-n", str(LIB)], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_lib_sources_cleanly_under_set_u():
    r = subprocess.run(
        ["bash", "-c", f'set -u; source "{LIB}"; echo OK'],
        capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip().endswith("OK")


def test_lib_sources_lib_bench():
    """lib_ch_cluster.sh sources lib_bench.sh, so log/warn/die and the shared
    broker helper are available (spec §8: 'sources lib_bench.sh')."""
    r = subprocess.run(
        ["bash", "-c",
         f'set -u; source "{LIB}"; '
         f'declare -F log die warn broker_topic_row_count >/dev/null && echo OK'],
        capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    assert "OK" in r.stdout


@pytest.mark.parametrize("fn", T8_FUNCTIONS)
def test_lib_defines_t8_function(fn):
    r = subprocess.run(
        ["bash", "-c", f'set -u; source "{LIB}" >/dev/null 2>&1; declare -F'],
        capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    declared = {ln.split()[-1] for ln in r.stdout.splitlines() if ln.strip()}
    assert fn in declared, f"lib_ch_cluster.sh must define T8 function {fn}"


def test_sourcing_executes_no_phase(tmp_path):
    """Sourcing must have NO side effect beyond variable defaults (no phase run):
    the PATH-stubbed kubectl/envsubst must record zero invocations."""
    r, calls = _run(tmp_path, "echo SOURCED")
    assert r.returncode == 0, r.stderr
    assert "SOURCED" in r.stdout
    assert calls == "", f"sourcing executed kubectl:\n{calls}"
    assert _marker(tmp_path) == "", "sourcing executed envsubst"


# --------------------------------------------------------------------------- #
# deploy_ch_cluster — waits for BOTH readiness and quorum
# --------------------------------------------------------------------------- #
def test_deploy_waits_for_readiness_and_quorum(tmp_path):
    r, calls = _run(tmp_path, "deploy_ch_cluster", extra_env={
        "STUB_READY_REPLICAS": "3",
        "STUB_KEEPER_STATES": "leader,follower,follower",
    })
    assert r.returncode == 0, r.stderr
    # both signals were probed before returning
    assert "readyReplicas" in calls, "deploy must probe readiness"
    assert "mntr" in calls, "deploy must probe keeper quorum (4lw mntr)"


def test_deploy_readiness_gates_quorum(tmp_path):
    """If readiness never reaches 3/3, deploy dies WITHOUT probing quorum —
    readiness is the first gate."""
    r, calls = _run(tmp_path, "deploy_ch_cluster", extra_env={
        "STUB_READY_REPLICAS": "2",
        "STUB_KEEPER_STATES": "leader,follower,follower",
    })
    assert r.returncode != 0, "deploy must fail when pods never become Ready"
    assert "readyReplicas" in calls
    assert "mntr" not in calls, "quorum must not be probed until readiness holds"


def test_deploy_fails_when_quorum_never_forms(tmp_path):
    """Readiness holds but no leader emerges (all followers) -> deploy dies at
    the quorum gate (having probed both)."""
    r, calls = _run(tmp_path, "deploy_ch_cluster", extra_env={
        "STUB_READY_REPLICAS": "3",
        "STUB_KEEPER_STATES": "follower,follower,follower",
    })
    assert r.returncode != 0, "deploy must fail when no keeper quorum forms"
    assert "readyReplicas" in calls
    assert "mntr" in calls


def test_deploy_fails_on_apply_error(tmp_path):
    r, _ = _run(tmp_path, "deploy_ch_cluster", extra_env={
        "STUB_APPLY_RC": "1",
    })
    assert r.returncode != 0, "deploy must fail loud when kubectl apply fails"


def test_deploy_renders_default_ch_image(tmp_path):
    """IC-8: CHAOS_CH_IMAGE = clickhouse/clickhouse-server:<CHAOS_CH_VERSION>,
    default latest, rendered into the manifest via envsubst."""
    r, _ = _run(tmp_path, "deploy_ch_cluster", extra_env={
        "STUB_READY_REPLICAS": "3",
        "STUB_KEEPER_STATES": "leader,follower,follower",
    })
    assert r.returncode == 0, r.stderr
    assert "CHAOS_CH_IMAGE=clickhouse/clickhouse-server:latest" in _marker(tmp_path)


def test_deploy_renders_overridden_ch_version(tmp_path):
    r, _ = _run(tmp_path, "deploy_ch_cluster", extra_env={
        "CHAOS_CH_VERSION": "24.8",
        "STUB_READY_REPLICAS": "3",
        "STUB_KEEPER_STATES": "leader,follower,follower",
    })
    assert r.returncode == 0, r.stderr
    assert "CHAOS_CH_IMAGE=clickhouse/clickhouse-server:24.8" in _marker(tmp_path)


# --------------------------------------------------------------------------- #
# teardown_ch_cluster — deletes PVCs, verifies STS gone
# --------------------------------------------------------------------------- #
def test_teardown_deletes_and_verifies_gone(tmp_path):
    r, calls = _run(tmp_path, "teardown_ch_cluster", extra_env={
        "STUB_STS_AFTER_DELETE": "",   # STS gone
        "STUB_PVC_AFTER_DELETE": "",   # PVCs gone
    })
    assert r.returncode == 0, r.stderr
    assert "delete statefulset ch-chaos" in calls
    assert "delete pvc" in calls, "teardown must delete PVCs (§4 rule 4)"
    assert "delete configmap ch-chaos-config" in calls
    # both Services deleted
    assert "delete service ch-chaos ch-chaos-headless" in calls \
        or ("delete service" in calls and "ch-chaos-headless" in calls)


def test_teardown_fails_loud_if_sts_survives(tmp_path):
    r, _ = _run(tmp_path, "teardown_ch_cluster", extra_env={
        "STUB_STS_AFTER_DELETE": "statefulset.apps/ch-chaos",
    })
    assert r.returncode != 0, "teardown must die if the StatefulSet survives"


# --------------------------------------------------------------------------- #
# resolve_keeper_leader — the C5 target / T9 interface
# --------------------------------------------------------------------------- #
def test_resolve_keeper_leader_picks_leader(tmp_path):
    r, _ = _run(tmp_path, "resolve_keeper_leader", extra_env={
        "STUB_KEEPER_STATES": "follower,leader,follower",
    })
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == "ch-chaos-1", \
        f"leader pod must be echoed on stdout, got {r.stdout!r}"


def test_resolve_keeper_leader_errors_on_zero_leaders(tmp_path):
    r, _ = _run(tmp_path, "resolve_keeper_leader", extra_env={
        "STUB_KEEPER_STATES": "follower,follower,follower",
    })
    assert r.returncode != 0, "0 leaders must be an error (quorum lost)"
    assert "ch-chaos" not in r.stdout, "no pod name on the error path"


def test_resolve_keeper_leader_errors_on_two_leaders(tmp_path):
    r, _ = _run(tmp_path, "resolve_keeper_leader", extra_env={
        "STUB_KEEPER_STATES": "leader,leader,follower",
    })
    assert r.returncode != 0, "2 leaders must be an error (split-brain read)"


# --------------------------------------------------------------------------- #
# keeper_map_reset — drop the exactly-once KeeperMap zkDatabase table (§2.4)
# --------------------------------------------------------------------------- #
def test_keeper_map_reset_drops_zk_database_table(tmp_path):
    r, calls = _run(tmp_path, "keeper_map_reset")
    assert r.returncode == 0, r.stderr
    assert "DROP TABLE IF EXISTS" in calls
    # the object is the zkDatabase-named table (KeeperStateProvider.init, IC-3)
    assert "connect_state_chaos" in calls
    assert "clickhouse-client" in calls, "reset issues the drop against the target"


def test_keeper_map_reset_respects_zk_database_override(tmp_path):
    r, calls = _run(tmp_path, "keeper_map_reset", extra_env={
        "ZK_DATABASE": "connect_state_custom",
    })
    assert r.returncode == 0, r.stderr
    assert "connect_state_custom" in calls


def test_keeper_map_reset_fails_loud(tmp_path):
    r, _ = _run(tmp_path, "keeper_map_reset", extra_env={"STUB_CH_QUERY_RC": "1"})
    assert r.returncode != 0, "reset must die when the DROP fails"


# --------------------------------------------------------------------------- #
# keeper_map_smoke — CREATE/DROP a KeeperMap table (§9.2 probe)
# --------------------------------------------------------------------------- #
def test_keeper_map_smoke_creates_and_drops(tmp_path):
    r, calls = _run(tmp_path, "keeper_map_smoke")
    assert r.returncode == 0, r.stderr
    assert "KeeperMap(" in calls, "smoke must CREATE a KeeperMap-engine table"
    assert "DROP TABLE IF EXISTS" in calls, "smoke must DROP the table afterwards"


def test_keeper_map_smoke_fails_loud(tmp_path):
    r, _ = _run(tmp_path, "keeper_map_smoke", extra_env={"STUB_CH_QUERY_RC": "1"})
    assert r.returncode != 0, "smoke must die when the KeeperMap CREATE/DROP fails"


# --------------------------------------------------------------------------- #
# apply_chaos_ddl — hits_chaos ReplicatedMergeTree from the ConfigMap
# --------------------------------------------------------------------------- #
def test_apply_chaos_ddl_on_each_replica(tmp_path):
    r, calls = _run(tmp_path, "apply_chaos_ddl")
    assert r.returncode == 0, r.stderr
    # DDL read from the ConfigMap, then applied to all three replicas
    assert "get configmap ch-chaos-config" in calls
    for i in (0, 1, 2):
        assert f"ch-chaos-{i}" in calls, f"DDL must be applied on ch-chaos-{i}"


def test_apply_chaos_ddl_fails_on_empty_configmap(tmp_path):
    r, _ = _run(tmp_path, "apply_chaos_ddl", extra_env={"STUB_DDL": ""})
    assert r.returncode != 0, "empty ConfigMap DDL must be a loud failure"


def test_apply_chaos_ddl_fails_on_apply_error(tmp_path):
    r, _ = _run(tmp_path, "apply_chaos_ddl", extra_env={"STUB_CH_QUERY_RC": "1"})
    assert r.returncode != 0, "a DDL apply failure must die"


# --------------------------------------------------------------------------- #
# await_replica_sync — system.replicas queue drain / SYSTEM SYNC REPLICA
# --------------------------------------------------------------------------- #
def test_await_replica_sync_returns_when_queue_empty(tmp_path):
    r, calls = _run(tmp_path, "await_replica_sync", extra_env={
        "STUB_REPLICA_QUEUE": "0",
    })
    assert r.returncode == 0, r.stderr
    assert "SYSTEM SYNC REPLICA" in calls
    assert "system.replicas" in calls


def test_await_replica_sync_times_out_when_queue_stuck(tmp_path):
    r, _ = _run(tmp_path, "await_replica_sync", extra_env={
        "STUB_REPLICA_QUEUE": "42",
    })
    assert r.returncode != 0, "a never-draining replication queue must time out"


# --------------------------------------------------------------------------- #
# read_ch_image_digest — IC-8 read-back
# --------------------------------------------------------------------------- #
def test_read_ch_image_digest_echoes_digest(tmp_path):
    r, calls = _run(tmp_path, "read_ch_image_digest", extra_env={
        "STUB_IMAGE_ID": "clickhouse/clickhouse-server@sha256:deadbeef",
    })
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == "clickhouse/clickhouse-server@sha256:deadbeef"
    assert "containerStatuses" in calls


def test_read_ch_image_digest_fails_on_empty(tmp_path):
    r, _ = _run(tmp_path, "read_ch_image_digest", extra_env={"STUB_IMAGE_ID": ""})
    assert r.returncode != 0, "an empty imageID must be a loud failure"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
