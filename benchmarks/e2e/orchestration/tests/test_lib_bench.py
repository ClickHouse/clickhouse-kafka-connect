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
"""Offline unit tests for the run-mode-agnostic bench library (T1 / IC-1).

No cluster, no creds, no network. Covers spec §8 factoring:
  * lib_bench.sh sources cleanly under a bare `set -u` shell
  * it defines EXACTLY the IC-1 exported function set (and NONE of the
    pair-only functions — the §10.2 boundary)
  * sourcing has no top-level side effect that executes a phase (a PATH-stubbed
    kubectl records zero invocations)
  * run_pair.sh sources lib_bench.sh, so its own tests keep working
  * build_runtime_json's RUNTIME_EXTRA_KEYS_JSON parameterization (IC-1): merge
    the chaos §4 vocabulary, reject a built-in-key collision, and stay
    byte-identical to pair output when the extra map is empty/absent
"""
import json
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(__file__)
ORCH = os.path.abspath(os.path.join(HERE, ".."))
RUN_PAIR = os.path.join(ORCH, "run_pair.sh")
LIB_BENCH = os.path.join(ORCH, "lib_bench.sh")

# The IC-1 exported function set (spec §8), grouped as in the plan.
IC1_FUNCTIONS = [
    # logging
    "log", "warn", "die",
    # digest-pin family
    "_ref_is_digest", "resolve_tag_to_digest", "validate_image_ref",
    "_ecr_newer_push_exists", "check_image_provenance",
    # preload
    "phase_preload", "broker_topic_row_count", "producer_rows_sent_sum",
    # poller host
    "phase_poller_host", "teardown_poller_pod", "run_poller_sample",
    # Connect family
    "apply_secret_and_metrics", "connect_pod", "deploy_connect",
    "delete_connect", "deploy_connector", "delete_connector",
    "wait_tasks_running",
    # scale / teardown
    "phase_scale_up", "phase_teardown_topic", "phase_scale_down",
    # runtime echo
    "build_runtime_json",
    # failure hook
    "fail_run",
]

# Pair-only functions (§10.2 NOT-moved list): must NOT be defined by lib_bench.
PAIR_ONLY_FUNCTIONS = [
    "finalize_and_insert_metrics", "capture_and_record", "ingest_failed",
    "ingest_fail_reason", "compute_cpu_gate_t0", "phase_pair_cost",
    "resolve_arm_order", "print_plan", "cleanup_trap", "main", "phase_arm",
    "append_flag",
]


# --------------------------------------------------------------------------- #
# lib_bench.sh sources cleanly and defines the right functions
# --------------------------------------------------------------------------- #
def test_lib_exists():
    assert os.path.isfile(LIB_BENCH), "lib_bench.sh must exist"


def test_lib_sources_cleanly_under_set_u():
    """A bare `set -u` shell (no run_pair.sh preamble) must source lib_bench.sh
    with no unbound-variable error — sourcing has variable defaults only."""
    r = subprocess.run(
        ["bash", "-c", f'set -u; source "{LIB_BENCH}"; echo OK'],
        capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip().endswith("OK")


def test_lib_syntax_ok():
    r = subprocess.run(["bash", "-n", LIB_BENCH], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def _declared_functions(source_path, extra_env=None):
    """Source the file and return the set of declared function names."""
    env = dict(os.environ)
    if extra_env:
        env.update(extra_env)
    r = subprocess.run(
        ["bash", "-c", f'set -u; source "{source_path}" >/dev/null 2>&1; declare -F'],
        capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stderr
    # `declare -F` prints lines like "declare -f name".
    return {ln.split()[-1] for ln in r.stdout.splitlines() if ln.strip()}


@pytest.mark.parametrize("fn", IC1_FUNCTIONS)
def test_lib_defines_ic1_function(fn):
    assert fn in _declared_functions(LIB_BENCH), \
        f"lib_bench.sh must define IC-1 function {fn}"


@pytest.mark.parametrize("fn", PAIR_ONLY_FUNCTIONS)
def test_lib_does_not_define_pair_only_function(fn):
    """The §10.2 boundary: pair-only functions stay in run_pair.sh."""
    assert fn not in _declared_functions(LIB_BENCH), \
        f"lib_bench.sh must NOT define pair-only function {fn} (§10.2)"


# --------------------------------------------------------------------------- #
# sourcing executes no phase (PATH-stub kubectl, assert zero invocations)
# --------------------------------------------------------------------------- #
def test_sourcing_executes_no_phase(tmp_path):
    """Sourcing lib_bench.sh must NEVER execute a phase (IC-1: no top-level side
    effects beyond variable defaults). A PATH-stubbed kubectl records every call;
    after a source, the call log must be empty."""
    calls = tmp_path / "kubectl.calls"
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    for tool in ("kubectl", "eksctl", "envsubst", "aws", "helm"):
        stub = stub_dir / tool
        stub.write_text(f'#!/usr/bin/env bash\necho "{tool} $*" >> "{calls}"\n')
        stub.chmod(0o755)
    env = dict(os.environ)
    env["PATH"] = f"{stub_dir}:{env['PATH']}"
    r = subprocess.run(
        ["bash", "-c", f'set -u; source "{LIB_BENCH}"; echo SOURCED'],
        capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stderr
    assert "SOURCED" in r.stdout
    assert not calls.exists(), \
        f"sourcing executed external commands:\n{calls.read_text()}"


# --------------------------------------------------------------------------- #
# run_pair.sh sources lib_bench.sh (its own tests keep working automatically)
# --------------------------------------------------------------------------- #
def test_run_pair_sources_lib_bench_in_text():
    src = open(RUN_PAIR).read()
    assert "lib_bench.sh" in src, "run_pair.sh must source lib_bench.sh"


def test_run_pair_exposes_lib_functions_when_sourced():
    """Sourcing run_pair.sh must make the IC-1 functions available (proves the
    source wiring) AND still define the pair-only functions locally."""
    fns = _declared_functions(RUN_PAIR)
    for fn in IC1_FUNCTIONS:
        assert fn in fns, f"sourcing run_pair.sh must expose {fn} (via lib_bench.sh)"
    for fn in PAIR_ONLY_FUNCTIONS:
        assert fn in fns, f"run_pair.sh must still define pair-only {fn}"


# --------------------------------------------------------------------------- #
# build_runtime_json RUNTIME_EXTRA_KEYS_JSON parameterization (IC-1)
# --------------------------------------------------------------------------- #
_BASE_ENV = {
    "PAIR_ID": "2026-07-07T04-15-32Z-91ac2dd",
    "TARGET_REGION": "us-east-2",
    "ENVIRONMENT_CLASS": "staging",
    "COMPUTE_REGION": "us-east-2",
    "CFG_MAX_POLL_RECORDS": "25000",
    "CFG_MAX_PARTITION_FETCH_BYTES": "104857600",
    "CFG_FETCH_MAX_BYTES": "209715200",
    "CFG_MAX_POLL_INTERVAL_MS": "600000",
    "CFG_INSERT_TIMEOUT_MS": "180000",
    "CFG_TASKS_MAX": "3",
    "CFG_PARTITION_SCHEME": "toYear(EventDate)",
    "KAFKA_CONNECT_VERSION": "3.9.0",
    "STRIMZI_VERSION": "0.46.0",
    "PLUGIN_SHA256": "abc123",
}


def _runtime_json(arm, tier, extra_env=None, source=LIB_BENCH):
    """Source the lib and call build_runtime_json; return the CompletedProcess."""
    env = dict(os.environ)
    env.update(_BASE_ENV)
    env.pop("OUTCOME", None)
    env.pop("RUNTIME_EXTRA_KEYS_JSON", None)
    if extra_env:
        env.update(extra_env)
    r = subprocess.run(
        ["bash", "-c", f'source "{source}"; build_runtime_json "$1" "$2"',
         "bash", arm, tier],
        capture_output=True, text=True, env=env)
    return r


def test_extra_keys_empty_is_byte_identical():
    """Empty/absent RUNTIME_EXTRA_KEYS_JSON => byte-identical pair output."""
    absent = _runtime_json("head", "1")
    empty = _runtime_json("head", "1", extra_env={"RUNTIME_EXTRA_KEYS_JSON": ""})
    assert absent.returncode == 0, absent.stderr
    assert empty.returncode == 0, empty.stderr
    assert absent.stdout == empty.stdout, "empty extra map must not change output"


def test_extra_keys_lib_matches_run_pair():
    """The SAME build_runtime_json is reachable via lib_bench.sh and run_pair.sh
    (proves the pair inherits the function, byte-for-byte)."""
    via_lib = _runtime_json("head", "1", source=LIB_BENCH)
    via_pair = _runtime_json("head", "1", source=RUN_PAIR)
    assert via_lib.returncode == 0 and via_pair.returncode == 0
    assert via_lib.stdout == via_pair.stdout


def test_extra_keys_merged_after_mandatory_check():
    """The chaos §4 vocabulary is merged into the runtime map."""
    extra = {
        "connector": "kafka-connect-chaos",
        "chaos_id": "2026-07-16T00-00-00Z-abc",
        "chaos_mode": "monkey",
        "chaos_seed": "1",
        "fault_type": "C4",
    }
    r = _runtime_json("head", "1",
                      extra_env={"RUNTIME_EXTRA_KEYS_JSON": json.dumps(extra)})
    assert r.returncode == 0, r.stderr
    rt = json.loads(r.stdout)
    for k, v in extra.items():
        assert rt[k] == v, f"extra key {k} not merged"
    # the built-in identity keys survive the merge
    assert rt["arm"] == "head" and rt["tier"] == "1"


def test_extra_keys_output_still_sorted():
    r = _runtime_json("head", "1", extra_env={
        "RUNTIME_EXTRA_KEYS_JSON": json.dumps({"zeta": "1", "alpha": "2"})})
    assert r.returncode == 0, r.stderr
    keys = list(json.loads(r.stdout).keys())
    assert keys == sorted(keys), "merged map must stay sort_keys=True"


@pytest.mark.parametrize("colliding", ["arm", "tier", "pair_id", "batch_size",
                                       "exactlyOnce", "dataset"])
def test_extra_keys_collision_rejected(colliding):
    """A key that collides with a built-in runtime key is a HARD error — chaos
    must never silently overwrite pair identity/config keys (IC-1)."""
    r = _runtime_json("head", "1", extra_env={
        "RUNTIME_EXTRA_KEYS_JSON": json.dumps({colliding: "hijack"})})
    assert r.returncode != 0, f"collision on {colliding} must fail: {r.stdout}"
    assert "collide" in r.stderr.lower() or "collision" in r.stderr.lower()


def test_extra_keys_invalid_json_rejected():
    r = _runtime_json("head", "1",
                      extra_env={"RUNTIME_EXTRA_KEYS_JSON": "{not json"})
    assert r.returncode != 0
    assert "JSON" in r.stderr or "json" in r.stderr


def test_extra_keys_non_object_rejected():
    """A flat JSON OBJECT is required; a JSON array/scalar is rejected."""
    r = _runtime_json("head", "1",
                      extra_env={"RUNTIME_EXTRA_KEYS_JSON": '["a","b"]'})
    assert r.returncode != 0
    assert "object" in r.stderr.lower()


def test_extra_keys_do_not_bypass_mandatory_check():
    """The extra map is merged AFTER the mandatory-key check: a missing
    mandatory key still fails even if the extra map is well-formed."""
    r = _runtime_json("head", "1", extra_env={
        "PAIR_ID": "",
        "RUNTIME_EXTRA_KEYS_JSON": json.dumps({"connector": "kafka-connect-chaos"})})
    assert r.returncode != 0
    assert "mandatory key" in r.stderr


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
