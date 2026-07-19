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
"""Offline unit tests for the chaos orchestrator assembly (task T11 / IC-3).

No cluster, no creds, no kubeconfig, no network. Everything is exercised through
chaos_run.sh's --plan dry-run, source-text assertions on its functions, and
PATH-stub-only invocations — mirroring test_orchestration.py's patterns.

Covered:
  * --plan goldens for smoke / monkey / quorum-loss / aggressive (execute NOTHING)
  * flag validation (--quorum-loss forces eo=1; --watch-cell rejected with eo=1;
    missing --seed in monkey rejected; invalid mode / exactly-once rejected)
  * source-text law: isolation guard before any mutating phase; image validation
    before phases; teardown verifies STS gone + nodegroups 0 (§9.8); artifact
    written on the timeout paths; NO perf.* / DWH export anywhere; digest
    rejection of bare tags (source the script, call the shared validator — proves
    the digest law is inherited from lib_bench.sh); cleanup trap present;
    keeper_map_reset only on the eo=1 path
  * sourcing chaos_run.sh executes NO phase (zero kubectl invocations)
"""
import json
import os
import re
import subprocess
import sys

import pytest

HERE = os.path.dirname(__file__)
ORCH = os.path.abspath(os.path.join(HERE, ".."))
CHAOS_RUN = os.path.join(ORCH, "chaos_run.sh")
REPO = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
RENDER_CONNECTOR = os.path.join(ORCH, "render_connector.py")
CHAOS_CONNECTOR_TMPL = os.path.join(
    REPO, "benchmarks", "e2e", "chaos", "templates", "chaos-connector.json.tmpl")


def _src():
    return open(CHAOS_RUN).read()


def _extract_function(src, name):
    m = re.search(rf"^{re.escape(name)}\(\) \{{\n(.*?)^\}}", src, re.S | re.M)
    assert m, f"could not extract function {name} from chaos_run.sh"
    return m.group(1)


def _run_plan(args, extra_env=None):
    env = dict(os.environ)
    if extra_env:
        env.update(extra_env)
    return subprocess.run(["bash", CHAOS_RUN, "--plan", *args],
                          capture_output=True, text=True, env=env)


# --------------------------------------------------------------------------- #
# --plan goldens (dry-run; execute NOTHING).
# --------------------------------------------------------------------------- #
def test_plan_monkey_exactly_once_golden():
    r = _run_plan(["--mode", "monkey", "--seed", "1", "--exactly-once", "1"])
    assert r.returncode == 0, r.stderr
    out = r.stdout
    for line in (
        "mode            : monkey",
        "delivery mode   : exactly_once (exactly_once=1)",
        "chaos_mode      : monkey",
        "seed            : 1",
        "rounds          : 20",
        "faults          : C1,C2,C3,C4,C5",
        "profile         : default",
        "quorum-loss     : no",
        "group=ch-sink-chaos-eo1 connector_cr=chaos-clickhouse-sink connect_cr=bench-connect",
        "connector=kafka-connect-chaos environment_class=self_hosted",
        "zkPath=/kafka-connect-chaos zkDatabase=connect_state_chaos",
        "scale           : bench-ng 0->5 + connect-ng 0->1 (SCALE_UP_NODES=5)",
        "[eo=1: YES]",
        "monkey: schedule.py(seed=1,rounds=20)",
    ):
        assert line in out, f"missing plan line: {line!r}"
    # dry-run must not name any executing tool output
    assert "=== END PHASE PLAN ===" in out


def test_plan_smoke_at_least_once_golden():
    r = _run_plan(["--mode", "smoke", "--exactly-once", "0"])
    assert r.returncode == 0, r.stderr
    out = r.stdout
    for line in (
        "mode            : smoke",
        "delivery mode   : at_least_once (exactly_once=0)",
        "chaos_mode      : smoke",
        "seed            : <none>",
        "group=ch-sink-chaos-eo0 connector_cr=chaos-clickhouse-sink connect_cr=bench-connect",
        "[eo=0: SKIPPED]",
        "smoke : drain-progress 50% -> single fault -> recovery gate -> fence",
    ):
        assert line in out, f"missing plan line: {line!r}"
    # smoke plan has no monkey schedule step
    assert "schedule.py(" not in out


def test_plan_quorum_loss_forces_eo1_and_tags_mode():
    """--quorum-loss forces exactly-once and tags chaos_mode=quorum_loss (§3.7)."""
    r = _run_plan(["--mode", "monkey", "--seed", "7", "--exactly-once", "0",
                   "--quorum-loss"])
    assert r.returncode == 0, r.stderr
    out = r.stdout
    assert "delivery mode   : exactly_once (exactly_once=1)" in out
    assert "chaos_mode      : quorum_loss" in out
    assert "faults          : quorum_loss" in out
    assert "quorum-loss     : yes" in out
    # the override is surfaced loudly on stderr
    assert "forces --exactly-once 1" in r.stderr


def test_plan_aggressive_profile():
    r = _run_plan(["--mode", "monkey", "--seed", "3", "--exactly-once", "1",
                   "--aggressive"])
    assert r.returncode == 0, r.stderr
    assert "profile         : aggressive" in r.stdout


def test_plan_executes_nothing_without_kubectl(tmp_path):
    """--plan must not shell out to kubectl/eksctl/aws/docker: it resolves flags
    and prints, nothing more. Run with a PATH that has NONE of them + a recording
    stub, and assert zero calls."""
    calls = tmp_path / "calls.log"
    for tool in ("kubectl", "eksctl", "aws", "docker", "envsubst"):
        p = tmp_path / tool
        p.write_text(f'#!/usr/bin/env bash\necho "{tool} $*" >> "{calls}"\n')
        p.chmod(0o755)
    env = dict(os.environ, PATH=f"{tmp_path}:/usr/bin:/bin")
    r = subprocess.run(["bash", CHAOS_RUN, "--plan", "--mode", "monkey",
                        "--seed", "1", "--exactly-once", "1"],
                       capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stderr
    assert not calls.exists(), f"--plan invoked external tools: {calls.read_text()}"


# --------------------------------------------------------------------------- #
# Flag validation.
# --------------------------------------------------------------------------- #
def test_watch_cell_rejected_with_exactly_once():
    r = _run_plan(["--mode", "smoke", "--exactly-once", "1", "--watch-cell"])
    assert r.returncode != 0
    assert "watch-cell" in r.stderr and "exactly-once 1" in r.stderr


def test_watch_cell_allowed_at_least_once_sets_ipwb_field():
    r = _run_plan(["--mode", "smoke", "--exactly-once", "0", "--watch-cell"])
    assert r.returncode == 0, r.stderr
    assert "watch-cell      : yes (ignore_partitions_when_batching=1)" in r.stdout
    assert "--ipwb true" in r.stdout


def test_monkey_requires_seed():
    r = _run_plan(["--mode", "monkey", "--exactly-once", "1"])
    assert r.returncode != 0
    assert "requires --seed" in r.stderr


def test_invalid_mode_rejected():
    r = _run_plan(["--mode", "bogus", "--exactly-once", "1"])
    assert r.returncode != 0
    assert "--mode must be" in r.stderr


@pytest.mark.parametrize("eo", ["2", "yes", ""])
def test_invalid_exactly_once_rejected(eo):
    args = ["--mode", "smoke"]
    if eo != "":
        args += ["--exactly-once", eo]
    r = _run_plan(args)
    assert r.returncode != 0


def test_unknown_arg_rejected():
    r = _run_plan(["--mode", "smoke", "--exactly-once", "0", "--nope"])
    assert r.returncode != 0
    assert "unknown arg" in r.stderr


# --------------------------------------------------------------------------- #
# Source-text law — ordering, isolation, teardown, artifact-on-every-path.
# --------------------------------------------------------------------------- #
def test_isolation_guard_before_any_mutating_phase():
    """§4 rule 1: the isolation guard must run before ANY cluster-mutating phase
    (scale-up is the first mutation)."""
    body = _extract_function(_src(), "main")
    assert body.index("isolation_guard") < body.index("phase_scale_up"), \
        "isolation guard must precede phase_scale_up"
    guard = _extract_function(_src(), "isolation_guard")
    # it keys off the PAIR's connector (chaos uses a distinct one)
    assert "PAIR_CONNECTOR_NAME" in guard
    assert "die" in guard  # refuses (does not merely warn)


def test_image_validation_before_phases_and_covers_arm_and_producer():
    src = _src()
    body = _extract_function(src, "main")
    assert body.index("phase_validate_images") < body.index("phase_scale_up"), \
        "image validation must precede phase_scale_up"
    ph = _extract_function(src, "phase_validate_images")
    assert "validate_image_ref ARM_IMAGE" in ph
    assert "validate_image_ref PRODUCER_IMAGE" in ph
    # CH image is the IC-8 exception: NOT digest-validated here
    assert "validate_image_ref CHAOS_CH" not in ph
    assert "validate_image_ref CH_IMAGE" not in ph


def test_isolation_and_images_precede_config_which_precedes_scaleup():
    body = _extract_function(_src(), "main")
    assert (body.index("isolation_guard")
            < body.index("phase_validate_images")
            < body.index("phase_validate_config")
            < body.index("phase_scale_up"))


def test_config_validation_uses_render_connector_validator():
    """The §3.2 config-combination gate runs via render_connector.py (which calls
    validate_chaos_config), reusing T7 rather than re-implementing it."""
    ph = _extract_function(_src(), "render_chaos_connector")
    assert "render_connector.py" in ph
    assert "--exactly-once" in ph and "--ipwb" in ph


def test_keeper_map_reset_only_on_exactly_once_path():
    body = _extract_function(_src(), "phase_ch_cluster")
    assert body.count("keeper_map_reset") == 1, \
        "keeper_map_reset must appear exactly once"
    # it lives inside an `if [ "${EXACTLY_ONCE}" = "1" ]` guard
    guard = 'if [ "${EXACTLY_ONCE}" = "1" ]'
    assert guard in body
    assert body.index(guard) < body.index("keeper_map_reset") < body.index("else")


def test_teardown_verifies_sts_gone_and_nodegroups_zero():
    body = _extract_function(_src(), "teardown_chaos")
    # STS gone is verified by teardown_ch_cluster; nodegroups 0 by scale-down
    assert "teardown_ch_cluster" in body
    assert "phase_scale_down" in body
    assert "§9.8" in body, "the §9.8 verify-both requirement must be documented"


def test_artifact_written_on_recovery_timeout_paths():
    """A recovery-gate timeout must write the artifact (IC-3: on EVERY path) via
    abort_with_artifact, mapping 21->fail/t_recover_timeout, 22->unverified."""
    handler = _extract_function(_src(), "_handle_recovery_rc")
    assert "abort_with_artifact 1 \"failed\" \"t_recover_timeout\"" in handler
    assert "abort_with_artifact 3 \"integrity_unverified\" \"t_recover_timeout\"" in handler
    abort = _extract_function(_src(), "abort_with_artifact")
    assert "write_result_artifact" in abort
    assert "teardown_chaos" in abort
    # abort writes the artifact BEFORE exiting
    assert abort.index("write_result_artifact") < abort.index('exit "${code}"')


def test_artifact_written_on_settle_timeout_path():
    body = _extract_function(_src(), "phase_quiescence")
    assert "abort_with_artifact 3 \"integrity_unverified\" \"t_settle_timeout\"" in body


def test_oracle_exit_codes_mapped_to_ic3():
    """oracle 0->PASS(exit 0), 1->MISMATCH(exit 1), else->UNVERIFIED(exit 3)."""
    body = _extract_function(_src(), "main")
    region = body[body.index('case "${oracle_rc}" in'):body.index("esac")]
    assert "0) exit_code=0" in region
    assert "1) exit_code=1" in region
    assert "*) exit_code=3" in region
    # the artifact is written before the final teardown+exit
    assert body.index("write_result_artifact") < body.index("exit \"${exit_code}\"")


def test_cleanup_trap_present_and_installed():
    src = _src()
    assert "trap cleanup_trap EXIT INT TERM" in src
    trap = _extract_function(src, "cleanup_trap")
    # best-effort artifact + teardown on any abnormal exit
    assert "write_result_artifact" in trap
    assert "teardown_chaos" in trap
    # the trap is installed AFTER the pre-flight gates (which must be able to die
    # cleanly) and before the first mutating phase
    main_body = _extract_function(src, "main")
    assert main_body.index("phase_validate_config") \
        < main_body.index("trap cleanup_trap") \
        < main_body.index("phase_scale_up")


# --------------------------------------------------------------------------- #
# Decision §10.2: NO perf.* / DWH export path anywhere in the chaos orchestrator.
# --------------------------------------------------------------------------- #
def test_no_perf_or_dwh_export_anywhere():
    src = _src()
    for banned in (
        "perf.",                       # no perf.runs / perf.metrics
        "export_metrics_to_dwh",
        "emit_run_cost",
        "insert_run_record",
        "run_metrics_sql",
        "rollback_run_metrics",
        "capture_and_record",
        "finalize_and_insert_metrics",
    ):
        assert banned not in src, f"chaos_run.sh must not reference {banned!r} (§10.2)"


def test_writes_the_ic9_artifact_via_write_artifact_py():
    body = _extract_function(_src(), "write_result_artifact")
    assert "write_artifact.py" in body
    # the IC-9 outcome/run_conclusion vocabulary is what the writer enforces
    assert "OUTCOME=" in body and "RUN_CONCLUSION=" in body


def test_banned_uniqueness_formula_absent():
    """The oracle formula law (§5): count()-uniqExact() on the target is BANNED.
    The chaos orchestrator delegates to check_integrity.py and must never inline
    the banned family."""
    src = _src()
    assert "uniqExact" not in src or "count() - uniqExact" not in src
    assert "count()-uniqExact" not in src
    assert "count() - uniqExact" not in src


# --------------------------------------------------------------------------- #
# Digest law inheritance: sourcing chaos_run.sh exposes lib_bench's
# validate_image_ref, and it rejects a bare tag on an unresolvable registry
# exactly as run_pair.sh does (source the script, call the shared validator).
# --------------------------------------------------------------------------- #
def _source_and_validate(ref, allow_tag=False):
    env = dict(os.environ)
    env["KAFKA_ALLOW_TAG"] = "1" if allow_tag else "0"
    script = f'source "{CHAOS_RUN}"; validate_image_ref TESTVAR "{ref}"'
    r = subprocess.run(["bash", "-c", script],
                       capture_output=True, text=True, env=env)
    return r.returncode, r.stdout.strip(), r.stderr


def test_sourcing_chaos_run_defines_shared_validator_and_rejects_bare_tag():
    rc, out, err = _source_and_validate(
        "registry.example.com/team/connect-bench:chaos-tag", allow_tag=False)
    assert rc != 0, f"a bare tag must be rejected (digest law), got rc={rc}"
    assert "MUTABLE TAG" in err
    assert out == ""


def test_sourcing_chaos_run_accepts_digest_ref():
    ref = "ghcr.io/clickhouse/clickhouse-kafka-connect@sha256:" + "a" * 64
    rc, out, _ = _source_and_validate(ref, allow_tag=False)
    assert rc == 0
    assert out == ref


def test_allow_tag_flag_maps_to_escape_hatch():
    assert "--allow-tag)     export KAFKA_ALLOW_TAG=1" in _src()


def test_sourcing_chaos_run_executes_no_phase(tmp_path):
    """Sourcing the orchestrator must have NO side effect beyond defs/defaults —
    it must never run a kubectl call (IC-1 invariant, inherited)."""
    calls = tmp_path / "kubectl.log"
    stub = tmp_path / "kubectl"
    stub.write_text(f'#!/usr/bin/env bash\necho "$*" >> "{calls}"\n')
    stub.chmod(0o755)
    env = dict(os.environ, PATH=f"{tmp_path}:{os.environ.get('PATH', '')}")
    r = subprocess.run(["bash", "-c", f'set -u; source "{CHAOS_RUN}"'],
                       capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stderr
    assert not calls.exists(), f"sourcing ran kubectl: {calls.read_text()}"


# --------------------------------------------------------------------------- #
# Identities (IC-3) surfaced in the plan and honored as overridable constants.
# --------------------------------------------------------------------------- #
def test_identities_match_ic3():
    r = _run_plan(["--mode", "monkey", "--seed", "1", "--exactly-once", "1"])
    out = r.stdout
    assert "topic=hits-chaos" in out
    assert "dlq=hits-chaos-dlq" in out
    assert "table=clickbench.hits_chaos" in out
    assert "connector_cr=chaos-clickhouse-sink" in out
    assert "connect_cr=bench-connect" in out
    assert "connector=kafka-connect-chaos" in out
    assert "environment_class=self_hosted" in out


def test_bench_log_prefix_and_scale_up_nodes_retargeted():
    src = _src()
    assert 'BENCH_LOG_PREFIX="${BENCH_LOG_PREFIX:-chaos_run}"' in src
    assert 'SCALE_UP_NODES="${SCALE_UP_NODES:-5}"' in src


# --------------------------------------------------------------------------- #
# T15 — `--reap`: state-free, idempotent teardown-only mode (kill-safety net).
# --------------------------------------------------------------------------- #
def test_reap_accepted_without_mode_or_exactly_once():
    """--reap is teardown-only: it must NOT require --mode / --exactly-once /
    --seed (those gate LIVE runs). `--plan --reap` alone resolves + exits 0."""
    r = _run_plan(["--reap"])
    assert r.returncode == 0, r.stderr
    # never complains about the missing run flags
    assert "--mode must be" not in r.stderr
    assert "requires --seed" not in r.stderr
    assert "--exactly-once must be" not in r.stderr


def test_plan_reap_prints_reap_plan_and_exits_zero():
    r = _run_plan(["--reap"])
    assert r.returncode == 0, r.stderr
    out = r.stdout
    assert "REAP PLAN" in out
    assert "=== END REAP PLAN ===" in out
    # it names the cost-critical §9.8 assertion and executes nothing
    assert "9.8" in out
    assert "nodegroups" in out.lower()
    assert "StatefulSet" in out or "ch_sts" in out
    # the full live phase plan must NOT be printed on the reap path
    assert "PHASE 6" not in out
    assert "PHASE PLAN" not in out
    # it documents that the live-run pre-flight gates are deliberately skipped
    assert "gates skipped" in out


def test_reap_is_reap_only_even_when_combined_with_run_flags():
    """If --reap is combined with a run, treat as reap-only (reap wins)."""
    r = _run_plan(["--reap", "--mode", "monkey", "--seed", "1",
                   "--exactly-once", "1"])
    assert r.returncode == 0, r.stderr
    assert "REAP PLAN" in r.stdout
    assert "PHASE PLAN" not in r.stdout


def test_reap_plan_executes_nothing(tmp_path):
    """--plan --reap resolves + prints; it must shell out to NOTHING."""
    calls = tmp_path / "calls.log"
    for tool in ("kubectl", "eksctl", "aws", "docker", "envsubst"):
        p = tmp_path / tool
        p.write_text(f'#!/usr/bin/env bash\necho "{tool} $*" >> "{calls}"\n')
        p.chmod(0o755)
    env = dict(os.environ, PATH=f"{tmp_path}:/usr/bin:/bin")
    r = subprocess.run(["bash", CHAOS_RUN, "--plan", "--reap"],
                       capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stderr
    assert not calls.exists(), f"--plan --reap invoked external tools: {calls.read_text()}"


def test_reap_branch_precedes_and_skips_liverun_gates():
    """The reap branch must run BEFORE validate_flags and must NOT pass through
    the PHASE-0 live-run gates (isolation guard / image / config validation)."""
    body = _extract_function(_src(), "main")
    assert "run_reap" in body
    # reap is decided at the very top of main, before flag validation for a run
    assert body.index("run_reap") < body.index("validate_flags")
    # and before (thus bypassing) every PHASE-0 live-run gate
    assert body.index("run_reap") < body.index("isolation_guard")
    assert body.index("run_reap") < body.index("phase_validate_images")
    assert body.index("run_reap") < body.index("phase_validate_config")


def test_reap_reuses_teardown_and_verifies_sts_and_nodegroups():
    """run_reap must REUSE teardown_chaos (DRY — it owns the identities) and then
    assert §9.8: CH StatefulSet gone AND both nodegroups desired=0. It must NOT
    re-implement the isolation guard / image validation."""
    body = _extract_function(_src(), "run_reap")
    assert "teardown_chaos" in body, "reap must reuse teardown_chaos (DRY)"
    # cost-leak verdict: STS gone + nodegroups 0
    assert "statefulset" in body.lower()
    assert "scale-down.sh" in body or "phase_scale_down" in body
    assert "9.8" in body
    # reap never runs the live-run pre-flight gates
    assert "isolation_guard" not in body
    assert "phase_validate_images" not in body
    # both chaos consumer groups are reaped (we do not know which eo ran)
    assert "ch-sink-chaos-eo" in body


def test_reap_parses_flag_in_parse_args():
    pa = _extract_function(_src(), "parse_args")
    assert "--reap" in pa


# --------------------------------------------------------------------------- #
# F-L2b — scoped one-shot preload timeout (fail-fast; §preload).
# --------------------------------------------------------------------------- #
def test_chaos_producer_timeout_default_900_exported_as_producer_timeout():
    """CHAOS_PRODUCER_TIMEOUT (default 900) must be exported as PRODUCER_TIMEOUT
    before the one-shot base-preload wait, so a failed smoke/base preload fails
    fast instead of waiting the pair's 6h default. It is scoped to the chaos
    preload path (not a global)."""
    src = _src()
    assert 'CHAOS_PRODUCER_TIMEOUT="${CHAOS_PRODUCER_TIMEOUT:-900}"' in src
    body = _extract_function(src, "phase_chaos_preload")
    assert 'export PRODUCER_TIMEOUT="${CHAOS_PRODUCER_TIMEOUT}"' in body
    # the wait must use the scoped bound, and the export precedes the wait
    assert body.index("export PRODUCER_TIMEOUT") < body.index("--for=condition=complete job/hits-producer")
    # the stream producer is SIGTERM-fenced (IC-7), documented as unaffected
    assert "IC-7" in body


# --------------------------------------------------------------------------- #
# IC-3 threading audit — chaos-distinct identities must reach the RENDERED
# artifacts, never fall back to a pair default (the TOPIC-bug class).
# --------------------------------------------------------------------------- #
def test_ic3_group_is_threaded_into_rendered_connector(tmp_path):
    """The consumer GROUP (ch-sink-chaos-eo<0|1>) is the one identity that VARIES
    per run; it MUST be threaded into the rendered connector via --group, not
    left to Kafka Connect's connect-<name> default."""
    out = tmp_path / "cr.json"
    r = subprocess.run(
        [sys.executable, RENDER_CONNECTOR, "--template", CHAOS_CONNECTOR_TMPL,
         "--exactly-once", "1", "--ipwb", "false",
         "--group", "ch-sink-chaos-eo1", "--out", str(out)],
        capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    cr = json.loads(out.read_text())
    assert cr["spec"]["config"]["consumer.override.group.id"] == "ch-sink-chaos-eo1"
    assert cr["spec"]["config"]["exactlyOnce"] == "true"


def test_ic3_chaos_connector_template_identities_are_chaos_distinct():
    """IC-3 audit: the chaos connector template's baked identities must equal the
    chaos defaults and NEVER a pair default (the TOPIC-bug class: a chaos env that
    silently renders a pair value). Locks name/topics/topic2TableMap/database/DLQ
    against drift toward the pair's hits / bench-clickhouse-sink."""
    tmpl = json.loads(open(CHAOS_CONNECTOR_TMPL).read())
    cfg = tmpl["spec"]["config"]
    assert tmpl["metadata"]["name"] == "chaos-clickhouse-sink"
    assert cfg["topics"] == "hits-chaos"
    assert cfg["topic2TableMap"] == "hits-chaos=hits_chaos"
    assert cfg["database"] == "clickbench"
    assert cfg["errors.deadletterqueue.topic.name"] == "hits-chaos-dlq"
    # never the PAIR identities
    blob = json.dumps(tmpl)
    assert "bench-clickhouse-sink" not in blob
    assert '"topics": "hits"' not in blob


def test_ic3_producer_job_topic_is_threaded_not_pair_default():
    """The producer Job is rendered from the PAIR's job.yaml (authored 'hits');
    chaos MUST thread --topic so the producer targets hits-chaos, not the pair
    topic (this was the original TOPIC cross-wire bug)."""
    body = _extract_function(_src(), "phase_chaos_preload")
    assert '--topic "${TOPIC}"' in body


# --------------------------------------------------------------------------- #
# #771 wiring: self-hosted CH creds/target must be SET by chaos_run.sh, or the
# PHASE-5 Connect deploy (apply_secret_and_metrics) and the PHASE-9 oracle
# (check_integrity.py --direct) fall back to a pair default and fail the live run.
# The chaos CH is user `default` with an EMPTY password over plaintext 8123, no
# TLS (ch-cluster.yaml users.xml / T7 connector template).
# --------------------------------------------------------------------------- #
LIB_BENCH = os.path.join(ORCH, "lib_bench.sh")


def test_target_ch_creds_and_wire_exported_by_chaos_run():
    """USER=default, PASSWORD SET-but-empty, PORT=8123, SECURE=false — exported so
    both the Connect Secret and the oracle read the self-hosted target."""
    src = _src()
    assert 'export TARGET_CH_USER="${TARGET_CH_USER:-default}"' in src
    # SET-but-allow-empty: `-` not `:-` (an operator override survives; unset => "")
    assert 'export TARGET_CH_PASSWORD="${TARGET_CH_PASSWORD-}"' in src
    assert 'export TARGET_CH_PORT="${TARGET_CH_PORT:-8123}"' in src
    assert 'export TARGET_CH_SECURE="${TARGET_CH_SECURE:-false}"' in src


def test_target_ch_creds_resolve_to_self_hosted_defaults_when_sourced():
    """Behavioral: sourcing resolves the self-hosted defaults AND leaves the
    empty password SET (a bare ${TARGET_CH_PASSWORD} would abort under set -u)."""
    # A CLEAN env (no inherited TARGET_CH_*) so we observe the script's own
    # defaults, not whatever the caller's shell happened to export.
    clean = {k: v for k, v in os.environ.items() if not k.startswith("TARGET_CH_")}
    script = (f'set -u; source "{CHAOS_RUN}"; '
              'printf "U=%s;PW=[%s];PORT=%s;SEC=%s" '
              '"${TARGET_CH_USER}" "${TARGET_CH_PASSWORD}" '
              '"${TARGET_CH_PORT}" "${TARGET_CH_SECURE}"')
    r = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=clean)
    assert r.returncode == 0, r.stderr
    assert "U=default" in r.stdout
    assert "PW=[]" in r.stdout        # SET and empty (no unbound-var abort)
    assert "PORT=8123" in r.stdout
    assert "SEC=false" in r.stdout


def test_oracle_threads_target_secure_and_port():
    body = _extract_function(_src(), "phase_oracle")
    # The oracle reads via resolved oracle_host/oracle_port (set from the
    # port-forward below or TARGET_CH_* directly), not the raw in-cluster host.
    assert 'TARGET_CH_HOST="${oracle_host}"' in body
    assert 'TARGET_CH_PORT="${oracle_port}"' in body
    assert 'oracle_port="${TARGET_CH_PORT}"' in body
    assert 'TARGET_CH_SECURE=' in body
    # password passed SET-but-empty (not :- which would coerce), matching lib_bench
    assert 'TARGET_CH_PASSWORD="${TARGET_CH_PASSWORD-}"' in body


def test_oracle_port_forwards_in_cluster_service():
    """The self-hosted CH client Service DNS is in-cluster-only; the operator-side
    oracle must port-forward it to localhost (regression for the live-L2 finding
    2026-07-19: oracle failed to resolve ch-chaos.kafka-bench.svc). Localhost
    hosts skip the forward; the tunnel is killed after the read."""
    body = _extract_function(_src(), "phase_oracle")
    assert 'port-forward "svc/${CH_SVC}"' in body
    assert "127.0.0.1|localhost)" in body           # already-local short-circuit
    assert 'oracle_host="127.0.0.1"' in body
    assert 'kill "${pf_pid}"' in body                # tunnel torn down after read


def test_smoke_fault_honors_faults_flag():
    """smoke picks its single fault from SMOKE_FAULT, else the first --faults
    entry (so `--faults C4` selects C4, not silently C1). Regression for the
    live-L2 footgun where --faults was ignored by smoke and defaulted to C1."""
    body = _extract_function(_src(), "run_smoke")
    assert 'fault="${SMOKE_FAULT:-${FAULTS%%,*}}"' in body
    assert '"${SMOKE_FAULT:-C1}"' not in body        # the old ignore-faults default is gone


def test_lib_bench_password_allows_empty_but_requires_set():
    """apply_secret_and_metrics: hostname/username REQUIRE non-empty (${VAR:?});
    password requires SET but ALLOWS EMPTY (${VAR?}) for the self-hosted CH."""
    lib = open(LIB_BENCH).read()
    assert '--from-literal=hostname="${TARGET_CH_HOST:?}"' in lib
    assert '--from-literal=username="${TARGET_CH_USER:?}"' in lib
    assert '--from-literal=password="${TARGET_CH_PASSWORD?}"' in lib
    # the old empty-rejecting form must be gone
    assert '--from-literal=password="${TARGET_CH_PASSWORD:?}"' not in lib


def test_apply_secret_accepts_empty_password_rejects_unset(tmp_path):
    """Behavioral: with a kubectl stub, an empty-but-SET password is accepted
    (self-hosted CH), while an UNSET password aborts (require SET)."""
    stub = tmp_path / "kubectl"
    stub.write_text("#!/usr/bin/env bash\ncat >/dev/null 2>&1 || true\nexit 0\n")
    stub.chmod(0o755)
    env_base = dict(os.environ, PATH=f"{tmp_path}:{os.environ.get('PATH', '')}",
                    TARGET_CH_HOST="ch-chaos.kafka-bench.svc", TARGET_CH_USER="default")
    # Mirror the orchestrator's shell mode: chaos_run.sh / run_pair.sh both run
    # under `set -uo pipefail`, so a ${VAR?} abort in the first pipeline stage
    # propagates (pipefail) to the `|| die`.
    call = f'set -uo pipefail; source "{LIB_BENCH}"; apply_secret_and_metrics'
    # empty-but-set password -> accepted (exit 0)
    env_ok = dict(env_base, TARGET_CH_PASSWORD="")
    r_ok = subprocess.run(["bash", "-c", call], capture_output=True, text=True, env=env_ok)
    assert r_ok.returncode == 0, r_ok.stderr
    # unset password -> aborts loudly (require SET)
    env_bad = dict(env_base)
    env_bad.pop("TARGET_CH_PASSWORD", None)
    r_bad = subprocess.run(["bash", "-c", call], capture_output=True, text=True, env=env_bad)
    assert r_bad.returncode != 0, "an UNSET password must still abort"
