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
"""Offline unit tests for the task-31 orchestration pure logic.

No cluster, no creds, no network. Covers:
  * day-parity arm order (even->head first, odd->pinned first) + the
    ARM_ORDER_SPEC single-source override (review F10)
  * run_id construction (<pair_id>-<arm>-t<tier>, contract §1.2)
  * config-echo runtime-map assembly (build_runtime_json, directive c; warm_up
    omitted; mandatory keys hard-fail when empty — review F9)
  * producer-summary JSON parsing (rows_expected)
  * §6 config cross-check: every §6 value present in BOTH the connector config
    template AND the runtime-map echo (overseer self-verification)
  * run_cost_usd invocation point: end-of-pair, outside capture gating (F6)
  * KafkaConnect CR config.providers wiring (F1) + the -rate exporter rule
    over-match fix (F12)
"""
import json
import os
import re
import subprocess
import sys

import pytest
import yaml

HERE = os.path.dirname(__file__)
ORCH = os.path.abspath(os.path.join(HERE, ".."))
RUN_PAIR = os.path.join(ORCH, "run_pair.sh")
CONNECTOR_TMPL = os.path.join(ORCH, "templates", "kafkaconnector.json.tmpl")
KAFKACONNECT_TMPL = os.path.join(ORCH, "templates", "kafkaconnect.yaml.tmpl")
METRICS_CM = os.path.join(ORCH, "templates", "connect-metrics-configmap.yaml")
HITS_DDL = os.path.abspath(os.path.join(ORCH, "..", "sql", "clickbench",
                                        "02_create_hits.sql"))


# --------------------------------------------------------------------------- #
# day-parity arm order — exercise the exact bash logic from run_pair.sh
# --------------------------------------------------------------------------- #
def _arm_order_for_doy(doy: int):
    # Mirror resolve_arm_order(): 10# base-10, even->head first.
    if doy % 2 == 0:
        return ("head", "pinned")
    return ("pinned", "head")


@pytest.mark.parametrize("doy,expected", [
    (2, ("head", "pinned")),    # even
    (3, ("pinned", "head")),    # odd
    (7, ("pinned", "head")),    # odd, leading-zero-ish (007)
    (100, ("head", "pinned")),  # even
    (365, ("pinned", "head")),  # odd
])
def test_arm_order_parity(doy, expected):
    assert _arm_order_for_doy(doy) == expected


def test_arm_order_matches_shell():
    """The exact bash parity expression used by BOTH run_pair.sh
    (resolve_arm_order) and benchmark-nightly.yml (image-slot resolver) must
    agree with the python mirror for a sweep of day-of-year values, including
    leading-zero DOYs (007) that would be octal without the 10# base-10 prefix.
    The one-liner is asserted to be present verbatim in run_pair.sh so this test
    tracks the real code, not a copy."""
    src = open(RUN_PAIR).read()
    assert "if [ $((10#$doy % 2)) -eq 0 ]; then" in src, \
        "parity one-liner drifted from run_pair.sh"
    # The exact expression from run_pair.sh (even -> head first).
    script = (
        'doy="$1"; if [ $((10#$doy % 2)) -eq 0 ]; then echo "head pinned"; '
        'else echo "pinned head"; fi\n'
    )
    for doy in ("002", "003", "007", "100", "365"):
        out = subprocess.run(["bash", "-c", script, "bash", doy],
                             capture_output=True, text=True).stdout.strip()
        exp = " ".join(_arm_order_for_doy(int(doy)))
        assert out == exp, f"doy={doy}: shell={out!r} python={exp!r}"


# --------------------------------------------------------------------------- #
# ARM_ORDER_SPEC single-source override (review F10) — exercised via the real
# script's --plan mode (no cluster/creds needed).
# --------------------------------------------------------------------------- #
def _run_plan(extra_env=None):
    env = dict(os.environ)
    if extra_env:
        env.update(extra_env)
    return subprocess.run(["bash", RUN_PAIR, "--plan"],
                          capture_output=True, text=True, env=env)


@pytest.mark.parametrize("spec,first,second", [
    ("head pinned", "head", "pinned"),
    ("pinned head", "pinned", "head"),
])
def test_arm_order_spec_override_honored(spec, first, second):
    """When CI passes ARM_ORDER_SPEC, run_pair.sh uses it verbatim (no second
    parity derivation — the UTC-midnight-straddle mislabeling fix)."""
    r = _run_plan({"ARM_ORDER_SPEC": spec})
    assert r.returncode == 0, r.stderr
    assert f"arm order       : {first} then {second}" in r.stdout
    assert "ARM_ORDER_SPEC (workflow-resolved" in r.stdout


def test_arm_order_spec_invalid_rejected():
    r = _run_plan({"ARM_ORDER_SPEC": "head head"})
    assert r.returncode != 0
    assert "invalid arm order" in r.stderr


def test_arm_order_fallback_without_spec():
    """Standalone (no ARM_ORDER_SPEC): parity fallback still resolves a valid
    order for whatever today is."""
    env = {k: v for k, v in os.environ.items() if k != "ARM_ORDER_SPEC"}
    r = subprocess.run(["bash", RUN_PAIR, "--plan"],
                       capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stderr
    assert ("arm order       : head then pinned" in r.stdout
            or "arm order       : pinned then head" in r.stdout)
    assert "standalone fallback" in r.stdout


# --------------------------------------------------------------------------- #
# run_id construction (contract §1.2)
# --------------------------------------------------------------------------- #
def _run_id(pair_id, arm, tier):
    return f"{pair_id}-{arm}-t{tier}"


def test_run_id_form():
    pair = "2026-07-07T04-15-32Z-91ac2dd"
    assert _run_id(pair, "head", 1) == "2026-07-07T04-15-32Z-91ac2dd-head-t1"
    assert _run_id(pair, "pinned", 0) == "2026-07-07T04-15-32Z-91ac2dd-pinned-t0"


def test_run_id_four_distinct_per_night():
    pair = "2026-07-07T04-15-32Z-91ac2dd"
    ids = {_run_id(pair, a, t) for a in ("head", "pinned") for t in (0, 1)}
    assert len(ids) == 4  # contract §1.2: 4 rows/night, all sharing pair_id
    assert all(i.startswith(pair) for i in ids)


# --------------------------------------------------------------------------- #
# config-echo runtime-map assembly (build_runtime_json in run_pair.sh)
# --------------------------------------------------------------------------- #
def _runtime_json_raw(arm, tier, extra_env=None):
    """Run the extracted build_runtime_json python body; return the process."""
    env = dict(os.environ)
    env.update({
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
    })
    # OUTCOME is owned by ingest_failed(); keep the baseline env clean so the
    # outcome-absent-on-success assertions cannot be polluted ambiently.
    env.pop("OUTCOME", None)
    if extra_env:
        env.update(extra_env)
    # invoke the function by sourcing run_pair.sh's python heredoc indirectly:
    # extract build_runtime_json's python and run it (it is self-contained).
    src = open(RUN_PAIR).read()
    m = re.search(r"build_runtime_json\(\).*?python3 - \"\$arm\" \"\$tier\" <<'PY'\n(.*?)\nPY",
                  src, re.S)
    assert m, "could not extract build_runtime_json python body"
    body = m.group(1)
    return subprocess.run([sys.executable, "-c", body, arm, tier],
                          capture_output=True, text=True, env=env)


def _build_runtime_json(arm, tier, extra_env=None):
    out = _runtime_json_raw(arm, tier, extra_env)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_runtime_map_scope_and_identity_keys():
    rt = _build_runtime_json("head", "1")
    assert rt["arm"] == "head"
    assert rt["tier"] == "1"
    assert rt["pair_id"] == "2026-07-07T04-15-32Z-91ac2dd"
    assert rt["target_region"] == "us-east-2"       # contract §1.1 mandatory
    assert rt["environment_class"] == "staging"     # contract §1.1 mandatory
    assert rt["compute_region"] == "us-east-2"       # directive c NEW key


def test_runtime_map_omits_warm_up():
    """OMIT the warm_up key (kafka has no priming step; absent => no warm-up)."""
    rt = _build_runtime_json("pinned", "0")
    assert "warm_up" not in rt


def test_runtime_map_shared_config_keys():
    rt = _build_runtime_json("head", "1")
    # contract §1.4 shared keys
    assert rt["batch_size"] == "25000"           # sink flush size = max.poll.records
    assert rt["write_parallelism"] == "3"        # tasks.max
    assert rt["async_insert"] == "0"
    assert rt["dataset"] == "hits"
    assert rt["partition_scheme"] == "toYear(EventDate)"  # = the Tier 1 DDL


def test_runtime_map_drops_empty_provenance():
    rt = _build_runtime_json("head", "1", extra_env={"PLUGIN_SHA256": ""})
    assert "plugin_sha256" not in rt  # empty dropped, not stored as ""


@pytest.mark.parametrize("missing", [
    "PAIR_ID", "TARGET_REGION", "ENVIRONMENT_CLASS", "COMPUTE_REGION",
])
def test_runtime_map_fails_on_empty_mandatory_key(missing):
    """Review F9: mandatory identity/scope keys HARD-FAIL when empty — they
    must never silently vanish from a runs row."""
    out = _runtime_json_raw("head", "1", extra_env={missing: ""})
    assert out.returncode != 0, f"{missing}='' must fail, got: {out.stdout}"
    assert "mandatory key" in out.stderr


def test_runtime_map_fails_on_empty_arm():
    out = _runtime_json_raw("", "1")
    assert out.returncode != 0
    assert "mandatory key" in out.stderr


def test_partition_scheme_matches_tier1_ddl():
    """The echoed partition_scheme must be the ACTUAL Tier 1 DDL partitioning
    (sql/clickbench/02_create_hits.sql), and run_pair.sh's default must match."""
    ddl = open(HITS_DDL).read()
    # ^-anchored so the header comment mentioning "PARTITION BY" is not matched.
    m = re.search(r"^PARTITION BY\s+(\S+)", ddl, re.M)
    assert m, "no PARTITION BY in the hits DDL"
    ddl_scheme = m.group(1)
    src = open(RUN_PAIR).read()
    m2 = re.search(r'CFG_PARTITION_SCHEME:-([^}]+)\}', src)
    assert m2, "no CFG_PARTITION_SCHEME default in run_pair.sh"
    assert m2.group(1) == ddl_scheme, (
        f"run_pair.sh default '{m2.group(1)}' != DDL '{ddl_scheme}'")


# --------------------------------------------------------------------------- #
# producer-summary JSON parsing (rows_expected from the last stdout line)
# --------------------------------------------------------------------------- #
def test_producer_summary_parse():
    summary = ('{"topic":"hits","partitions":3,"rows_sent":300,'
               '"rows_expected":300,"match":true}')
    log = "some noise\nmore noise\n" + summary
    last = log.splitlines()[-1]
    assert json.loads(last)["rows_expected"] == 300


def test_producer_summary_mismatch_detectable():
    summary = '{"rows_sent":301,"rows_expected":300,"match":false}'
    d = json.loads(summary)
    assert d["match"] is False  # orchestrator fails on producer exit!=0 (exit 2)


# --------------------------------------------------------------------------- #
# §6 config cross-check — every §6 value in BOTH the connector template AND the
# runtime-map echo (overseer self-verification: "cross-check EVERY §6 config
# value appears in both").
# --------------------------------------------------------------------------- #
def _connector_config():
    doc = json.load(open(CONNECTOR_TMPL))
    return doc["spec"]["config"], doc["spec"]


SECTION6 = {
    # (human name, value, connector-config assertion, runtime-key)
    "exactlyOnce": "false",
    # 25000 (was 100000, review F2): only live-proven pairing with 4096m heap;
    # TODO(#32) co-tunes poll+heap upward on the connect-ng m6i.xlarge.
    "max.poll.records": "25000",
    "max.partition.fetch.bytes": "104857600",
    "fetch.max.bytes": "209715200",
    "max.poll.interval.ms": "600000",
    "clickhouseClientInsertTimeoutMs": "180000",
    "tasks.max": "3",
}


def test_section6_in_connector_template():
    cfg, spec = _connector_config()
    assert cfg["exactlyOnce"] == "false"
    assert cfg["consumer.override.max.poll.records"] == "25000"
    assert cfg["consumer.override.max.partition.fetch.bytes"] == "104857600"
    assert cfg["consumer.override.fetch.max.bytes"] == "209715200"
    assert cfg["consumer.override.max.poll.interval.ms"] == "600000"
    assert cfg["clickhouseClientInsertTimeoutMs"] == "180000"
    assert spec["tasksMax"] == 3
    # async OFF by construction, pinned for provenance
    assert "async_insert=0" in cfg["clickhouseSettings"]
    assert "wait_end_of_query=1" in cfg["clickhouseSettings"]


def test_insert_timeout_below_poll_interval_with_margin():
    cfg, _ = _connector_config()
    insert_timeout = int(cfg["clickhouseClientInsertTimeoutMs"])
    poll_interval = int(cfg["consumer.override.max.poll.interval.ms"])
    assert insert_timeout < poll_interval, "insert timeout must be < poll interval (§6)"
    # documented margin: at least several minutes of headroom
    assert poll_interval - insert_timeout >= 300_000, "margin too small (§6)"


def test_section6_in_runtime_echo():
    rt = _build_runtime_json("head", "1")
    assert rt["exactlyOnce"] == "false"
    assert rt["consumer_max_poll_records"] == "25000"
    assert rt["consumer_max_partition_fetch_bytes"] == "104857600"
    assert rt["consumer_fetch_max_bytes"] == "209715200"
    assert rt["consumer_max_poll_interval_ms"] == "600000"
    assert rt["clickhouse_client_insert_timeout_ms"] == "180000"
    assert rt["write_parallelism"] == "3"  # tasks.max echoed
    assert rt["client_version"] == "V1"    # plan §6 decision 5
    assert "Avro" in rt["insert_format"]


def test_section6_values_agree_between_template_and_echo():
    """The two sources of §6 values (connector template + runtime echo) must
    carry IDENTICAL values — a drift would ship a config under test that the
    dashboard misrepresents."""
    cfg, spec = _connector_config()
    rt = _build_runtime_json("head", "1")
    assert cfg["consumer.override.max.poll.records"] == rt["consumer_max_poll_records"]
    assert cfg["consumer.override.max.partition.fetch.bytes"] == rt["consumer_max_partition_fetch_bytes"]
    assert cfg["consumer.override.fetch.max.bytes"] == rt["consumer_fetch_max_bytes"]
    assert cfg["consumer.override.max.poll.interval.ms"] == rt["consumer_max_poll_interval_ms"]
    assert cfg["clickhouseClientInsertTimeoutMs"] == rt["clickhouse_client_insert_timeout_ms"]
    assert str(spec["tasksMax"]) == rt["write_parallelism"]
    assert cfg["exactlyOnce"] == rt["exactlyOnce"]


# --------------------------------------------------------------------------- #
# run_cost_usd invocation point (review F6): once, end-of-pair, OUTSIDE the
# capture gate — never inside capture_and_record (which would charge ~half the
# node-hours mid-pair).
# --------------------------------------------------------------------------- #
def _extract_function(src, name):
    m = re.search(rf"^{re.escape(name)}\(\) \{{\n(.*?)^\}}", src, re.S | re.M)
    assert m, f"could not extract function {name} from run_pair.sh"
    return m.group(1)


def test_run_cost_not_in_capture_and_record():
    src = open(RUN_PAIR).read()
    body = _extract_function(src, "capture_and_record")
    assert "emit_run_cost" not in body, \
        "run_cost must not be charged inside the per-run capture gate (F6)"


def test_run_cost_emitted_end_of_pair_on_first_arm_tier1():
    src = open(RUN_PAIR).read()
    body = _extract_function(src, "phase_pair_cost")
    assert "emit_run_cost.py" in body
    assert '-t1"' in body, "pair cost must land on the first-run arm's TIER-1 row"
    # main() order: both phase_arm calls precede phase_pair_cost.
    main_body = _extract_function(src, "main")
    arm1 = main_body.index('phase_arm "${ARM_ORDER[0]}"')
    arm2 = main_body.index('phase_arm "${ARM_ORDER[1]}"')
    cost = main_body.index("phase_pair_cost")
    assert arm1 < arm2 < cost, "pair cost must be emitted AFTER both arms (F6)"


# --------------------------------------------------------------------------- #
# KafkaConnect CR wiring (review F1) + exporter -rate rule (review F12)
# --------------------------------------------------------------------------- #
def test_kafkaconnect_cr_registers_env_config_provider():
    """F1: without config.providers=env the worker never resolves ${env:CH_*}
    and the sink would receive the literal placeholder as its hostname."""
    tmpl = open(KAFKACONNECT_TMPL).read()
    doc = yaml.safe_load(tmpl.replace("${ARM_IMAGE}", "img")
                             .replace("${CONNECT_HEAP}", "2048m"))
    cfg = doc["spec"]["config"]
    assert cfg.get("config.providers") == "env"
    assert (cfg.get("config.providers.env.class")
            == "org.apache.kafka.common.config.provider.EnvVarConfigProvider")


def test_rate_exporter_rule_not_overmatching():
    """F12: client-id must be ([^,]+), not (.*) — the greedy form also matches
    the per-topic MBean and ~doubles the summed records_consumed_rate."""
    cm = yaml.safe_load(open(METRICS_CM))
    rules = yaml.safe_load(cm["data"]["metrics-config.yml"])["rules"]
    # The benchmark-owned rule is the one whose ATTRIBUTE capture is (.+-rate);
    # the stock rules only mention compression-rate inside alternations.
    rate_rules = [r for r in rules if "(.+-rate)" in r.get("pattern", "")]
    assert len(rate_rules) == 1, "expected exactly one (.+-rate) rule"
    pat = rate_rules[0]["pattern"]
    assert "client-id=([^,]+)" in pat, pat
    assert "client-id=(.*)" not in pat, "over-matching client-id capture (F12)"


# --------------------------------------------------------------------------- #
# Wait audit (live fix): every wait must have a FAILURE-side exit, not just a
# success condition + a long timeout. The wait logic itself is kubectl-driven
# (not unit-executable offline), so these structural tests pin the shape.
# --------------------------------------------------------------------------- #
def test_producer_wait_watches_both_terminal_conditions():
    """Live fix: a one-sided `kubectl wait --for=condition=complete` cannot see
    a Failed job (terminal with backoffLimit=0) and burned cluster-hours toward
    the 6h PRODUCER_TIMEOUT. The wait must poll BOTH terminal conditions."""
    src = open(RUN_PAIR).read()
    body = _extract_function(src, "phase_preload")
    assert "Complete=True" in body, "producer wait must watch Complete"
    assert "Failed=True" in body, "producer wait must watch Failed"
    # the one-sided wait must not come back as CODE (comments may describe it)
    code = "\n".join(l for l in body.splitlines()
                     if not l.strip().startswith("#"))
    assert "--for=condition=complete" not in code, \
        "one-sided kubectl wait must not come back"
    # Failed path must be diagnosable: dump the pod's last log lines.
    assert "logs job/hits-producer" in body


def test_wait_tasks_running_fast_fails_on_failed():
    """Same one-sided-wait class: a FAILED connector/task is terminal (Connect
    does not auto-restart FAILED; errors.tolerance=none) — die fast, don't
    burn the RUNNING-wait timeout."""
    src = open(RUN_PAIR).read()
    body = _extract_function(src, "wait_tasks_running")
    assert "FAILED" in body, "must inspect the FAILED state"
    assert "fail_run" in body, "FAILED must fail the run (with rollback)"
    # both the connector state and the per-task failed count are checked
    assert "failed" in body and "'FAILED'" in body


def test_no_unbounded_wait_true_deletes():
    """Unbounded variant of the same class: `kubectl delete --wait=true`
    without --timeout blocks forever on a stuck finalizer."""
    src = open(RUN_PAIR).read()
    for i, line in enumerate(src.splitlines(), 1):
        if "kubectl" in line and " delete " in line and "--wait=true" in line:
            assert "--timeout=" in line, \
                f"run_pair.sh:{i}: --wait=true delete without --timeout: {line.strip()}"


# --------------------------------------------------------------------------- #
# DIGEST-PINNED image deployment by default (stale-tag class fix).
#
# The live failure: mutable tags were served STALE twice during the first pair
# (node/registry cache), so the two arms silently ran the wrong image. run_pair.sh
# now validates every deployed image ref is a digest (repo@sha256:...): a digest
# is accepted; a mutable tag is either resolved to a digest or hard-fails, unless
# KAFKA_ALLOW_TAG=1 / --allow-tag (local-hacking escape hatch).
#
# validate_image_ref / resolve_tag_to_digest are sourced from run_pair.sh (its
# `main` runs only under direct execution, not `source`) and exercised directly.
# A registry that is neither ECR nor ghcr cannot be resolved offline, so it is
# the clean way to test the STRICT-reject path with no network.
# --------------------------------------------------------------------------- #
def _validate_image_ref(ref, allow_tag=False, extra_env=None):
    """Source run_pair.sh and call validate_image_ref; return (rc, stdout, stderr)."""
    env = dict(os.environ)
    env["KAFKA_ALLOW_TAG"] = "1" if allow_tag else "0"
    if extra_env:
        env.update(extra_env)
    # Use a non-ECR/non-ghcr registry so the strict path cannot resolve via a
    # network call — it must reject on shape alone (deterministic, offline).
    script = f'source "{RUN_PAIR}"; validate_image_ref TESTVAR "{ref}"'
    r = subprocess.run(["bash", "-c", script],
                       capture_output=True, text=True, env=env)
    return r.returncode, r.stdout.strip(), r.stderr


def test_digest_ref_accepted_strict():
    """A digest reference is accepted unchanged in strict (default) mode."""
    ref = "ghcr.io/clickhouse/clickhouse-kafka-connect@sha256:" + "a" * 64
    rc, out, _ = _validate_image_ref(ref, allow_tag=False)
    assert rc == 0
    assert out == ref  # echoed unchanged


def test_mutable_tag_rejected_by_default():
    """A mutable tag on an unresolvable registry HARD-FAILS in strict mode —
    the stale-tag class must never silently deploy a tag."""
    rc, out, err = _validate_image_ref(
        "registry.example.com/team/connect-bench:benchmark-head-abc123",
        allow_tag=False)
    assert rc != 0, f"expected reject, got rc={rc} out={out!r}"
    assert "MUTABLE TAG" in err
    assert "stale-tag" in err.lower()
    assert out == ""  # nothing echoed on failure


def test_mutable_tag_accepted_with_escape_hatch():
    """KAFKA_ALLOW_TAG=1 (== --allow-tag) lets a mutable tag through for local
    hacking, but WARNS loudly and echoes the tag unchanged."""
    ref = "registry.example.com/team/connect-bench:benchmark-head-abc123"
    rc, out, err = _validate_image_ref(ref, allow_tag=True)
    assert rc == 0
    assert out == ref
    assert "KAFKA_ALLOW_TAG=1" in err
    assert "escape hatch" in err.lower()


def test_empty_ref_rejected():
    rc, out, err = _validate_image_ref("", allow_tag=False)
    assert rc != 0
    assert "empty" in err


def test_allow_tag_flag_sets_escape_hatch():
    """--allow-tag must map to KAFKA_ALLOW_TAG=1 (the escape hatch) in main()."""
    src = open(RUN_PAIR).read()
    assert "--allow-tag) export KAFKA_ALLOW_TAG=1" in src


def test_run_pair_validates_all_three_images_before_phases():
    """run_pair.sh must validate ARM0/ARM1/PRODUCER images and it must happen
    BEFORE any cluster-mutating phase (no stale image ever reaches a deploy)."""
    src = open(RUN_PAIR).read()
    body = _extract_function(src, "main")
    for var in ("ARM0_IMAGE", "ARM1_IMAGE", "PRODUCER_IMAGE"):
        assert f"validate_image_ref {var}" in body, f"{var} not validated in main()"
    v = body.index("validate_image_ref ARM0_IMAGE")
    up = body.index("phase_scale_up")
    assert v < up, "images must be validated before phase_scale_up"


def test_workflow_outputs_are_digest_refs():
    """benchmark-images.yml workflow_call outputs must be the resolved DIGESTS,
    not the mutable tags (the stale-tag class fix at the source)."""
    wf = os.path.abspath(os.path.join(ORCH, "..", "..", "..",
                                      ".github", "workflows", "benchmark-images.yml"))
    doc = yaml.safe_load(open(wf))
    # YAML 1.1 parses the bare `on:` key as boolean True (the well-known gotcha).
    on = doc.get("on", doc.get(True))
    outs = on["workflow_call"]["outputs"]
    assert "head_image" in outs and "pinned_image" in outs
    text = open(wf).read()
    # export step sets outputs from the per-step .digest outputs, and each build
    # step passes --push --digest-out and asserts the @sha256: shape.
    assert "head_image=${{ steps.head.outputs.digest }}" in text
    assert "pinned_image=${{ steps.pinned.outputs.digest }}" in text
    assert "--push --digest-out" in text
    assert text.count("*@sha256:*)") >= 2  # both build steps assert digest shape


def test_build_arm_push_resolves_and_emits_digest():
    """build-arm.sh --push must resolve the registry digest and emit the
    machine-readable IMAGE_DIGEST=... line (and fail if it cannot resolve)."""
    ba = os.path.abspath(os.path.join(ORCH, "..", "docker", "build-arm.sh"))
    src = open(ba).read()
    assert "--push)" in src and "--digest-out)" in src
    assert "resolve_image_digest" in src
    assert 'echo "IMAGE_DIGEST=${IMAGE_DIGEST_REF}"' in src
    assert "RepoDigests" in src  # docker inspect digest resolution
    # push path must FAIL LOUD (exit 1) if the digest cannot be resolved.
    assert "could not resolve its registry digest" in src


# --------------------------------------------------------------------------- #
# outcome='failed' capture-on-failure semantics (contract §1.3 amendment):
# failed runs are FULLY captured + exported, marked — no survivorship bias.
# Success rows OMIT the key (absent => success, Map semantics).
# --------------------------------------------------------------------------- #
def test_runtime_map_outcome_absent_on_success():
    """Success path: outcome MUST be absent — dashboards distinguish failed
    runs by outcome VALUE, and legacy rows never carried the key."""
    rt = _build_runtime_json("head", "1")
    assert "outcome" not in rt


def test_runtime_map_outcome_failed_present():
    rt = _build_runtime_json("head", "1", extra_env={"OUTCOME": "failed"})
    assert rt["outcome"] == "failed"


def test_runtime_map_outcome_success_rejected():
    """Writing outcome='success' explicitly is prohibited (absent => success);
    any value other than 'failed' is a hard error."""
    out = _runtime_json_raw("head", "1", extra_env={"OUTCOME": "success"})
    assert out.returncode != 0
    assert "OMIT the outcome key" in out.stderr


def test_poller_sample_no_longer_rolls_back():
    """Post-drain-start failures are the caller's decision now: the sampler
    returns 0 (drained) / 2 (timeout — ingest incomplete) / 1 (hard failure)
    and never rolls back or dies itself."""
    src = open(RUN_PAIR).read()
    body = _extract_function(src, "run_poller_sample")
    assert "fail_run" not in body, "sampler must not roll back — the caller routes to ingest_failed"
    assert "return 2" in body and "return 1" in body and "return 0" in body


def test_ingest_failed_marks_and_captures():
    src = open(RUN_PAIR).read()
    body = _extract_function(src, "ingest_failed")
    assert 'export OUTCOME="failed"' in body
    assert 'capture_and_record "${arm}" "${tier}" "failed"' in body
    assert "PAIR_HAD_FAILURE=1" in body
    assert "unset OUTCOME" in body  # success runs must never inherit it
    # no rollback on the failed-class path — the evidence must survive
    assert "rollback_run_metrics" not in body


def test_phase_arm_routes_failures_to_ingest_failed():
    """Both tiers must branch: success -> capture_and_record (strict), failure
    -> ingest_failed. The drain-start boundary is the poller sample."""
    src = open(RUN_PAIR).read()
    body = _extract_function(src, "phase_arm")
    # one CALL per tier (count call sites, not comment mentions)
    assert body.count('ingest_failed "${arm}"') == 2
    assert 'ingest_failed "${arm}" "0"' in body
    assert 'ingest_failed "${arm}" "1"' in body
    # per-run flag/settle state reset (no bleed onto a failed-class run)
    assert body.count('FLAGGED=0; FLAG_REASON=""') == 2


def test_capture_and_record_failed_mode_semantics():
    src = open(RUN_PAIR).read()
    body = _extract_function(src, "capture_and_record")
    # mode param exists, defaulting strict
    assert 'mode="${3:-strict}"' in body
    # failed mode skips SQL 20 (no integrity claim on a failed run)
    assert '[ "${mode}" = "failed" ]' in body
    # failed-class tier 1 is integrity_unverified BY DEFINITION
    assert 'append_flag "integrity_unverified"' in body
    # per-file capture failure on the failed path continues (no rollback-die)
    assert "partial evidence beats none" in body
    # integrity verdict runs on the STRICT path only
    assert '[ "${mode}" = "strict" ] && [ "${tier}" = "1" ]' in body


def test_pair_continues_and_exits_nonzero_on_failure():
    """Pair-level policy: a failed-class run never aborts the pair (the other
    runs are still captured), but main() exits non-zero at the very end."""
    src = open(RUN_PAIR).read()
    main_body = _extract_function(src, "main")
    assert 'if [ "${PAIR_HAD_FAILURE}" = "1" ]' in main_body
    assert "exit 1" in main_body
    # the failure exit comes AFTER cleanup (teardown + scale down)
    assert main_body.index("phase_scale_down") < main_body.index('PAIR_HAD_FAILURE}" = "1"')
    # ambient OUTCOME must be cleared before any run
    assert "unset OUTCOME" in main_body


# --------------------------------------------------------------------------- #
# Pair-cost §1.2 join-rule gate: run_cost_usd only attaches to a run_id whose
# perf.runs row actually landed (no orphaned metrics for rolled-back records).
# --------------------------------------------------------------------------- #
def test_capture_and_record_tracks_recorded_runs():
    src = open(RUN_PAIR).read()
    body = _extract_function(src, "capture_and_record")
    append = 'RECORDED_RUNS="${RECORDED_RUNS} ${RUN_ID}"'
    assert append in body
    # the append happens AFTER the insert_run_record gate, BEFORE the export
    assert body.index("insert_run_record.py") < body.index(append) \
        < body.index("export_metrics_to_dwh.py")


def test_pair_cost_skipped_when_target_row_never_landed():
    """phase_pair_cost must consult RECORDED_RUNS and skip-with-loud-warn (no
    silent re-anchor to the other arm) when the first-arm t1 row was rolled
    back — an orphaned run_cost_usd would violate the §1.2 join rule."""
    src = open(RUN_PAIR).read()
    body = _extract_function(src, "phase_pair_cost")
    assert "RECORDED_RUNS" in body
    assert "PHASE 3c SKIPPED" in body
    assert "return 0" in body            # skip, not die (job already red)
    assert "re-anchor" in body           # the no-silent-re-anchor decision is documented
    # the gate sits BEFORE the emit
    assert body.index("RECORDED_RUNS") < body.index("emit_run_cost.py")


def test_pair_cost_gate_matching_logic():
    """Exercise the exact case-pattern used by phase_pair_cost: substring-safe
    matching on the space-delimited RECORDED_RUNS list."""
    script = '''
RECORDED_RUNS="$1"; cost_run_id="$2"
case " ${RECORDED_RUNS} " in
  *" ${cost_run_id} "*) echo EMIT ;;
  *) echo SKIP ;;
esac
'''
    def gate(recorded, target):
        return subprocess.run(["bash", "-c", script, "bash", recorded, target],
                              capture_output=True, text=True).stdout.strip()
    pair = "2026-07-09T04-15-32Z-abc1234"
    assert gate(f"{pair}-head-t0 {pair}-head-t1", f"{pair}-head-t1") == "EMIT"
    assert gate(f"{pair}-head-t0", f"{pair}-head-t1") == "SKIP"
    # substring safety: a longer id containing the target must not match
    assert gate(f"{pair}-head-t1-extra", f"{pair}-head-t1") == "SKIP"
    assert gate("", f"{pair}-head-t1") == "SKIP"


# --------------------------------------------------------------------------- #
# Instrument runtime keys (kafka-namespaced, deployed truth) + CPU gate.
# --------------------------------------------------------------------------- #
INSTRUMENT_ENV = {
    "KAFKA_COMPUTE_INSTANCE_TYPE": "m6i.xlarge",
    "KAFKA_WORKER_CPU_REQUEST": "3",
    "KAFKA_WORKER_CPU_LIMIT": "4",
    "KAFKA_WORKER_MEM_REQUEST": "5Gi",
    "KAFKA_WORKER_MEM_LIMIT": "6Gi",
}


def test_runtime_map_instrument_keys_present():
    rt = _build_runtime_json("head", "1", extra_env=INSTRUMENT_ENV)
    assert rt["kafka_compute_instance_type"] == "m6i.xlarge"
    assert rt["kafka_worker_cpu_request"] == "3"
    assert rt["kafka_worker_cpu_limit"] == "4"
    assert rt["kafka_worker_mem_request"] == "5Gi"
    assert rt["kafka_worker_mem_limit"] == "6Gi"


def test_runtime_map_instrument_keys_absent_when_unread():
    """Unreadable deployed values must be ABSENT (not empty strings)."""
    rt = _build_runtime_json("head", "1")
    for k in ("kafka_compute_instance_type", "kafka_worker_cpu_request",
              "kafka_worker_cpu_limit", "kafka_worker_mem_request",
              "kafka_worker_mem_limit"):
        assert k not in rt


def test_runtime_map_cpu_share_tier0_only():
    """kafka_worker_cpu_share_t0 rides the TIER-0 row only (arm-scoped value,
    but a t1 row carrying a t0 gate number would be misleading)."""
    env = dict(INSTRUMENT_ENV, KAFKA_WORKER_CPU_SHARE_T0="0.6100")
    rt0 = _build_runtime_json("head", "0", extra_env=env)
    rt1 = _build_runtime_json("head", "1", extra_env=env)
    assert rt0["kafka_worker_cpu_share_t0"] == "0.6100"
    assert "kafka_worker_cpu_share_t0" not in rt1
    # and absent on tier 0 when the gate could not compute
    rt0_none = _build_runtime_json("head", "0", extra_env=INSTRUMENT_ENV)
    assert "kafka_worker_cpu_share_t0" not in rt0_none


def _cpu_gate_share(finalize_doc, limit):
    """Run the extracted compute_cpu_gate_t0 python body against a synthetic
    finalize JSON; returns the printed share string ('' = unavailable)."""
    import tempfile
    src = open(RUN_PAIR).read()
    m = re.search(
        r'compute_cpu_gate_t0\(\).*?python3 - "\$\{fin\}" "\$\{KAFKA_WORKER_CPU_LIMIT:-\}" <<\'PY\'\n(.*?)\nPY',
        src, re.S)
    assert m, "could not extract compute_cpu_gate_t0 python body"
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(finalize_doc, f)
        path = f.name
    out = subprocess.run([sys.executable, "-c", m.group(1), path, limit],
                         capture_output=True, text=True)
    os.unlink(path)
    assert out.returncode == 0, out.stderr
    return out.stdout.strip()


def test_cpu_gate_math():
    # cpu_seconds = 12 s/Mrows * 10 Mrows = 120 CPU-s over a 60 s drain with a
    # 4-core limit => 120 / (60*4) = 0.5
    doc = {"rows_expected": 10_000_000,
           "scalars": {"connect_cpu_seconds_per_Mrows": 12.0,
                       "drain_seconds": 60.0}}
    assert _cpu_gate_share(doc, "4") == "0.5000"
    # millicore limit form: 4000m == 4 cores
    assert _cpu_gate_share(doc, "4000m") == "0.5000"
    # saturated instrument: 240 CPU-s / (60*1 core) = 4.0 (prints SUSPECT)
    doc2 = {"rows_expected": 10_000_000,
            "scalars": {"connect_cpu_seconds_per_Mrows": 24.0,
                        "drain_seconds": 60.0}}
    assert _cpu_gate_share(doc2, "1000m") == "4.0000"


def test_cpu_gate_unavailable_paths():
    base = {"rows_expected": 10_000_000,
            "scalars": {"connect_cpu_seconds_per_Mrows": 12.0,
                        "drain_seconds": 60.0}}
    # cadvisor unwired -> CPU scalar is None -> unavailable
    doc = json.loads(json.dumps(base))
    doc["scalars"]["connect_cpu_seconds_per_Mrows"] = None
    assert _cpu_gate_share(doc, "4") == ""
    # no limit readable -> unavailable
    assert _cpu_gate_share(base, "") == ""
    # zero drain -> unavailable (no division blow-up)
    doc2 = json.loads(json.dumps(base))
    doc2["scalars"]["drain_seconds"] = 0
    assert _cpu_gate_share(doc2, "4") == ""


def test_cpu_gate_wired_after_tier0_finalize_and_log_only():
    src = open(RUN_PAIR).read()
    body = _extract_function(src, "phase_arm")
    # gate runs on the tier-0 SUCCESS path, after finalize, before capture
    assert body.index("finalize_and_insert_metrics 0") \
        < body.index("compute_cpu_gate_t0") \
        < body.index('capture_and_record "${arm}" "0"')
    # per-run reset so arm 2 never inherits arm 1's share
    assert 'KAFKA_WORKER_CPU_SHARE_T0=""' in body
    # verdict is LOG-ONLY: the gate function must not flag or fail the run
    gate = _extract_function(src, "compute_cpu_gate_t0")
    assert "INSTRUMENT_RESIZE_SUSPECT" in gate and "PASS" in gate
    assert "append_flag" not in gate and "fail_run" not in gate and "die " not in gate
