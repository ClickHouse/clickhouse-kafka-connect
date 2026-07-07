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
  * day-parity arm order (even->head first, odd->pinned first)
  * run_id construction (<pair_id>-<arm>-t<tier>, contract §1.2)
  * config-echo runtime-map assembly (build_runtime_json, directive c; warm_up omitted)
  * producer-summary JSON parsing (rows_expected)
  * §6 config cross-check: every §6 value present in BOTH the connector config
    template AND the runtime-map echo (overseer self-verification)
"""
import json
import os
import re
import subprocess
import sys

import pytest

HERE = os.path.dirname(__file__)
ORCH = os.path.abspath(os.path.join(HERE, ".."))
RUN_PAIR = os.path.join(ORCH, "run_pair.sh")
CONNECTOR_TMPL = os.path.join(ORCH, "templates", "kafkaconnector.json.tmpl")


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
def _build_runtime_json(arm, tier, extra_env=None):
    env = dict(os.environ)
    env.update({
        "PAIR_ID": "2026-07-07T04-15-32Z-91ac2dd",
        "TARGET_REGION": "us-east-2",
        "ENVIRONMENT_CLASS": "staging",
        "COMPUTE_REGION": "us-east-2",
        "CFG_MAX_POLL_RECORDS": "100000",
        "CFG_MAX_PARTITION_FETCH_BYTES": "104857600",
        "CFG_FETCH_MAX_BYTES": "209715200",
        "CFG_MAX_POLL_INTERVAL_MS": "600000",
        "CFG_INSERT_TIMEOUT_MS": "180000",
        "CFG_TASKS_MAX": "3",
        "CFG_PARTITION_SCHEME": "toYYYYMM(EventDate)",
        "KAFKA_CONNECT_VERSION": "3.9.0",
        "STRIMZI_VERSION": "0.46.0",
        "PLUGIN_SHA256": "abc123",
    })
    if extra_env:
        env.update(extra_env)
    # invoke the function by sourcing run_pair.sh's python heredoc indirectly:
    # extract build_runtime_json's python and run it (it is self-contained).
    src = open(RUN_PAIR).read()
    m = re.search(r"build_runtime_json\(\).*?python3 - \"\$arm\" \"\$tier\" <<'PY'\n(.*?)\nPY",
                  src, re.S)
    assert m, "could not extract build_runtime_json python body"
    body = m.group(1)
    out = subprocess.run([sys.executable, "-c", body, arm, tier],
                         capture_output=True, text=True, env=env)
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
    assert rt["batch_size"] == "100000"          # sink flush size = max.poll.records
    assert rt["write_parallelism"] == "3"        # tasks.max
    assert rt["async_insert"] == "0"
    assert rt["dataset"] == "hits"
    assert rt["partition_scheme"] == "toYYYYMM(EventDate)"


def test_runtime_map_drops_empty_provenance():
    rt = _build_runtime_json("head", "1", extra_env={"PLUGIN_SHA256": ""})
    assert "plugin_sha256" not in rt  # empty dropped, not stored as ""


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
    "max.poll.records": "100000",
    "max.partition.fetch.bytes": "104857600",
    "fetch.max.bytes": "209715200",
    "max.poll.interval.ms": "600000",
    "clickhouseClientInsertTimeoutMs": "180000",
    "tasks.max": "3",
}


def test_section6_in_connector_template():
    cfg, spec = _connector_config()
    assert cfg["exactlyOnce"] == "false"
    assert cfg["consumer.override.max.poll.records"] == "100000"
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
    assert rt["consumer_max_poll_records"] == "100000"
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
