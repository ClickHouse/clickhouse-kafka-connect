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
"""Offline unit tests for the chaos (#771) connector template + render logic.

No cluster, no creds, no network. Covers (task T7, spec §3.2 + §8):
  * the chaos KafkaConnector CR template pins the identities/config §3.2 requires
  * render_connector.py renders both delivery modes (eo=0/eo=1) and IPWB, and the
    default (pair) path is UNCHANGED
  * validate_chaos_config() enforces §3.2 (mirrors ClickHouseSinkTask.java:54-62
    and :150) — every rejection has a fail-then-pass pair, plus the WATCH-cell
    carve-out
  * a spelling-drift guard: every connector-owned key the chaos template pins
    exists verbatim in ClickHouseSinkConfig.java (a typo here would silently
    deploy an ignored config)
"""
import importlib.util
import json
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(__file__)
ORCH = os.path.abspath(os.path.join(HERE, "..", "..", "orchestration"))
RENDER = os.path.join(ORCH, "render_connector.py")
PAIR_TMPL = os.path.join(ORCH, "templates", "kafkaconnector.json.tmpl")
CHAOS_TMPL = os.path.abspath(os.path.join(HERE, "..", "templates",
                                          "chaos-connector.json.tmpl"))
SINK_CONFIG = os.path.abspath(os.path.join(
    HERE, "..", "..", "..", "..",
    "src", "main", "java", "com", "clickhouse", "kafka", "connect",
    "sink", "ClickHouseSinkConfig.java"))


# --------------------------------------------------------------------------- #
# Import render_connector as a module so validate_chaos_config can be unit-tested
# as the pure function it is (no subprocess round-trip for the logic tests).
# --------------------------------------------------------------------------- #
def _load_render_module():
    spec = importlib.util.spec_from_file_location("render_connector", RENDER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


RC = _load_render_module()


def _render(args):
    return subprocess.run([sys.executable, RENDER] + args,
                          capture_output=True, text=True)


def _render_chaos(exactly_once, ipwb=None, watch_cell=False, group="ch-sink-chaos"):
    args = ["--template", CHAOS_TMPL, "--exactly-once", str(exactly_once),
            "--group", group, "--out", "-"]
    if ipwb is not None:
        args += ["--ipwb", ipwb]
    if watch_cell:
        args += ["--watch-cell"]
    return _render(args)


def _cfg_of(stdout):
    doc = json.loads(stdout)
    return doc, doc["spec"]["config"]


# --------------------------------------------------------------------------- #
# validate_chaos_config — pure function, §3.2. Every rejection fail-then-pass.
# --------------------------------------------------------------------------- #
def test_validate_accepts_pinned_defaults_both_modes():
    """The pinned combo (bufferCount=0, ipwb=false) is valid in BOTH modes."""
    RC.validate_chaos_config(exactly_once=True, ipwb=False)
    RC.validate_chaos_config(exactly_once=False, ipwb=False)


def test_validate_rejects_buffercount_with_exactly_once():
    """bufferCount>0 + exactlyOnce throws at task start
    (ClickHouseSinkTask.java:54-62) — the renderer must reject it first."""
    # fail
    with pytest.raises(ValueError) as e:
        RC.validate_chaos_config(exactly_once=True, ipwb=False, buffer_count=1)
    assert "bufferCount" in str(e.value)
    # pass — bufferCount=0 with exactly-once is fine
    RC.validate_chaos_config(exactly_once=True, ipwb=False, buffer_count=0)


def test_validate_allows_buffercount_in_at_least_once():
    """The Java guard only fires under exactly-once; at-least-once permits it."""
    RC.validate_chaos_config(exactly_once=False, ipwb=False, buffer_count=1)


def test_validate_rejects_ipwb_true_with_exactly_once():
    """ignorePartitionsWhenBatching=true is SILENTLY IGNORED under exactlyOnce
    (ClickHouseSinkTask.java:150) — a run would test a config its runtime keys
    do not reflect. Reject loudly."""
    # fail
    with pytest.raises(ValueError) as e:
        RC.validate_chaos_config(exactly_once=True, ipwb=True)
    assert "ignorePartitionsWhenBatching" in str(e.value)
    # pass — ipwb false under exactly-once
    RC.validate_chaos_config(exactly_once=True, ipwb=False)


def test_validate_rejects_ipwb_true_at_least_once_without_watch_cell():
    """IPWB=true is a WATCH-only cell (§3.2): permitted in at-least-once ONLY
    with the explicit watch-cell flag."""
    # fail — at-least-once, ipwb true, no watch cell
    with pytest.raises(ValueError) as e:
        RC.validate_chaos_config(exactly_once=False, ipwb=True, watch_cell=False)
    assert "watch" in str(e.value).lower()
    # pass — the carve-out
    RC.validate_chaos_config(exactly_once=False, ipwb=True, watch_cell=True)


def test_validate_watch_cell_does_not_rescue_exactly_once():
    """--watch-cell cannot unlock IPWB under exactly-once (still silently
    ignored) — the exactly-once rejection wins."""
    with pytest.raises(ValueError):
        RC.validate_chaos_config(exactly_once=True, ipwb=True, watch_cell=True)


# --------------------------------------------------------------------------- #
# Rendering — chaos mode, both delivery columns (§3.2), assert emitted CR.
# --------------------------------------------------------------------------- #
def test_render_chaos_exactly_once_emits_expected_cr():
    r = _render_chaos(1, group="ch-sink-chaos-eo1")
    assert r.returncode == 0, r.stderr
    doc, cfg = _cfg_of(r.stdout)
    assert doc["metadata"]["name"] == "chaos-clickhouse-sink"
    assert cfg["topics"] == "hits-chaos"
    assert cfg["topic2TableMap"] == "hits-chaos=hits_chaos"
    assert cfg["database"] == "clickbench"
    assert cfg["port"] == "8123"
    assert cfg["ssl"] == "false"
    assert cfg["exactlyOnce"] == "true"           # eo=1 -> "true"
    assert cfg["zkPath"] == "/kafka-connect-chaos"
    assert cfg["zkDatabase"] == "connect_state_chaos"
    assert cfg["keeperOnCluster"] == ""
    assert cfg["tolerateStateMismatch"] == "false"
    assert cfg["bufferCount"] == "0"
    assert cfg["ignorePartitionsWhenBatching"] == "false"
    # DLQ pins (make §3.6b depth meaningful)
    assert cfg["errors.tolerance"] == "all"
    assert cfg["errors.deadletterqueue.topic.name"] == "hits-chaos-dlq"
    assert cfg["errors.deadletterqueue.topic.replication.factor"] == "1"
    assert cfg["errors.deadletterqueue.context.headers.enable"] == "true"
    # consumer group set from --group
    assert cfg["consumer.override.group.id"] == "ch-sink-chaos-eo1"
    # no leftover render placeholders in the emitted CR
    assert "${EXACTLY_ONCE}" not in r.stdout
    assert "${IPWB}" not in r.stdout
    # doc keys stripped
    assert not any(k.startswith("//") for k in cfg)


def test_render_chaos_at_least_once_sets_false():
    r = _render_chaos(0, group="ch-sink-chaos-eo0")
    assert r.returncode == 0, r.stderr
    _, cfg = _cfg_of(r.stdout)
    assert cfg["exactlyOnce"] == "false"          # eo=0 -> "false"
    assert cfg["ignorePartitionsWhenBatching"] == "false"  # default


def test_render_chaos_ipwb_watch_cell():
    """The WATCH cell: at-least-once + ipwb=true + --watch-cell renders true."""
    r = _render_chaos(0, ipwb="true", watch_cell=True)
    assert r.returncode == 0, r.stderr
    _, cfg = _cfg_of(r.stdout)
    assert cfg["ignorePartitionsWhenBatching"] == "true"
    assert cfg["exactlyOnce"] == "false"


def test_render_chaos_env_creds_untouched():
    """${env:CH_*} placeholders are worker-resolved — never substituted here."""
    r = _render_chaos(1)
    assert r.returncode == 0, r.stderr
    _, cfg = _cfg_of(r.stdout)
    assert cfg["hostname"] == "${env:CH_HOSTNAME}"
    assert cfg["username"] == "${env:CH_USERNAME}"
    assert cfg["password"] == "${env:CH_PASSWORD}"


# --------------------------------------------------------------------------- #
# Rendering — the renderer runs validate_chaos_config and REFUSES a bad combo.
# --------------------------------------------------------------------------- #
def test_render_rejects_ipwb_true_with_exactly_once():
    r = _render_chaos(1, ipwb="true")
    assert r.returncode != 0, r.stdout
    assert "ignorePartitionsWhenBatching" in r.stderr


def test_render_rejects_ipwb_true_at_least_once_without_watch_cell():
    r = _render_chaos(0, ipwb="true")
    assert r.returncode != 0, r.stdout
    assert "watch" in r.stderr.lower()


def test_render_accepts_ipwb_true_watch_cell():
    r = _render_chaos(0, ipwb="true", watch_cell=True)
    assert r.returncode == 0, r.stderr


# --------------------------------------------------------------------------- #
# Default (pair) path UNCHANGED — no --exactly-once => existing behavior.
# --------------------------------------------------------------------------- #
def test_render_pair_path_unchanged():
    r = _render(["--template", PAIR_TMPL, "--ch-table", "hits",
                 "--group", "bench-head-t1", "--out", "-"])
    assert r.returncode == 0, r.stderr
    doc, cfg = _cfg_of(r.stdout)
    assert doc["metadata"]["name"] == "bench-clickhouse-sink"
    assert cfg["topic2TableMap"] == "hits=hits"        # ${CH_TABLE} -> hits
    assert cfg["consumer.override.group.id"] == "bench-head-t1"
    assert cfg["exactlyOnce"] == "false"
    assert cfg["errors.tolerance"] == "none"           # pair pins none, not all


def test_render_pair_path_tier0_table():
    r = _render(["--template", PAIR_TMPL, "--ch-table", "hits_null",
                 "--group", "g", "--out", "-"])
    assert r.returncode == 0, r.stderr
    _, cfg = _cfg_of(r.stdout)
    assert cfg["topic2TableMap"] == "hits=hits_null"


# --------------------------------------------------------------------------- #
# Spelling-drift guard: every connector-OWNED key the chaos template pins must
# exist verbatim in ClickHouseSinkConfig.java (a typo => silently-ignored key).
# errors.* are Kafka-Connect framework keys (not in the sink config) — excluded.
# --------------------------------------------------------------------------- #
CONNECTOR_OWNED_KEYS = [
    "hostname", "port", "database", "ssl",
    "topic2TableMap", "exactlyOnce", "clickhouseSettings",
    "zkPath", "zkDatabase", "keeperOnCluster",
    "tolerateStateMismatch", "bufferCount", "ignorePartitionsWhenBatching",
]


def test_pinned_keys_exist_in_sink_config_source():
    src = open(SINK_CONFIG).read()
    doc = json.load(open(CHAOS_TMPL))
    cfg = {k: v for k, v in doc["spec"]["config"].items()
           if not k.startswith("//")}
    for key in CONNECTOR_OWNED_KEYS:
        assert key in cfg, f"chaos template missing pinned key {key!r}"
        assert f'"{key}"' in src, \
            f"config key {key!r} not found in ClickHouseSinkConfig.java (spelling drift)"


def test_template_is_valid_json_and_strimzi_shape():
    doc = json.load(open(CHAOS_TMPL))
    assert doc["kind"] == "KafkaConnector"
    assert doc["apiVersion"].startswith("kafka.strimzi.io/")
    assert doc["metadata"]["name"] == "chaos-clickhouse-sink"
    assert doc["spec"]["class"] == \
        "com.clickhouse.kafka.connect.ClickHouseSinkConnector"
