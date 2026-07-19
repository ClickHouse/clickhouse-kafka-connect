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
"""Offline structural + lockstep tests for chaos/ch-cluster.yaml (task T2).

No cluster, no creds, no network. Two concerns:

  * STRUCTURAL — the manifest realises the IC-2 contract: namespace
    kafka-bench, StatefulSet `ch-chaos` (3 replicas, nodeSelector role=bench,
    ${CHAOS_CH_IMAGE} placeholder, gp3-bench PVC template `data`), headless
    Service `ch-chaos-headless` governing the STS, client Service `ch-chaos`
    on 8123/9000, ConfigMap `ch-chaos-config` carrying config.xml / users.xml /
    the clickbench.hits_chaos ReplicatedMergeTree DDL, /ping readiness probe,
    and the one_shard_three_replicas topology ONLY (the 3-shard shape is
    deliberately not carried over).

  * LOCKSTEP — the "derived from the test fixture" law. We parse BOTH the
    docker fixture config.xml and the ConfigMap's config.xml and assert the
    invariant elements are byte-equal: keeper_map_path_prefix, keeper
    coordination_settings, ports (8123/9000/9009/9181/9234), macro key names,
    3 raft members, internal_replication=true. Drift in the fixture breaks
    these tests by construction.

Plus an OPTIONAL live `kubectl apply --dry-run=client` gate that skips
gracefully when kubectl is absent (the structural/lockstep assertions are the
primary gate, per the T2 acceptance).
"""

import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
import yaml

# tests -> chaos -> e2e -> benchmarks -> repo root
REPO_ROOT = Path(__file__).resolve().parents[4]
MANIFEST = REPO_ROOT / "benchmarks/e2e/chaos/ch-cluster.yaml"
FIXTURE_CONFIG = (
    REPO_ROOT / "src/testFixtures/docker/clickhouse/cluster/config.xml"
)
FIXTURE_USERS = (
    REPO_ROOT / "src/testFixtures/docker/clickhouse/cluster/users.xml"
)

NS = "kafka-bench"
STS_NAME = "ch-chaos"
HEADLESS_SVC = "ch-chaos-headless"
CLIENT_SVC = "ch-chaos"
CONFIGMAP = "ch-chaos-config"


# --------------------------------------------------------------------------- #
# fixtures / loaders
# --------------------------------------------------------------------------- #
def _load_docs():
    assert MANIFEST.exists(), f"missing manifest: {MANIFEST}"
    with MANIFEST.open() as fh:
        docs = [d for d in yaml.safe_load_all(fh) if d]
    return docs


@pytest.fixture(scope="module")
def docs():
    return _load_docs()


def _by_kind(docs, kind, name=None):
    out = [
        d
        for d in docs
        if d.get("kind") == kind
        and (name is None or d.get("metadata", {}).get("name") == name)
    ]
    return out


@pytest.fixture(scope="module")
def sts(docs):
    got = _by_kind(docs, "StatefulSet", STS_NAME)
    assert len(got) == 1, f"expected exactly one StatefulSet {STS_NAME}"
    return got[0]


@pytest.fixture(scope="module")
def configmap(docs):
    got = _by_kind(docs, "ConfigMap", CONFIGMAP)
    assert len(got) == 1, f"expected exactly one ConfigMap {CONFIGMAP}"
    return got[0]


@pytest.fixture(scope="module")
def cm_config_xml(configmap):
    data = configmap["data"]
    assert "config.xml" in data, "ConfigMap must carry config.xml"
    return ET.fromstring(data["config.xml"])


@pytest.fixture(scope="module")
def fixture_config_xml():
    assert FIXTURE_CONFIG.exists(), f"missing fixture: {FIXTURE_CONFIG}"
    return ET.fromstring(FIXTURE_CONFIG.read_text())


# --------------------------------------------------------------------------- #
# structural — namespace + kinds present
# --------------------------------------------------------------------------- #
def test_all_resources_in_namespace(docs):
    for d in docs:
        # StorageClass is cluster-scoped; skip if ever present.
        if d.get("kind") == "StorageClass":
            continue
        assert (
            d["metadata"]["namespace"] == NS
        ), f"{d.get('kind')}/{d['metadata'].get('name')} not in {NS}"


def test_expected_kinds_present(docs):
    kinds = {d["kind"] for d in docs}
    assert {"StatefulSet", "Service", "ConfigMap"} <= kinds


# --------------------------------------------------------------------------- #
# structural — StatefulSet
# --------------------------------------------------------------------------- #
def test_statefulset_replicas_and_governing_service(sts):
    assert sts["spec"]["replicas"] == 3
    assert sts["spec"]["serviceName"] == HEADLESS_SVC


def test_statefulset_pod_management_is_parallel(sts):
    # REQUIRED: the embedded 3-node keeper quorum can only bootstrap if all pods
    # start at once (raft pre-vote needs >=2 peers reachable, and a pod is not
    # Ready until CH finishes startup, which waits on keeper). OrderedReady
    # deadlocks pod-0 waiting for peers that are never created. Regression guard
    # for the live-L1 finding (2026-07-19).
    assert sts["spec"]["podManagementPolicy"] == "Parallel"


def test_statefulset_nodeselector_bench(sts):
    ns = sts["spec"]["template"]["spec"]["nodeSelector"]
    assert ns.get("role") == "bench"


def test_container_uses_image_placeholder(sts):
    containers = sts["spec"]["template"]["spec"]["containers"]
    assert len(containers) == 1
    assert containers[0]["image"] == "${CHAOS_CH_IMAGE}"


def test_container_exposes_all_ports(sts):
    c = sts["spec"]["template"]["spec"]["containers"][0]
    ports = {p["containerPort"] for p in c["ports"]}
    assert {8123, 9000, 9009, 9181, 9234} <= ports


def test_readiness_probe_is_ping(sts):
    c = sts["spec"]["template"]["spec"]["containers"][0]
    probe = c["readinessProbe"]
    assert probe["httpGet"]["path"] == "/ping"
    assert probe["httpGet"]["port"] == 8123


def test_ordinal_derivation_is_envsubst_safe(sts):
    """server_id / REPLICA_NUM derive from the pod ordinal via the container
    command, and that command must survive `envsubst < ch-cluster.yaml` — so it
    must not reference bare $VAR / ${VAR} (which envsubst would clobber). Only
    $( ) / $(( )) command/arith substitution is preserved."""
    c = sts["spec"]["template"]["spec"]["containers"][0]
    cmd = "\n".join(c.get("command", []) + c.get("args", []))
    assert "hostname" in cmd, "ordinal must be derived from the pod hostname"
    assert "REPLICA_NUM" in cmd and "SERVER_INDEX" in cmd
    # No bare/braced variable references that envsubst would replace, except
    # the CHAOS_CH_IMAGE placeholder (which belongs in the image field, not
    # the command). Scan char-by-char: a '$' followed by a letter/_ or '{' is
    # a substitution envsubst would eat.
    for i, ch in enumerate(cmd):
        if ch == "$":
            nxt = cmd[i + 1] if i + 1 < len(cmd) else ""
            assert nxt in ("(", "$", "'", '"', ""), (
                f"envsubst-unsafe reference near: {cmd[i:i + 12]!r}"
            )


def test_pvc_template_uses_gp3_bench(sts):
    vct = sts["spec"]["volumeClaimTemplates"]
    names = {v["metadata"]["name"] for v in vct}
    assert "data" in names
    data_pvc = next(v for v in vct if v["metadata"]["name"] == "data")
    assert data_pvc["spec"]["storageClassName"] == "gp3-bench"


# --------------------------------------------------------------------------- #
# structural — Services
# --------------------------------------------------------------------------- #
def test_headless_service(docs):
    got = _by_kind(docs, "Service", HEADLESS_SVC)
    assert len(got) == 1
    svc = got[0]
    assert svc["spec"]["clusterIP"] == "None", "headless => clusterIP None"
    assert svc["spec"]["selector"].get("app") == STS_NAME


def test_client_service_ports(docs):
    got = _by_kind(docs, "Service", CLIENT_SVC)
    assert len(got) == 1
    svc = got[0]
    assert svc["spec"].get("clusterIP") != "None", "client svc is not headless"
    ports = {p["port"] for p in svc["spec"]["ports"]}
    assert {8123, 9000} <= ports
    assert svc["spec"]["selector"].get("app") == STS_NAME


# --------------------------------------------------------------------------- #
# structural — ConfigMap payloads
# --------------------------------------------------------------------------- #
def test_configmap_has_all_payloads(configmap):
    data = configmap["data"]
    assert "config.xml" in data
    assert "users.xml" in data
    # the hits_chaos DDL, keyed however the manifest names it, must be present
    ddl_keys = [k for k in data if "hits_chaos" in k or k.endswith(".sql")]
    assert ddl_keys, "ConfigMap must carry the hits_chaos DDL"


def test_configmap_users_xml_matches_fixture(configmap):
    got = ET.fromstring(configmap["data"]["users.xml"])
    fixture = ET.fromstring(FIXTURE_USERS.read_text())
    # default user with access_management carried over verbatim
    assert (
        got.find("users/default/access_management").text
        == fixture.find("users/default/access_management").text
    )


def test_hits_chaos_ddl_is_replicated(configmap):
    data = configmap["data"]
    ddl_key = next(
        k for k in data if "hits_chaos" in k or k.endswith(".sql")
    )
    ddl = data[ddl_key]
    assert "clickbench.hits_chaos" in ddl
    assert "ReplicatedMergeTree" in ddl
    # keyed on the fixture macros so replication actually forms
    assert "{replica}" in ddl
    assert "{shard}" in ddl


def test_configmap_drops_three_shard_topology(cm_config_xml):
    """The survivable-by-design shape only: the 3-shard fixture cluster must
    NOT be carried over (spec §2, IC-2)."""
    rs = cm_config_xml.find("remote_servers")
    assert rs is not None
    assert rs.find("three_shards_one_replica_each") is None
    assert rs.find("one_shard_three_replicas") is not None


# --------------------------------------------------------------------------- #
# LOCKSTEP — fixture config.xml vs ConfigMap config.xml
# --------------------------------------------------------------------------- #
def test_lockstep_keeper_map_path_prefix(cm_config_xml, fixture_config_xml):
    got = cm_config_xml.find("keeper_map_path_prefix").text
    want = fixture_config_xml.find("keeper_map_path_prefix").text
    assert got == want == "/keeper_map_tables"


def test_lockstep_coordination_settings(cm_config_xml, fixture_config_xml):
    def _settings(root):
        cs = root.find("keeper_server/coordination_settings")
        assert cs is not None
        return {child.tag: (child.text or "").strip() for child in cs}

    got = _settings(cm_config_xml)
    want = _settings(fixture_config_xml)
    assert got == want
    # sanity: the fixture's actual values, so drift in either side is caught
    assert want["operation_timeout_ms"] == "10000"
    assert want["session_timeout_ms"] == "30000"


def test_lockstep_ports(cm_config_xml, fixture_config_xml):
    def _ports(root):
        return {
            "http": root.find("http_port").text,
            "tcp": root.find("tcp_port").text,
            "interserver": root.find("interserver_http_port").text,
            "keeper": root.find("keeper_server/tcp_port").text,
        }

    got = _ports(cm_config_xml)
    want = _ports(fixture_config_xml)
    assert got == want
    assert want == {
        "http": "8123",
        "tcp": "9000",
        "interserver": "9009",
        "keeper": "9181",
    }
    # raft port 9234 on every member, both sides
    for root in (cm_config_xml, fixture_config_xml):
        raft_ports = {
            s.find("port").text
            for s in root.findall("keeper_server/raft_configuration/server")
        }
        assert raft_ports == {"9234"}


def test_lockstep_macro_key_names(cm_config_xml, fixture_config_xml):
    got = {child.tag for child in cm_config_xml.find("macros")}
    want = {child.tag for child in fixture_config_xml.find("macros")}
    assert got == want


def test_lockstep_three_raft_members(cm_config_xml, fixture_config_xml):
    got = cm_config_xml.findall("keeper_server/raft_configuration/server")
    want = fixture_config_xml.findall("keeper_server/raft_configuration/server")
    assert len(got) == len(want) == 3
    # ids are the quorum identity — must be {1,2,3} on both sides
    got_ids = {s.find("id").text for s in got}
    want_ids = {s.find("id").text for s in want}
    assert got_ids == want_ids == {"1", "2", "3"}


def test_lockstep_internal_replication(cm_config_xml, fixture_config_xml):
    def _ir(root):
        return (
            root.find(
                "remote_servers/one_shard_three_replicas/shard/"
                "internal_replication"
            ).text
        )

    assert _ir(cm_config_xml) == _ir(fixture_config_xml) == "true"


def test_lockstep_three_replicas_in_shard(cm_config_xml):
    replicas = cm_config_xml.findall(
        "remote_servers/one_shard_three_replicas/shard/replica"
    )
    assert len(replicas) == 3


# --------------------------------------------------------------------------- #
# OPTIONAL — live kubectl dry-run (skips gracefully without kubectl)
# --------------------------------------------------------------------------- #
def test_kubectl_dry_run_apply():
    kubectl = shutil.which("kubectl")
    envsubst = shutil.which("envsubst")
    if not kubectl:
        pytest.skip("kubectl not on PATH — structural/lockstep tests are the gate")
    if not envsubst:
        pytest.skip("envsubst not on PATH")

    env = dict(os.environ)
    env["CHAOS_CH_IMAGE"] = "clickhouse/clickhouse-server:latest"
    rendered = subprocess.run(
        [envsubst],
        stdin=MANIFEST.open("rb"),
        capture_output=True,
        env=env,
    )
    assert rendered.returncode == 0, rendered.stderr.decode()
    proc = subprocess.run(
        [
            kubectl,
            "apply",
            "--dry-run=client",
            "--validate=false",
            "-f",
            "-",
        ],
        input=rendered.stdout,
        capture_output=True,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.decode()
        # `kubectl apply --dry-run=client` still does server API-group discovery
        # to build RESTMappings, so it needs a reachable cluster. Phase-1 has no
        # cluster/credentials: that is an environment limitation, not a manifest
        # defect — skip. A genuine manifest error (decode/validation) still
        # fails, so this stays a real gate wherever a cluster IS reachable.
        infra_markers = (
            "provide credentials",
            "couldn't get current server API group list",
            "Unable to connect to the server",
            "connection refused",
            "no configuration has been provided",
            "context was not found",
            "did you specify the right host or port",
        )
        if any(m in stderr for m in infra_markers):
            pytest.skip(
                "kubectl present but no reachable cluster — structural/"
                "lockstep tests are the primary gate. stderr:\n" + stderr
            )
        pytest.fail(
            f"kubectl dry-run rejected the manifest:\n"
            f"STDOUT:{proc.stdout.decode()}\nSTDERR:{stderr}"
        )
