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
"""Simulated end-to-end: fixture JSONL -> finalize -> full expected scalar set,
plus programmatic validation of the emitted metric NAMES against the contract."""
import json

import pytest

import finalizer as F
import metric_names as mn
import sampler


def full_stream():
    """A complete drain with all four sources present."""
    return [
        {
            "t": 0.0,
            "offsets": {"0": {"committed": 0, "end": 300},
                        "1": {"committed": 0, "end": 300},
                        "2": {"committed": 0, "end": 300}},
            "connect": {"connector_state": "RUNNING",
                        "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "w1"},
                                  {"id": 1, "state": "RUNNING", "worker_id": "w1"},
                                  {"id": 2, "state": "RUNNING", "worker_id": "w1"}],
                        "unavailable": False},
            "jmx": {"put_batch_avg_time_ms": 5.0, "records_consumed_rate": 1000.0,
                    "fetch_latency_avg_ms": 2.0, "jvm_heap_used_bytes": 1e8,
                    "jvm_heap_max_bytes": 2e8, "gc_collection_seconds_sum": 1.0,
                    "unavailable": False},
            "pod": {"cpu_seconds_total": 10.0,
                    "memory_working_set_bytes": 5e8, "unavailable": False},
        },
        {
            "t": 30.0,
            "offsets": {"0": {"committed": 300, "end": 300},
                        "1": {"committed": 300, "end": 300},
                        "2": {"committed": 300, "end": 300}},
            "connect": {"connector_state": "RUNNING",
                        "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "w1"},
                                  {"id": 1, "state": "RUNNING", "worker_id": "w1"},
                                  {"id": 2, "state": "RUNNING", "worker_id": "w1"}],
                        "unavailable": False},
            "jmx": {"put_batch_avg_time_ms": 7.0, "records_consumed_rate": 1200.0,
                    "fetch_latency_avg_ms": 3.0, "jvm_heap_used_bytes": 1.5e8,
                    "jvm_heap_max_bytes": 2e8, "gc_collection_seconds_sum": 2.5,
                    "unavailable": False},
            "pod": {"cpu_seconds_total": 25.0,
                    "memory_working_set_bytes": 6e8, "unavailable": False},
        },
    ]


@pytest.mark.parametrize("tier", [0, 1])
def test_emitted_name_set_matches_contract(tier):
    """The emitted names for a full stream MUST equal exactly the tier's pinned
    allowed set (contract §2 + §1.2, plan §7). No missing, no stray."""
    res = F.finalize(full_stream(), tier=tier, rows_expected=900)
    emitted = set(res["scalars"].keys())
    assert emitted == mn.METRICS_BY_TIER[tier]


def test_tier0_headline_is_null_drain():
    res = F.finalize(full_stream(), tier=0, rows_expected=900)
    assert mn.NULL_DRAIN_ROWS_PER_SEC in res["scalars"]
    assert mn.DRAIN_ROWS_PER_SEC not in res["scalars"]  # tier-exclusive


def test_tier1_headline_is_drain_rows():
    res = F.finalize(full_stream(), tier=1, rows_expected=900)
    assert mn.DRAIN_ROWS_PER_SEC in res["scalars"]
    assert mn.NULL_DRAIN_ROWS_PER_SEC not in res["scalars"]


def test_throughput_value():
    # 900 rows / 30 s = 30 rows/s
    res = F.finalize(full_stream(), tier=1, rows_expected=900)
    assert res["scalars"][mn.DRAIN_ROWS_PER_SEC] == pytest.approx(30.0)
    assert res["scalars"][mn.DRAIN_SECONDS] == pytest.approx(30.0)


def test_cpu_per_mrows_value():
    # cpu 10->25 = 15 cpu-s over 900 rows = 15 / (900/1e6) = 16666.67 s/Mrows
    res = F.finalize(full_stream(), tier=1, rows_expected=900)
    assert res["scalars"][mn.CONNECT_CPU_SECONDS_PER_MROWS] == pytest.approx(15 / (900 / 1e6))


def test_put_batch_time_weighted():
    # 5.0 held 30s, then 7.0 uncredited -> 5.0
    res = F.finalize(full_stream(), tier=1, rows_expected=900)
    assert res["scalars"][mn.PUT_BATCH_AVG_TIME_MS] == pytest.approx(5.0)


def test_every_emitted_name_has_a_pinned_unit():
    res = F.finalize(full_stream(), tier=1, rows_expected=900)
    for name in res["scalars"]:
        assert mn.unit_for(name)  # raises if missing


def test_missing_sources_yield_none_not_zero():
    """A stream with no JMX/pod -> those scalars are None (skipped on insert)."""
    stream = [
        {"t": 0.0, "offsets": {"0": {"committed": 0, "end": 100}},
         "connect": {"connector_state": "RUNNING",
                     "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "w"}],
                     "unavailable": False},
         "jmx": {"unavailable": True}, "pod": {"unavailable": True}},
        {"t": 10.0, "offsets": {"0": {"committed": 100, "end": 100}},
         "connect": {"connector_state": "RUNNING",
                     "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "w"}],
                     "unavailable": False},
         "jmx": {"unavailable": True}, "pod": {"unavailable": True}},
    ]
    res = F.finalize(stream, tier=0, rows_expected=100)
    assert res["scalars"][mn.PUT_BATCH_AVG_TIME_MS] is None
    assert res["scalars"][mn.CONNECT_CPU_SECONDS_PER_MROWS] is None
    assert res["scalars"][mn.CONNECT_JVM_HEAP_PEAK] is None
    # headline still computed from offsets
    assert res["scalars"][mn.NULL_DRAIN_ROWS_PER_SEC] == pytest.approx(10.0)


def test_finalize_rejects_bad_tier():
    with pytest.raises(ValueError):
        F.finalize(full_stream(), tier=2, rows_expected=900)


def test_jsonl_roundtrip(tmp_path):
    """Write the fixture as JSONL, load it back, finalize -> same result."""
    p = tmp_path / "samples.jsonl"
    with open(p, "w") as f:
        for s in full_stream():
            f.write(json.dumps(s) + "\n")
    loaded = sampler.load_samples(str(p))
    res = F.finalize(loaded, tier=1, rows_expected=900)
    assert res["scalars"][mn.DRAIN_ROWS_PER_SEC] == pytest.approx(30.0)


def test_prometheus_parser():
    text = """
# HELP jvm_memory_bytes_used help
# TYPE jvm_memory_bytes_used gauge
jvm_memory_bytes_used{area="heap"} 1.23e8
jvm_memory_bytes_used{area="nonheap"} 5.0e7
jvm_gc_collection_seconds_sum{gc="G1 Young"} 3.5
kafka_connect_sink_task_put_batch_avg_time_ms{connector="ch",task="0"} 4.2
"""
    series = sampler._parse_prometheus(text)
    heap = sampler._sum_matching(
        series, lambda n: "jvm_memory_bytes_used" in n,
        lambda l: l.get("area") == "heap")
    assert heap == pytest.approx(1.23e8)
    gc = sampler._sum_matching(series, lambda n: "jvm_gc_collection_seconds_sum" in n)
    assert gc == pytest.approx(3.5)


# --------------------------------------------------------------------------- #
# heap series naming across jmx_exporter versions (pair-2 blindness, symptom d)
# --------------------------------------------------------------------------- #
# Older jmx_exporter agents emit jvm_memory_bytes_used; newer client_java emits
# jvm_memory_used_bytes. The live pair-2 exporter used the newer spelling, so
# connect_jvm_heap_peak was null while gc (unaffected) was present. sample_jmx
# must capture heap under BOTH spellings.
_JMX_OLD_SPELLING = """
jvm_memory_bytes_used{area="heap"} 1.0e8
jvm_memory_bytes_used{area="nonheap"} 3.0e7
jvm_memory_bytes_max{area="heap"} 2.0e8
jvm_gc_collection_seconds_sum{gc="G1 Young"} 5.0
"""

_JMX_NEW_SPELLING = """
jvm_memory_used_bytes{area="heap"} 1.0e8
jvm_memory_used_bytes{area="nonheap"} 3.0e7
jvm_memory_max_bytes{area="heap"} 2.0e8
jvm_gc_collection_seconds_sum{gc="G1 Young"} 5.0
"""


@pytest.mark.parametrize("text", [_JMX_OLD_SPELLING, _JMX_NEW_SPELLING])
def test_sample_jmx_heap_both_exporter_spellings(text):
    res = sampler.sample_jmx(_FakeReq(text), "http://connect:9404/metrics")
    assert res["unavailable"] is False
    # heap captured (not None) under either spelling, nonheap excluded by area.
    assert res["jvm_heap_used_bytes"] == pytest.approx(1.0e8)
    assert res["jvm_heap_max_bytes"] == pytest.approx(2.0e8)
    assert res["gc_collection_seconds_sum"] == pytest.approx(5.0)


# --------------------------------------------------------------------------- #
# cadvisor scrape label hygiene (review follow-up 1)
# --------------------------------------------------------------------------- #
class _FakeResp:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        pass


class _FakeReq:
    def __init__(self, text):
        self._text = text

    def get(self, url, timeout=None):
        return _FakeResp(self._text)


# For one pod cadvisor emits the pod-aggregate (container=""), the pause
# container ("POD"), AND the per-container series. 60+40 = the aggregate 100,
# so summing everything would report 201 (double-count) instead of 100.
_CADVISOR_TEXT = """
container_cpu_usage_seconds_total{pod="connect-0",container=""} 100.0
container_cpu_usage_seconds_total{pod="connect-0",container="POD"} 1.0
container_cpu_usage_seconds_total{pod="connect-0",container="connect"} 60.0
container_cpu_usage_seconds_total{pod="connect-0",container="sidecar"} 40.0
container_cpu_usage_seconds_total{pod="other",container="connect"} 999.0
container_memory_working_set_bytes{pod="connect-0",container=""} 500.0
container_memory_working_set_bytes{pod="connect-0",container="POD"} 5.0
container_memory_working_set_bytes{pod="connect-0",container="connect"} 300.0
container_memory_working_set_bytes{pod="connect-0",container="sidecar"} 200.0
"""


def test_cadvisor_no_container_filter_excludes_aggregate_and_pause():
    res = sampler.sample_pod_cadvisor(
        _FakeReq(_CADVISOR_TEXT), "http://kubelet/metrics/cadvisor",
        pod="connect-0", container="")
    assert res["unavailable"] is False
    assert res["cpu_seconds_total"] == pytest.approx(100.0)   # 60+40, NOT 201
    assert res["memory_working_set_bytes"] == pytest.approx(500.0)  # 300+200


def test_cadvisor_explicit_container_filter_takes_exactly_that_container():
    res = sampler.sample_pod_cadvisor(
        _FakeReq(_CADVISOR_TEXT), "http://kubelet/metrics/cadvisor",
        pod="connect-0", container="connect")
    assert res["cpu_seconds_total"] == pytest.approx(60.0)
    assert res["memory_working_set_bytes"] == pytest.approx(300.0)


# --------------------------------------------------------------------------- #
# cadvisor startup self-check (pair-2 symptom b: blindness was undiagnosable)
# --------------------------------------------------------------------------- #
class _StatusResp:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        pass


class _StatusReq:
    def __init__(self, text, status_code=200, raise_exc=None):
        self._text = text
        self._status = status_code
        self._raise = raise_exc

    def get(self, url, timeout=None, headers=None, verify=None):
        if self._raise is not None:
            raise self._raise
        return _StatusResp(self._text, self._status)


def _capture_log():
    lines = []
    return lines, lines.append


def test_probe_cadvisor_armed_logs_matched_count():
    lines, log = _capture_log()
    res = sampler.probe_cadvisor(
        _StatusReq(_CADVISOR_TEXT, 200), "https://k/proxy/metrics/cadvisor",
        pod="connect-0", container="connect", log=log)
    assert res["ok"] is True and res["matched"] == 1
    assert any("ARMED" in l for l in lines)


def test_probe_cadvisor_403_logs_rbac_hint():
    lines, log = _capture_log()
    res = sampler.probe_cadvisor(
        _StatusReq("Forbidden", 403), "https://k/proxy/metrics/cadvisor",
        pod="connect-0", container="connect", log=log)
    assert res["ok"] is False and res["status"] == 403
    assert any("403" in l or "RBAC" in l for l in lines)


def test_probe_cadvisor_empty_body_logs_empty():
    lines, log = _capture_log()
    res = sampler.probe_cadvisor(
        _StatusReq("", 200), "https://k/proxy/metrics/cadvisor",
        pod="connect-0", container="connect", log=log)
    assert res["ok"] is False and res["bytes"] == 0
    assert any("EMPTY" in l for l in lines)


def test_probe_cadvisor_no_matching_series_logs_label_hint():
    lines, log = _capture_log()
    res = sampler.probe_cadvisor(
        _StatusReq(_CADVISOR_TEXT, 200), "https://k/proxy/metrics/cadvisor",
        pod="does-not-exist", container="", log=log)
    assert res["ok"] is False and res["matched"] == 0
    assert any("0 container_cpu_usage_seconds_total" in l for l in lines)


def test_probe_cadvisor_connection_error_logged_not_raised():
    lines, log = _capture_log()
    res = sampler.probe_cadvisor(
        _StatusReq("", raise_exc=OSError("connection refused")),
        "https://k/proxy/metrics/cadvisor",
        pod="connect-0", container="connect", log=log)
    assert res["ok"] is False
    assert any("GET FAILED" in l for l in lines)


def test_cadvisor_other_pod_never_included():
    res = sampler.sample_pod_cadvisor(
        _FakeReq(_CADVISOR_TEXT), "http://kubelet/metrics/cadvisor",
        pod="does-not-exist", container="")
    assert res["unavailable"] is True


# --------------------------------------------------------------------------- #
# cadvisor API-server-proxy auth (poller prerequisite 2 — sighted CPU gate)
# --------------------------------------------------------------------------- #
class _FakeOs:
    """Minimal os-like shim: `path.exists` answers from a set of present paths."""
    def __init__(self, present):
        self._present = set(present)
        self.path = self

    def exists(self, p):
        return p in self._present


def test_cadvisor_auth_http_url_is_unauthenticated():
    # A plain http:// URL (test / pre-authorized proxy) gets no token/CA — the
    # sampler then issues a bare GET, matching the existing offline tests.
    headers, verify = sampler._cadvisor_auth(
        "http://kubelet:10255/metrics/cadvisor", os_mod=_FakeOs([]))
    assert headers is None and verify is None


def test_cadvisor_auth_https_reads_token_and_ca(tmp_path, monkeypatch):
    # In-cluster https:// proxy path: read the projected SA token, send it as a
    # Bearer header, and verify TLS against the mounted cluster CA.
    token_file = tmp_path / "token"
    token_file.write_text("  abc.def.ghi\n")   # whitespace must be stripped
    ca_file = tmp_path / "ca.crt"
    ca_file.write_text("---CA---")
    monkeypatch.setattr(sampler, "SA_TOKEN_PATH", str(token_file))
    monkeypatch.setattr(sampler, "SA_CA_PATH", str(ca_file))
    headers, verify = sampler._cadvisor_auth(
        "https://kubernetes.default.svc/api/v1/nodes/n1/proxy/metrics/cadvisor",
        os_mod=_FakeOs([str(ca_file)]))
    assert headers == {"Authorization": "Bearer abc.def.ghi"}
    assert verify == str(ca_file)


def test_cadvisor_auth_https_no_token_falls_back_to_bare():
    # https:// but the SA token is not mounted (not in-cluster) -> no auth added,
    # so sample_pod_cadvisor degrades to a bare GET rather than crashing.
    headers, verify = sampler._cadvisor_auth(
        "https://kubernetes.default.svc/api/v1/nodes/n1/proxy/metrics/cadvisor",
        os_mod=_FakeOs([]))  # os shim irrelevant; token open() will fail
    assert headers is None and verify is None


class _CapturingReq:
    """Fake requests that records the kwargs of the last get() call."""
    def __init__(self, text):
        self._text = text
        self.calls = []

    def get(self, url, timeout=None, headers=None, verify=None):
        self.calls.append({"url": url, "timeout": timeout,
                           "headers": headers, "verify": verify})
        return _FakeResp(self._text)


def test_cadvisor_forwards_headers_and_verify_to_get():
    # When the caller supplies auth, sample_pod_cadvisor must forward it to
    # requests.get (Bearer header + CA verify) AND still parse correctly.
    req = _CapturingReq(_CADVISOR_TEXT)
    res = sampler.sample_pod_cadvisor(
        req, "https://kubernetes.default.svc/.../proxy/metrics/cadvisor",
        pod="connect-0", container="connect",
        headers={"Authorization": "Bearer tok"}, verify="/ca.crt")
    assert res["cpu_seconds_total"] == pytest.approx(60.0)
    call = req.calls[-1]
    assert call["headers"] == {"Authorization": "Bearer tok"}
    assert call["verify"] == "/ca.crt"
    assert call["timeout"] == 10
