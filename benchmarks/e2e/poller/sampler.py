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
"""Sampler — samples offsets / Connect REST / JMX / pod stats every POLL_INTERVAL.

Every source TOLERATES ABSENCE: a source that is down/unreachable marks itself
``unavailable`` in the sample rather than crashing the loop. Only the offsets
source (Kafka admin) is load-bearing for the drain-complete decision; if it is
unreachable the loop keeps trying (a sample with no offsets contributes nothing
but does not end the drain).

Raw samples are appended to a per-run JSONL file (one JSON object per line) so
the orchestrator can archive them and the finalizer can recompute offline.

Sources are injected (each is a zero-arg callable returning a dict-or-None) so
the loop is unit-testable without a live cluster; :func:`build_sources` wires the
real ones from config.
"""
import json
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional


# --------------------------------------------------------------------------- #
# offsets — Kafka admin API (confluent_kafka)
# --------------------------------------------------------------------------- #
def sample_offsets(admin, consumer, topic: str, group: str) -> Optional[Dict[str, Any]]:
    """Per-partition committed offset + end offset -> the raw material for lag.

    Uses confluent_kafka: the consumer's committed()/get_watermark_offsets for
    the group's committed position and the topic's high watermark (end). Returns
    {"<partition>": {"committed": int|None, "end": int}}; None on any failure
    (source unavailable this tick)."""
    try:
        from confluent_kafka import TopicPartition  # local import: tolerate absence
        md = consumer.list_topics(topic, timeout=10)
        tmd = md.topics.get(topic)
        if tmd is None or tmd.error is not None:
            return None
        tps = [TopicPartition(topic, p) for p in tmd.partitions.keys()]
        committed = consumer.committed(tps, timeout=10)
        committed_by_p = {tp.partition: (tp.offset if tp.offset >= 0 else None)
                          for tp in committed}
        out: Dict[str, Any] = {}
        for tp in tps:
            low, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
            out[str(tp.partition)] = {
                "committed": committed_by_p.get(tp.partition),
                "end": int(high),
            }
        return out
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# Connect REST /connectors/<name>/status
# --------------------------------------------------------------------------- #
def sample_connect_status(requests_mod, base_url: str, connector: str) -> Dict[str, Any]:
    """Connect REST status -> connector/task states + worker ids. Never raises;
    on failure returns {"unavailable": True}."""
    try:
        url = f"{base_url.rstrip('/')}/connectors/{connector}/status"
        r = requests_mod.get(url, timeout=10)
        r.raise_for_status()
        body = r.json()
        tasks = []
        for t in body.get("tasks", []):
            tasks.append({
                "id": t.get("id"),
                "state": t.get("state"),
                "worker_id": t.get("worker_id"),
            })
        return {
            "connector_state": (body.get("connector") or {}).get("state"),
            "tasks": tasks,
            "unavailable": False,
        }
    except Exception:
        return {"unavailable": True}


# --------------------------------------------------------------------------- #
# JMX via the Strimzi jmxPrometheusExporter endpoint (see README prerequisite)
# --------------------------------------------------------------------------- #
# The metric names below are the ones the repo's jmx-export-connector.yml maps
# the MBeans to (verified against that file). Standard Kafka Connect/consumer
# MBeans surface with the exporter's default naming; the sink's own MBeans
# surface as clickhouse_kafka_connect{attribute="..."}.
_PROM_LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+0-9.eE]+|NaN|\+?Inf)\s*$')


def _parse_prometheus(text: str) -> List[Dict[str, Any]]:
    """Minimal Prometheus text-exposition parser: [{name,labels,value}, ...].
    Ignores # comments and unparseable lines."""
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _PROM_LINE.match(line)
        if not m:
            continue
        name, labelblock, vals = m.group(1), m.group(2), m.group(3)
        try:
            val = float(vals)
        except ValueError:
            continue
        labels = {}
        if labelblock:
            inner = labelblock[1:-1]
            for kv in re.findall(r'([a-zA-Z0-9_]+)="((?:[^"\\]|\\.)*)"', inner):
                labels[kv[0]] = kv[1].replace('\\"', '"').replace('\\\\', '\\')
        out.append({"name": name, "labels": labels, "value": val})
    return out


def _sum_matching(series, name_pred, label_pred=lambda l: True):
    vals = [s["value"] for s in series
            if name_pred(s["name"]) and label_pred(s["labels"])]
    return sum(vals) if vals else None


def _avg_matching(series, name_pred, label_pred=lambda l: True):
    vals = [s["value"] for s in series
            if name_pred(s["name"]) and label_pred(s["labels"])]
    return (sum(vals) / len(vals)) if vals else None


def sample_jmx(requests_mod, metrics_url: str) -> Dict[str, Any]:
    """Scrape the prometheus endpoint and extract the sink-relevant series.

    Extracted (all best-effort; a missing series -> None for that field):
      * put_batch_avg_time_ms   — Connect sink-task-metrics put-batch-avg-time-ms
                                  (moving average; averaged across the tasks seen).
                                  Covered by the repo's jmx-export-connector.yml
                                  (`.+-avg` rule).
      * records_consumed_rate   — consumer-fetch-manager-metrics
                                  records-consumed-rate. NOT covered by the
                                  repo's jmx-export-connector.yml as-is: none of
                                  its rules matches a `-rate`-suffixed attribute.
                                  The KafkaConnect CR (task 31) MUST add the
                                  extra `-rate` exporter rule documented in the
                                  README prerequisites, or this field stays None.
      * fetch_latency_avg_ms    — consumer-fetch-manager-metrics fetch-latency-avg
                                  (covered by the `.+-avg` rule).
      * jvm_heap_used_bytes / jvm_heap_max_bytes — JVM heap. Matched under BOTH
                                  jmx_exporter/client_java spellings:
                                  jvm_memory_bytes_used{area="heap"} (older) and
                                  jvm_memory_used_bytes{area="heap"} (newer) —
                                  the deployed agent version decides which one is
                                  on the wire (see the version note below).
      * gc_collection_seconds_sum — cumulative GC seconds (jvm_gc_collection_seconds_sum)

    Name forms: the exporter sanitizes MBean attribute names into prometheus
    names (dashes -> underscores, lowercased), so only underscore forms can
    appear on the wire — there is no dash-form fallback to look for.

    Never raises; on scrape failure returns {"unavailable": True}."""
    try:
        r = requests_mod.get(metrics_url, timeout=10)
        r.raise_for_status()
        series = _parse_prometheus(r.text)
    except Exception:
        return {"unavailable": True}

    def name_has(substr):
        return lambda n: substr in n

    # put-batch-avg-time-ms: exporter emits kafka_connect_sink_task_<metric>.
    put_batch = _avg_matching(series, name_has("put_batch_avg_time_ms"))

    # requires the extra `-rate` exporter rule (README prerequisite #1a);
    # None until task 31 wires it.
    consumed = _sum_matching(series, name_has("records_consumed_rate"))

    fetch_lat = _avg_matching(series, name_has("fetch_latency_avg"))

    # JVM heap naming differs across jmx_exporter / client_java versions:
    #   * older jmx_exporter agent (<0.20):   jvm_memory_bytes_used{area="heap"}
    #   * newer client_java / jmx_exporter:   jvm_memory_used_bytes{area="heap"}
    # Live pair-2 (2026-07-09) emitted gc_collection_seconds_sum but NOT the
    # heap series -> connect_jvm_heap_peak was blind because we only matched the
    # `_bytes_used` spelling while the deployed exporter used `_used_bytes`.
    # Accept BOTH spellings (used and max) so heap is captured regardless of the
    # agent version. `_used_bytes` is anchored so it does not also match the
    # `_bytes_used` name — the two are checked as distinct substrings.
    heap_used = _sum_matching(
        series,
        lambda n: "jvm_memory_bytes_used" in n or "jvm_memory_used_bytes" in n,
        lambda l: l.get("area") == "heap")
    heap_max = _sum_matching(
        series,
        lambda n: "jvm_memory_bytes_max" in n or "jvm_memory_max_bytes" in n,
        lambda l: l.get("area") == "heap")
    gc_sum = _sum_matching(series, name_has("jvm_gc_collection_seconds_sum"))

    return {
        "put_batch_avg_time_ms": put_batch,
        "records_consumed_rate": consumed,
        "fetch_latency_avg_ms": fetch_lat,
        "jvm_heap_used_bytes": heap_used,
        "jvm_heap_max_bytes": heap_max,
        "gc_collection_seconds_sum": gc_sum,
        "unavailable": False,
    }


# --------------------------------------------------------------------------- #
# Connect pod CPU/mem — kubelet metrics.k8s.io via `kubectl top` fallback
# --------------------------------------------------------------------------- #
def sample_pod(subprocess_mod, namespace: str, pod_selector: str) -> Dict[str, Any]:
    """Connect pod container CPU seconds + working-set bytes.

    NOTE: `kubectl top` reports an INSTANTANEOUS CPU rate (millicores), not a
    cumulative counter, so it cannot be integrated for connect_cpu_seconds_per_Mrows
    directly. The real integration source is the cadvisor cumulative counter
    ``container_cpu_usage_seconds_total`` scraped from the kubelet /metrics/cadvisor
    endpoint. That requires cluster RBAC the poller may not have from a pod; the
    README documents metrics-server + the cadvisor scrape as the task-31
    prerequisite. This function returns the cumulative counter when a scrape URL
    is configured; otherwise it marks the source unavailable so CPU-per-Mrows is
    simply not emitted (tolerated absence, plan open-decision 1).

    Sources are injected via config; here we only support the documented cadvisor
    cumulative path (set POD_CADVISOR_URL). Without it -> unavailable."""
    # This function is intentionally a stub-with-contract: the real cadvisor
    # scrape is wired in build_sources when POD_CADVISOR_URL is set. Kept
    # separate so the loop/finalizer can be exercised with pod=None cleanly.
    return {"unavailable": True}


def sample_pod_cadvisor(requests_mod, cadvisor_url: str, pod: str, container: str,
                        headers: Optional[Dict[str, str]] = None,
                        verify: Any = None) -> Dict[str, Any]:
    """Scrape the kubelet cadvisor endpoint for the cumulative container CPU
    counter + working set. cadvisor exposes:
      container_cpu_usage_seconds_total{pod="...",container="..."}  (cumulative)
      container_memory_working_set_bytes{pod="...",container="..."}
    Label hygiene: for one pod, cadvisor emits a POD-AGGREGATE series
    (container="") PLUS the pause container (container="POD") PLUS the real
    per-container series. Summing all of them silently DOUBLE-COUNTS the CPU.
    With an explicit ``container`` filter we take exactly that container;
    without one we sum the real containers only, excluding container in
    ("", "POD").

    Auth/TLS: the direct kubelet endpoint (``:10250``) and the API-server proxy
    path (``https://kubernetes.default.svc/api/v1/nodes/<node>/proxy/metrics/cadvisor``)
    both require a Bearer token and CA-verified TLS. ``headers`` (e.g. the
    ServiceAccount ``Authorization: Bearer <token>``) and ``verify`` (the CA
    bundle path, or True/False) are forwarded to ``requests.get`` when set;
    when both are None the call is a plain unauthenticated GET (used by the
    unit tests' fake ``requests`` and by any pre-authorized proxy). The
    caller (:func:`build_sources`) fills these in for the in-cluster proxy path.
    Never raises; unavailable on failure."""
    try:
        kwargs: Dict[str, Any] = {"timeout": 10}
        if headers is not None:
            kwargs["headers"] = headers
        if verify is not None:
            kwargs["verify"] = verify
        r = requests_mod.get(cadvisor_url, **kwargs)
        r.raise_for_status()
        series = _parse_prometheus(r.text)
    except Exception:
        return {"unavailable": True}

    def label_match(labels):
        if labels.get("pod") != pod:
            return False
        c = labels.get("container", "")
        if container:
            return c == container
        # no explicit filter: real containers only — never the pod-aggregate
        # ("") or the pause container ("POD"), which would double-count.
        return c not in ("", "POD")

    cpu = _sum_matching(series,
                        lambda n: n == "container_cpu_usage_seconds_total",
                        label_match)
    mem = _sum_matching(series,
                        lambda n: n == "container_memory_working_set_bytes",
                        label_match)
    if cpu is None and mem is None:
        return {"unavailable": True}
    return {
        "cpu_seconds_total": cpu,
        "memory_working_set_bytes": mem,
        "unavailable": False,
    }


# --------------------------------------------------------------------------- #
# cadvisor startup self-check (pair-2 symptom b: the source never armed and the
# pod logs had ZERO cadvisor lines, so blindness was undiagnosable). One probe
# scrape at sample start logs the HTTP status + first bytes so a 403 (RBAC),
# an empty body, or a wrong URL is visible in the pod logs immediately.
# --------------------------------------------------------------------------- #
def probe_cadvisor(requests_mod, cadvisor_url: str, pod: str, container: str,
                   headers: Optional[Dict[str, str]] = None,
                   verify: Any = None, log=None) -> Dict[str, Any]:
    """Do ONE diagnostic GET of the cadvisor URL and log the outcome.

    Logs (via ``log``, one line, to the poller's stderr so it lands in pod logs):
      * the HTTP status code,
      * the number of bytes returned + the first ~120 bytes of the body,
      * how many container_cpu_usage_seconds_total series matched the pod filter
        (the number that actually feeds connect_cpu_seconds_per_Mrows).
    A non-200, an empty body, or 0 matching series each print a distinct WARN so
    the failure class (403 RBAC / empty / wrong-label) is readable at a glance.
    Never raises. Returns a small dict for the caller/tests; side effect is the
    log line(s)."""
    def _log(msg):
        if log:
            log(msg)

    kwargs: Dict[str, Any] = {"timeout": 10}
    if headers is not None:
        kwargs["headers"] = headers
    if verify is not None:
        kwargs["verify"] = verify
    try:
        r = requests_mod.get(cadvisor_url, **kwargs)
    except Exception as e:
        _log(f"cadvisor self-check: GET FAILED ({type(e).__name__}: {e}) "
             f"url={cadvisor_url} — pod CPU source will be UNAVAILABLE")
        return {"ok": False, "status": None, "error": str(e)}

    status = getattr(r, "status_code", None)
    body = getattr(r, "text", "") or ""
    head = body[:120].replace("\n", "\\n")
    if status is not None and status != 200:
        _log(f"cadvisor self-check: HTTP {status} ({len(body)} bytes) — "
             f"likely RBAC/403 (bench-poller-sa lacks nodes/proxy get) or a bad "
             f"node URL. first bytes: {head!r}")
        return {"ok": False, "status": status, "bytes": len(body)}
    if not body:
        _log(f"cadvisor self-check: HTTP {status} but EMPTY body — "
             f"proxy reachable but returned nothing; pod CPU source UNAVAILABLE")
        return {"ok": False, "status": status, "bytes": 0}

    series = _parse_prometheus(body)

    def _match(labels):
        if labels.get("pod") != pod:
            return False
        c = labels.get("container", "")
        return c == container if container else c not in ("", "POD")

    matched = sum(1 for s in series
                  if s["name"] == "container_cpu_usage_seconds_total"
                  and _match(s["labels"]))
    if matched == 0:
        _log(f"cadvisor self-check: HTTP {status} OK ({len(body)} bytes) but "
             f"0 container_cpu_usage_seconds_total series matched pod={pod!r} "
             f"container={container!r} — check the pod/container labels. "
             f"first bytes: {head!r}")
        return {"ok": False, "status": status, "bytes": len(body), "matched": 0}

    _log(f"cadvisor self-check: HTTP {status} OK ({len(body)} bytes), "
         f"{matched} cpu series matched pod={pod!r} container={container!r} — "
         f"pod CPU source ARMED")
    return {"ok": True, "status": status, "bytes": len(body), "matched": matched}


# --------------------------------------------------------------------------- #
# the sampling loop
# --------------------------------------------------------------------------- #
def run_sampler(
    out_path: str,
    offsets_source: Callable[[], Optional[Dict[str, Any]]],
    connect_source: Callable[[], Dict[str, Any]],
    jmx_source: Callable[[], Dict[str, Any]],
    pod_source: Callable[[], Dict[str, Any]],
    poll_interval: float = 10.0,
    timeout: float = 3600.0,
    sleep=time.sleep,
    now=time.time,
    log=None,
) -> Dict[str, Any]:
    """Sample every poll_interval until total lag == 0 or timeout.

    Writes one JSON sample per line to out_path (flushed each tick so a crash
    leaves a usable partial file). Returns a small dict:
      {"drained": bool, "timed_out": bool, "samples": int, "out": out_path}
    Exit-code mapping is the CLI's job (0 drained / 2 timeout).
    """
    def _log(msg):
        if log:
            log(msg)

    start = now()
    n = 0
    drained = False
    with open(out_path, "a", buffering=1) as f:
        while True:
            t = now()
            offsets = offsets_source()
            sample = {
                "t": t,
                "offsets": offsets,
                "connect": connect_source(),
                "jmx": jmx_source(),
                "pod": pod_source(),
            }
            f.write(json.dumps(sample) + "\n")
            f.flush()
            n += 1

            total_lag = _total_lag_of(offsets)
            _log(f"sample {n}: lag={total_lag} "
                 f"connect_up={not sample['connect'].get('unavailable')} "
                 f"jmx_up={not sample['jmx'].get('unavailable')}")

            if total_lag is not None and total_lag <= 0:
                drained = True
                break
            if (t - start) >= timeout:
                break
            sleep(poll_interval)

    return {
        "drained": drained,
        "timed_out": not drained,
        "samples": n,
        "out": out_path,
    }


def _total_lag_of(offsets: Optional[Dict[str, Any]]) -> Optional[int]:
    if not offsets:
        return None
    total = 0
    for _, po in offsets.items():
        c, e = po.get("committed"), po.get("end")
        if c is None or e is None:
            return None
        total += max(0, e - c)
    return total


def load_samples(path: str) -> List[Dict[str, Any]]:
    """Read a JSONL sample file into a list of dicts. Skips blank lines."""
    samples = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                samples.append(json.loads(line))
    return samples


# --------------------------------------------------------------------------- #
# wiring the real sources from config (imported lazily so tests need no deps)
# --------------------------------------------------------------------------- #
# Standard in-pod ServiceAccount mount (projected by the kubelet). Used to
# authenticate the kubelet cadvisor scrape through the API-server proxy.
SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
SA_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


def _cadvisor_auth(cadvisor_url: str, os_mod=os):
    """Return (headers, verify) for a cadvisor scrape.

    The API-server proxy path (``https://kubernetes.default.svc/.../proxy/metrics/cadvisor``)
    and the direct kubelet HTTPS port both require the pod's ServiceAccount
    Bearer token and the cluster CA for TLS verification. When the SA token is
    mounted (the in-cluster case) we send ``Authorization: Bearer <token>`` and
    verify against the mounted CA bundle. For a plain ``http://`` URL (test /
    pre-authorized proxy) no auth is added. Returns (None, None) when no token
    is available so :func:`sample_pod_cadvisor` falls back to a bare GET."""
    if not cadvisor_url.lower().startswith("https"):
        return None, None
    try:
        with open(SA_TOKEN_PATH) as f:
            token = f.read().strip()
    except OSError:
        return None, None
    headers = {"Authorization": f"Bearer {token}"}
    verify = SA_CA_PATH if os_mod.path.exists(SA_CA_PATH) else True
    return headers, verify


def build_sources(cfg: Dict[str, Any]):
    """Build the four zero-arg source callables from a config dict.

    cfg keys: bootstrap, group, topic, connect_url, connector, jmx_url,
              cadvisor_url (optional), pod_name (optional), pod_container (opt).
    Returns (offsets_source, connect_source, jmx_source, pod_source).
    Kafka/requests are imported here so the finalizer/tests never require them.
    """
    import requests
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": cfg["bootstrap"],
        "group.id": cfg["group"],
        # do NOT join the group being measured; only read its committed offsets.
        "enable.auto.commit": False,
        # librdkafka rejects the Java-consumer value "none" (_INVALID_ARG, live
        # run 2026-07-08); its equivalent is "error". Irrelevant in practice —
        # this consumer never subscribes/polls messages, only reads watermarks
        # and committed offsets — but the config must parse.
        "auto.offset.reset": "error",
    })

    def offsets_source():
        return sample_offsets(None, consumer, cfg["topic"], cfg["group"])

    def connect_source():
        return sample_connect_status(requests, cfg["connect_url"], cfg["connector"])

    def jmx_source():
        if not cfg.get("jmx_url"):
            return {"unavailable": True}
        return sample_jmx(requests, cfg["jmx_url"])

    def pod_source():
        cadvisor_url = cfg.get("cadvisor_url")
        if not cadvisor_url:
            return {"unavailable": True}
        headers, verify = _cadvisor_auth(cadvisor_url)
        return sample_pod_cadvisor(
            requests, cadvisor_url,
            cfg.get("pod_name", ""), cfg.get("pod_container", ""),
            headers=headers, verify=verify)

    return offsets_source, connect_source, jmx_source, pod_source
