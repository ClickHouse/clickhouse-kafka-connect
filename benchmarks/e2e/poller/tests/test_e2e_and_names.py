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
