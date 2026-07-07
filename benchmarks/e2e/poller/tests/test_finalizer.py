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
"""Finalizer math tests: synthetic sample streams with hand-computed answers."""
import math

import pytest

import finalizer as F
import metric_names as mn


def mk_sample(t, offsets=None, jmx=None, pod=None, connect=None):
    return {
        "t": float(t),
        "offsets": offsets,
        "jmx": jmx,
        "pod": pod,
        "connect": connect if connect is not None else {
            "connector_state": "RUNNING",
            "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "w1:8083"}],
            "unavailable": False,
        },
    }


def off(committed, end):
    """single-partition offsets dict."""
    return {"0": {"committed": committed, "end": end}}


# --------------------------------------------------------------------------- #
# CoV
# --------------------------------------------------------------------------- #
def test_cov_flat_stream_is_zero():
    assert F._coefficient_of_variation([5.0, 5.0, 5.0]) == 0.0


def test_cov_known_value():
    # values 2,4,4,4,5,5,7,9 -> mean 5, population stddev 2, CoV = 0.4
    vals = [2, 4, 4, 4, 5, 5, 7, 9]
    assert F._coefficient_of_variation(vals) == pytest.approx(0.4)


def test_cov_single_point_is_none():
    assert F._coefficient_of_variation([3.0]) is None


def test_cov_zero_mean_is_none():
    assert F._coefficient_of_variation([-1.0, 1.0]) is None


# --------------------------------------------------------------------------- #
# time-weighted average
# --------------------------------------------------------------------------- #
def test_time_weighted_uneven_intervals():
    # value 10 held for 10s, value 20 held for 30s, last value uncredited.
    # weighted = (10*10 + 20*30) / (10+30) = (100+600)/40 = 17.5
    pairs = [(0.0, 10.0), (10.0, 20.0), (40.0, 99.0)]
    assert F._time_weighted_average(pairs) == pytest.approx(17.5)


def test_time_weighted_single_sample_returns_value():
    assert F._time_weighted_average([(5.0, 42.0)]) == 42.0


def test_time_weighted_skips_none_values():
    # None at t=10 is skipped; 10 held from 0->40 (next valid), then 40 uncredited
    pairs = [(0.0, 10.0), (10.0, None), (40.0, 30.0)]
    # valid = [(0,10),(40,30)] -> 10 weighted over 40s = 10.0
    assert F._time_weighted_average(pairs) == pytest.approx(10.0)


def test_time_weighted_nonmonotonic_clock_interval_ignored():
    # clock goes backwards between s1 and s2: that interval dropped.
    # (0,10),(5,20),(2,30) -> intervals: 0->5 credit 10*5; 5->2 dt<0 dropped.
    # den=5, num=50 -> 10.0
    pairs = [(0.0, 10.0), (5.0, 20.0), (2.0, 30.0)]
    assert F._time_weighted_average(pairs) == pytest.approx(10.0)


def test_time_weighted_all_none_is_none():
    assert F._time_weighted_average([(0.0, None), (1.0, None)]) is None


# --------------------------------------------------------------------------- #
# drain_seconds + lag_reached_zero
# --------------------------------------------------------------------------- #
def test_drain_seconds_basic():
    samples = [
        mk_sample(100, off(0, 1000)),
        mk_sample(110, off(500, 1000)),
        mk_sample(130, off(1000, 1000)),  # lag 0 here
        mk_sample(140, off(1000, 1000)),
    ]
    drain, reached = F.compute_drain_seconds(samples)
    assert drain == pytest.approx(30.0)  # 130 - 100
    assert reached is True


def test_drain_seconds_never_reaches_zero():
    samples = [
        mk_sample(100, off(0, 1000)),
        mk_sample(160, off(800, 1000)),
    ]
    drain, reached = F.compute_drain_seconds(samples)
    assert drain == pytest.approx(60.0)  # window to last sample
    assert reached is False


def test_drain_seconds_single_sample():
    samples = [mk_sample(100, off(1000, 1000))]
    drain, reached = F.compute_drain_seconds(samples)
    assert drain == 0.0
    assert reached is True  # already at lag 0


def test_drain_seconds_empty():
    drain, reached = F.compute_drain_seconds([])
    assert drain == 0.0 and reached is False


def test_drain_seconds_nonmonotonic_clock_never_negative():
    samples = [mk_sample(100, off(0, 10)), mk_sample(90, off(10, 10))]
    drain, reached = F.compute_drain_seconds(samples)
    assert drain == 0.0  # max(0, 90-100)
    assert reached is True


# --------------------------------------------------------------------------- #
# drain_rate_stability
# --------------------------------------------------------------------------- #
def test_drain_rate_stability_steady_is_zero():
    # equal deltas over equal intervals -> flat rate -> CoV 0
    samples = [
        mk_sample(0, off(0, 300)),
        mk_sample(10, off(100, 300)),
        mk_sample(20, off(200, 300)),
        mk_sample(30, off(300, 300)),
    ]
    assert F.compute_drain_rate_stability(samples) == pytest.approx(0.0)


def test_drain_rate_stability_sawtooth_positive():
    # deltas 100 then 0 then 200 over equal intervals -> nonzero CoV
    samples = [
        mk_sample(0, off(0, 1000)),
        mk_sample(10, off(100, 1000)),
        mk_sample(20, off(100, 1000)),
        mk_sample(30, off(300, 1000)),
    ]
    cov = F.compute_drain_rate_stability(samples)
    assert cov is not None and cov > 0.0


def test_drain_rate_stability_ignores_offset_regression():
    # a rebalance resets committed downward; that negative delta is dropped.
    samples = [
        mk_sample(0, off(0, 1000)),
        mk_sample(10, off(100, 1000)),
        mk_sample(20, off(50, 1000)),   # regression, ignored
        mk_sample(30, off(150, 1000)),
    ]
    # usable rates: (100/10)=10 and (100/10)=10 -> CoV 0
    assert F.compute_drain_rate_stability(samples) == pytest.approx(0.0)


# --------------------------------------------------------------------------- #
# partition_skew
# --------------------------------------------------------------------------- #
def test_partition_skew_peak_spread():
    samples = [
        mk_sample(0, {"0": {"committed": 100, "end": 500},
                      "1": {"committed": 90, "end": 500},
                      "2": {"committed": 80, "end": 500}}),
        mk_sample(10, {"0": {"committed": 400, "end": 500},
                       "1": {"committed": 200, "end": 500},
                       "2": {"committed": 150, "end": 500}}),
    ]
    # spreads: 20 then 250 -> peak 250
    assert F.compute_partition_skew(samples) == pytest.approx(250.0)


def test_partition_skew_single_partition_none():
    samples = [mk_sample(0, off(100, 500))]
    assert F.compute_partition_skew(samples) is None


# --------------------------------------------------------------------------- #
# jvm heap / gc / cpu-per-Mrows
# --------------------------------------------------------------------------- #
def test_jvm_heap_peak():
    samples = [
        mk_sample(0, off(0, 10), jmx={"jvm_heap_used_bytes": 100.0}),
        mk_sample(10, off(5, 10), jmx={"jvm_heap_used_bytes": 300.0}),
        mk_sample(20, off(10, 10), jmx={"jvm_heap_used_bytes": 200.0}),
    ]
    assert F.compute_jvm_heap_peak(samples) == 300.0


def test_gc_time_share():
    # GC cumulative 1.0 -> 4.0 over 30 wall seconds = 0.1
    samples = [
        mk_sample(0, off(0, 10), jmx={"gc_collection_seconds_sum": 1.0}),
        mk_sample(30, off(10, 10), jmx={"gc_collection_seconds_sum": 4.0}),
    ]
    assert F.compute_gc_time_share(samples) == pytest.approx(0.1)


def test_gc_time_share_counter_reset_is_none():
    samples = [
        mk_sample(0, off(0, 10), jmx={"gc_collection_seconds_sum": 5.0}),
        mk_sample(30, off(10, 10), jmx={"gc_collection_seconds_sum": 1.0}),
    ]
    assert F.compute_gc_time_share(samples) is None


def test_connect_cpu_seconds_per_Mrows():
    # cpu 10 -> 40 = 30 cpu-seconds over 2e6 rows = 30 / 2 = 15 s/Mrows
    samples = [
        mk_sample(0, off(0, 10), pod={"cpu_seconds_total": 10.0}),
        mk_sample(30, off(10, 10), pod={"cpu_seconds_total": 40.0}),
    ]
    assert F.compute_connect_cpu_seconds_per_Mrows(samples, 2_000_000) == pytest.approx(15.0)


def test_connect_cpu_unavailable_is_none():
    samples = [mk_sample(0, off(0, 10), pod={"unavailable": True}),
               mk_sample(10, off(10, 10), pod={"unavailable": True})]
    assert F.compute_connect_cpu_seconds_per_Mrows(samples, 2_000_000) is None


# --------------------------------------------------------------------------- #
# guards + flag tokens
# --------------------------------------------------------------------------- #
def test_guards_clean_run():
    samples = [
        mk_sample(0, off(0, 10)),
        mk_sample(10, off(10, 10)),
    ]
    g = F.compute_guards(samples, lag_reached_zero=True)
    assert g["flagged"] == 0
    assert g["flag_reason"] == ""
    assert g["counts"][mn.REBALANCE_COUNT] == 0.0
    assert g["counts"][mn.LAG_REACHED_ZERO] == 1.0


def test_guards_drain_incomplete_token():
    samples = [mk_sample(0, off(0, 10))]
    g = F.compute_guards(samples, lag_reached_zero=False)
    assert g["flagged"] == 1
    assert g["flag_reason"] == "drain_incomplete"


def test_guards_task_restart_via_worker_change():
    samples = [
        mk_sample(0, off(0, 10), connect={
            "connector_state": "RUNNING",
            "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "wA"}],
            "unavailable": False}),
        mk_sample(10, off(10, 10), connect={
            "connector_state": "RUNNING",
            "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "wB"}],
            "unavailable": False}),
    ]
    g = F.compute_guards(samples, lag_reached_zero=True)
    assert g["counts"][mn.CONNECT_TASK_RESTARTS] == 1.0
    assert "task_restart" in g["flag_reason"].split("|")


def test_guards_failed_task_maps_to_task_retries():
    samples = [
        mk_sample(0, off(0, 10), connect={
            "connector_state": "RUNNING",
            "tasks": [{"id": 0, "state": "FAILED", "worker_id": "wA"}],
            "unavailable": False}),
    ]
    g = F.compute_guards(samples, lag_reached_zero=True)
    assert g["counts"][mn.TASK_FAILED_COUNT] == 1.0
    assert "task_retries" in g["flag_reason"].split("|")


def test_guards_rebalance_via_state():
    samples = [
        mk_sample(0, off(0, 10), connect={
            "connector_state": "REBALANCING",
            "tasks": [], "unavailable": False}),
    ]
    g = F.compute_guards(samples, lag_reached_zero=True)
    assert g["counts"][mn.REBALANCE_COUNT] == 1.0
    assert "rebalance" in g["flag_reason"].split("|")


def test_guards_multiple_tokens_pipe_joined():
    samples = [
        mk_sample(0, off(0, 10), connect={
            "connector_state": "REBALANCING",
            "tasks": [{"id": 0, "state": "FAILED", "worker_id": "wA"}],
            "unavailable": False}),
    ]
    g = F.compute_guards(samples, lag_reached_zero=False)
    tokens = g["flag_reason"].split("|")
    assert set(tokens) >= {"rebalance", "task_retries", "drain_incomplete"}
    assert "," not in g["flag_reason"]  # only pipe permitted (contract §1.3)


def test_guards_unavailable_connect_does_not_fabricate_trips():
    samples = [
        mk_sample(0, off(0, 10), connect={"unavailable": True}),
        mk_sample(10, off(10, 10), connect={"unavailable": True}),
    ]
    g = F.compute_guards(samples, lag_reached_zero=True)
    assert g["flagged"] == 0
