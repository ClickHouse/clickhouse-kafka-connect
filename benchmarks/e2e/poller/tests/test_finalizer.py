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
    # equal deltas over equal intervals -> flat rate -> CoV 0.
    # bucket_seconds=10 => one interval per bucket (per-poll granularity).
    samples = [
        mk_sample(0, off(0, 300)),
        mk_sample(10, off(100, 300)),
        mk_sample(20, off(200, 300)),
        mk_sample(30, off(300, 300)),
    ]
    assert F.compute_drain_rate_stability(
        samples, bucket_seconds=10) == pytest.approx(0.0)


def test_drain_rate_stability_sawtooth_positive():
    # deltas 100 then 0 then 200 over equal intervals -> nonzero CoV
    # (bucket_seconds=10 keeps each poll in its own bucket).
    samples = [
        mk_sample(0, off(0, 1000)),
        mk_sample(10, off(100, 1000)),
        mk_sample(20, off(100, 1000)),
        mk_sample(30, off(300, 1000)),
    ]
    cov = F.compute_drain_rate_stability(samples, bucket_seconds=10)
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
    assert F.compute_drain_rate_stability(
        samples, bucket_seconds=10) == pytest.approx(0.0)


# --------------------------------------------------------------------------- #
# drain_rate_stability — bucketing refinement (2026-07-10)
# --------------------------------------------------------------------------- #
def _lumpy_commit_stream():
    """A STEADY drain whose committed-offset counter advances in lumps.

    True drain rate is a flat 2500 rows/s. But the sink commits offsets only
    every ~30s in a single 75000-row lump, so at the 10s poll cadence the
    committed position steps 0, 0, 75000, 0, 0, 75000, ... The per-poll deltas
    are therefore wildly uneven (0 / 0 / 75000) even though the drain is steady.
    A 30s bucket contains exactly one commit lump each, so every 30s bucket sees
    75000 rows / 30s = 2500 rows/s -> flat -> CoV ~ 0.
    """
    samples = []
    committed = 0
    t = 0
    # 9 polls => 3 full 30s commit cycles.
    for i in range(1, 10):
        t = i * 10
        if i % 3 == 0:            # commit lands every 30s
            committed += 75000
        samples.append(mk_sample(t, off(committed, 1_000_000)))
    # prepend the t=0 origin sample (committed 0) so the first interval exists.
    return [mk_sample(0, off(0, 1_000_000))] + samples


def test_drain_rate_stability_10s_bucket_is_high_on_lumpy_commits():
    # OLD behaviour (per-poll 10s buckets): the 0/0/75000 commit lumps produce a
    # large CoV on a drain that is actually steady. Pin it high to prove the
    # commit-cadence contamination the refinement removes.
    cov10 = F.compute_drain_rate_stability(
        _lumpy_commit_stream(), bucket_seconds=10)
    assert cov10 is not None and cov10 > 1.0


def test_drain_rate_stability_60s_bucket_is_low_on_lumpy_commits():
    # NEW behaviour: 30s buckets each capture exactly one commit lump, so every
    # bucket reports the true 2500 rows/s -> CoV ~ 0. Wider (60s) buckets are
    # even smoother; here we use 30 to land whole lumps in whole buckets and get
    # an exactly-hand-computable answer.
    cov30 = F.compute_drain_rate_stability(
        _lumpy_commit_stream(), bucket_seconds=30)
    assert cov30 is not None
    assert cov30 == pytest.approx(0.0, abs=1e-9)


def test_drain_rate_stability_default_bucket_is_60s():
    # With no bucket_seconds the default is 60s (BUCKET_SECONDS_DEFAULT) — a
    # single 90s lumpy-commit drain collapses to too few buckets to vary, so the
    # refinement's smoothing is what makes the metric chartable. Assert the
    # default matches the explicit-60 call (i.e. the env default is wired).
    stream = _lumpy_commit_stream()
    assert F.compute_drain_rate_stability(stream) == \
        F.compute_drain_rate_stability(stream, bucket_seconds=60)


def test_bucket_seconds_default_is_exactly_60():
    # Pin the constant itself. The equality test above cannot distinguish 30
    # from 60 for the lumpy stream (both give CoV ~ 0), so assert the VALUE
    # directly: a regression to 30 (the pre-2026-07-10 bucketing) would slip
    # past every behavioural test but change chart comparability.
    assert F.BUCKET_SECONDS_DEFAULT == 60.0


def test_bucket_seconds_default_survives_set_but_empty_env(monkeypatch):
    # `or "60"` idiom (B7 / 083e836 class): the orchestrator may export
    # DRAIN_RATE_STABILITY_BUCKET_SECONDS SET-BUT-EMPTY. The old
    # float(os.environ.get(k, "60")) would see "" and raise ValueError on
    # import; `get(k) or "60"` collapses empty to the default.
    import importlib
    monkeypatch.setenv("DRAIN_RATE_STABILITY_BUCKET_SECONDS", "")
    reloaded = importlib.reload(F)
    try:
        assert reloaded.BUCKET_SECONDS_DEFAULT == 60.0
    finally:
        # Restore the module to the unset-env default for later tests.
        monkeypatch.delenv("DRAIN_RATE_STABILITY_BUCKET_SECONDS", raising=False)
        importlib.reload(F)


def test_drain_rate_stability_zero_bucket_falls_back_to_default():
    # A non-positive bucket width is coerced to the default rather than dividing
    # by zero / making one bucket per interval accidentally.
    stream = _lumpy_commit_stream()
    assert F.compute_drain_rate_stability(stream, bucket_seconds=0) == \
        F.compute_drain_rate_stability(stream, bucket_seconds=60)


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


def test_partition_skew_skips_startup_none_committed():
    # partition 0 has not committed at t=0; treating it as 0 would fabricate a
    # 400 spread. The sample is skipped; only the post-startup spread counts.
    samples = [
        mk_sample(0, {"0": {"committed": None, "end": 500},
                      "1": {"committed": 400, "end": 500}}),
        mk_sample(10, {"0": {"committed": 100, "end": 500},
                       "1": {"committed": 120, "end": 500}}),
    ]
    assert F.compute_partition_skew(samples) == pytest.approx(20.0)


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


# --------------------------------------------------------------------------- #
# guard startup grace (review follow-up 3)
# --------------------------------------------------------------------------- #
def _connect(tasks, state="RUNNING"):
    return {"connector_state": state, "tasks": tasks, "unavailable": False}


def _task(tid, state, worker="wA"):
    return {"id": tid, "state": state, "worker_id": worker}


def test_guards_normal_startup_is_not_flagged():
    # 0 tasks -> 3 UNASSIGNED -> 3 RUNNING -> steady drain: no rebalance, no
    # restart — the classic false-flag pattern the grace exists for.
    samples = [
        mk_sample(0, off(None, 100), connect=_connect([])),
        mk_sample(10, off(None, 100), connect=_connect(
            [_task(0, "UNASSIGNED"), _task(1, "UNASSIGNED"), _task(2, "UNASSIGNED")])),
        mk_sample(20, off(30, 100), connect=_connect(
            [_task(0, "RUNNING"), _task(1, "RUNNING"), _task(2, "RUNNING")])),
        mk_sample(30, off(100, 100), connect=_connect(
            [_task(0, "RUNNING"), _task(1, "RUNNING"), _task(2, "RUNNING")])),
    ]
    g = F.compute_guards(samples, lag_reached_zero=True)
    assert g["flagged"] == 0
    assert g["flag_reason"] == ""
    assert g["counts"][mn.REBALANCE_COUNT] == 0.0
    assert g["counts"][mn.CONNECT_TASK_RESTARTS] == 0.0


def test_guards_post_startup_task_count_change_is_rebalance():
    samples = [
        mk_sample(0, off(0, 100), connect=_connect(
            [_task(0, "RUNNING"), _task(1, "RUNNING"), _task(2, "RUNNING")])),
        mk_sample(10, off(50, 100), connect=_connect(
            [_task(0, "RUNNING"), _task(1, "RUNNING")])),  # a task vanished
    ]
    g = F.compute_guards(samples, lag_reached_zero=True)
    assert g["counts"][mn.REBALANCE_COUNT] == 1.0
    assert "rebalance" in g["flag_reason"].split("|")


def test_guards_post_startup_downstate_to_running_is_restart():
    samples = [
        mk_sample(0, off(0, 100), connect=_connect([_task(0, "RUNNING")])),
        mk_sample(10, off(40, 100), connect=_connect([_task(0, "UNASSIGNED")])),
        mk_sample(20, off(80, 100), connect=_connect([_task(0, "RUNNING")])),
    ]
    g = F.compute_guards(samples, lag_reached_zero=True)
    assert g["counts"][mn.CONNECT_TASK_RESTARTS] == 1.0
    assert "task_restart" in g["flag_reason"].split("|")


def test_guards_expected_tasks_extends_grace_over_partial_startup():
    # with expected_tasks=3, the 1-RUNNING sample is still startup, so the
    # 1 -> 3 count change is NOT a rebalance; a post-grace worker move IS a
    # restart.
    samples = [
        mk_sample(0, off(None, 100), connect=_connect([_task(0, "RUNNING")])),
        mk_sample(10, off(0, 100), connect=_connect(
            [_task(0, "RUNNING"), _task(1, "RUNNING"), _task(2, "RUNNING")])),
        mk_sample(20, off(60, 100), connect=_connect(
            [_task(0, "RUNNING", worker="wB"), _task(1, "RUNNING"), _task(2, "RUNNING")])),
    ]
    g = F.compute_guards(samples, lag_reached_zero=True, expected_tasks=3)
    assert g["counts"][mn.REBALANCE_COUNT] == 0.0
    assert g["counts"][mn.CONNECT_TASK_RESTARTS] == 1.0


def test_guards_failed_during_startup_still_counts():
    # never false-clean: a FAILED task inside the grace window still trips
    # task_retries.
    samples = [
        mk_sample(0, off(None, 100), connect=_connect(
            [_task(0, "FAILED"), _task(1, "UNASSIGNED")])),
    ]
    g = F.compute_guards(samples, lag_reached_zero=False)
    assert g["counts"][mn.TASK_FAILED_COUNT] == 1.0
    assert "task_retries" in g["flag_reason"].split("|")
