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
"""Sampler loop tests with injected sources + fake clock (no live cluster)."""
import sampler


class Clock:
    def __init__(self):
        self.t = 1000.0

    def now(self):
        return self.t

    def sleep(self, dt):
        self.t += dt


def test_loop_stops_at_lag_zero(tmp_path):
    clk = Clock()
    lags = iter([
        {"0": {"committed": 0, "end": 100}},
        {"0": {"committed": 60, "end": 100}},
        {"0": {"committed": 100, "end": 100}},  # lag 0 -> stop
    ])
    out = str(tmp_path / "s.jsonl")
    res = sampler.run_sampler(
        out_path=out,
        offsets_source=lambda: next(lags),
        connect_source=lambda: {"unavailable": True},
        jmx_source=lambda: {"unavailable": True},
        pod_source=lambda: {"unavailable": True},
        poll_interval=10.0, timeout=3600.0,
        sleep=clk.sleep, now=clk.now)
    assert res["drained"] is True
    assert res["timed_out"] is False
    assert res["samples"] == 3
    loaded = sampler.load_samples(out)
    assert len(loaded) == 3


def test_loop_times_out(tmp_path):
    clk = Clock()
    out = str(tmp_path / "s.jsonl")
    # lag never reaches 0; loop must give up at timeout.
    res = sampler.run_sampler(
        out_path=out,
        offsets_source=lambda: {"0": {"committed": 10, "end": 100}},
        connect_source=lambda: {"unavailable": True},
        jmx_source=lambda: {"unavailable": True},
        pod_source=lambda: {"unavailable": True},
        poll_interval=10.0, timeout=25.0,
        sleep=clk.sleep, now=clk.now)
    assert res["drained"] is False
    assert res["timed_out"] is True


def test_loop_tolerates_offsets_unavailable(tmp_path):
    clk = Clock()
    seq = iter([None, None, {"0": {"committed": 100, "end": 100}}])
    out = str(tmp_path / "s.jsonl")
    res = sampler.run_sampler(
        out_path=out,
        offsets_source=lambda: next(seq),
        connect_source=lambda: {"unavailable": True},
        jmx_source=lambda: {"unavailable": True},
        pod_source=lambda: {"unavailable": True},
        poll_interval=5.0, timeout=3600.0,
        sleep=clk.sleep, now=clk.now)
    # None offsets do not end the drain; loop continues until real lag 0.
    assert res["drained"] is True
    assert res["samples"] == 3
