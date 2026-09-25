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
"""Unit tests for chaos/schedule.py (IC-5 seeded monkey schedule).

The schedule is the pure, replayable heart of the monkey loop (spec §3.3): a
fixed seed reproduces the fault sequence, intervals, and target windows EXACTLY.
These tests pin the determinism contract (byte-identical stdout for identical
input), the interval/fault/window invariants, the exact round count, and the
loud rejection of an empty or unknown fault list. They also pin the flag
contract: `--aggressive` / `--quorum-loss` may change only the allowed set or
the caps metadata, NEVER the shape of the RNG stream for a given flag
combination.

Run: python3 -m pytest benchmarks/e2e/chaos/tests/test_schedule.py -q
"""
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import schedule  # noqa: E402

SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "schedule.py"
)


def run_cli(*args):
    """Invoke schedule.py as a subprocess; return (returncode, stdout, stderr)."""
    proc = subprocess.run(
        [sys.executable, SCRIPT, *args],
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout, proc.stderr


# --- determinism -----------------------------------------------------------


def test_same_seed_is_byte_identical_stdout():
    a = run_cli("--seed", "42", "--rounds", "20")
    b = run_cli("--seed", "42", "--rounds", "20")
    assert a[0] == 0 and b[0] == 0, (a, b)
    assert a[1] == b[1]
    # and non-empty
    assert a[1].strip()


def test_different_seed_differs():
    _, out_a, _ = run_cli("--seed", "1", "--rounds", "20")
    _, out_b, _ = run_cli("--seed", "2", "--rounds", "20")
    assert out_a != out_b


def test_render_is_pure_and_repeatable():
    o1 = schedule.build_output(
        seed=7, rounds=15, t_min=30.0, t_max=180.0,
        faults=["C1", "C2", "C3", "C4", "C5"],
        quorum_loss=False, aggressive=False,
    )
    o2 = schedule.build_output(
        seed=7, rounds=15, t_min=30.0, t_max=180.0,
        faults=["C1", "C2", "C3", "C4", "C5"],
        quorum_loss=False, aggressive=False,
    )
    assert schedule.render(o1) == schedule.render(o2)


# --- output shape / invariants --------------------------------------------


def _parse(*args):
    rc, out, err = run_cli(*args)
    assert rc == 0, err
    return json.loads(out)


def test_top_level_shape():
    doc = _parse("--seed", "42", "--rounds", "5")
    assert doc["seed"] == 42
    assert isinstance(doc["rounds"], list)


def test_exact_round_count():
    for r in (1, 5, 20, 37):
        doc = _parse("--seed", "42", "--rounds", str(r))
        assert len(doc["rounds"]) == r
        assert [x["round"] for x in doc["rounds"]] == list(range(1, r + 1))


def test_intervals_within_bounds():
    doc = _parse("--seed", "99", "--rounds", "50", "--t-min", "30", "--t-max", "180")
    for rnd in doc["rounds"]:
        assert 30.0 <= rnd["wait_seconds"] <= 180.0, rnd


def test_intervals_within_narrow_bounds():
    doc = _parse("--seed", "3", "--rounds", "40", "--t-min", "10", "--t-max", "12")
    for rnd in doc["rounds"]:
        assert 10.0 <= rnd["wait_seconds"] <= 12.0, rnd


def test_fault_types_subset_of_enabled():
    doc = _parse("--seed", "5", "--rounds", "60", "--faults", "C1,C4")
    enabled = set(doc["faults_enabled"])
    assert enabled == {"C1", "C4"}
    seen = {rnd["fault_type"] for rnd in doc["rounds"]}
    assert seen <= enabled


def test_target_windows_are_valid():
    doc = _parse("--seed", "5", "--rounds", "60")
    for rnd in doc["rounds"]:
        assert rnd["target_window"] in ("W1", "W2", "W3"), rnd


def test_all_enabled_faults_reachable_over_many_rounds():
    # With the full set and many rounds the choice() should exercise all faults.
    doc = _parse("--seed", "12345", "--rounds", "300")
    seen = {rnd["fault_type"] for rnd in doc["rounds"]}
    assert seen == {"C1", "C2", "C3", "C4", "C5"}


# --- loud rejection --------------------------------------------------------


def test_empty_fault_list_rejected_loudly():
    rc, out, err = run_cli("--seed", "42", "--faults", "")
    assert rc != 0
    assert out.strip() == ""
    assert "fault" in err.lower()


def test_whitespace_only_fault_list_rejected():
    rc, _, err = run_cli("--seed", "42", "--faults", " , ")
    assert rc != 0
    assert "fault" in err.lower()


def test_unknown_fault_rejected_loudly():
    rc, out, err = run_cli("--seed", "42", "--faults", "C1,C9")
    assert rc != 0
    assert out.strip() == ""
    assert "C9" in err


def test_bad_bounds_rejected():
    rc, _, err = run_cli("--seed", "42", "--t-min", "180", "--t-max", "30")
    assert rc != 0
    assert err.strip()


def test_nonpositive_rounds_rejected():
    rc, _, err = run_cli("--seed", "42", "--rounds", "0")
    assert rc != 0
    assert err.strip()


# --- flag contract: allowed set / caps only, never the RNG stream shape ----


def test_aggressive_changes_only_caps_not_the_sequence():
    base = _parse("--seed", "42", "--rounds", "20")
    aggr = _parse("--seed", "42", "--rounds", "20", "--aggressive")
    # The seeded per-round sequence is byte-identical...
    assert base["rounds"] == aggr["rounds"]
    assert base["faults_enabled"] == aggr["faults_enabled"]
    # ...only the caps metadata differs.
    assert base["caps"] != aggr["caps"]


def test_quorum_loss_changes_allowed_set_and_caps():
    base = _parse("--seed", "42", "--rounds", "20")
    ql = _parse("--seed", "42", "--rounds", "20", "--quorum-loss")
    assert ql["faults_enabled"] == ["quorum_loss"]
    assert base["faults_enabled"] != ql["faults_enabled"]
    assert all(rnd["fault_type"] == "quorum_loss" for rnd in ql["rounds"])
    assert base["caps"] != ql["caps"]


def test_quorum_loss_stream_shape_matches_a_single_fault_run():
    # "never the RNG stream shape for a given flag combination": the wait/window
    # draws must be identical to a default run whose allowed set is a single
    # fault (same 3-draws-per-round order), differing only in the chosen fault.
    ql = _parse("--seed", "42", "--rounds", "20", "--quorum-loss")
    single = _parse("--seed", "42", "--rounds", "20", "--faults", "C1")
    ql_waits = [(r["round"], r["wait_seconds"], r["target_window"]) for r in ql["rounds"]]
    s_waits = [(r["round"], r["wait_seconds"], r["target_window"]) for r in single["rounds"]]
    assert ql_waits == s_waits
