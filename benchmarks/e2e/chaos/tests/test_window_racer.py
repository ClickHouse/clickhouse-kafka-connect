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
"""Unit tests for chaos/window_racer.py (T9, spec §3.4).

The window racer tails the connector log stream, watches the state-marker
grammar, fires the supplied kill command inside a target crash window, and
records the window it ACTUALLY landed in (`W1|W2|W3|post_after|na`) — recorded,
never assumed (§3.4). These tests pin:
  * classification of every window over synthetic streams (exactly-once state
    writes + at-least-once doInsert markers),
  * the interval-only fallback (no target marker seen in time),
  * marker-never-seen  ==> na,
  * the racer fires the kill command EXACTLY once,
  * the CLI end-to-end (reads a log file, fires a real shell command).

Run: python3 -m pytest benchmarks/e2e/chaos/tests/test_window_racer.py -q
"""
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import window_racer as wr  # noqa: E402

SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "window_racer.py"
)

# --- realistic connector log lines (grounded in the cited source) ----------
L_DOLOGIC = (
    "INFO doLogic - Topic: [hits-chaos], Partition: [0], MinOffset: [0], "
    "MaxOffset: [99], Records: [100]"
)
L_BEFORE = (
    "INFO Write state record: StateRecord{topic='hits-chaos-0', "
    "state='BEFORE_PROCESSING'}"
)
L_INSERT = "DEBUG doInsert - Records: [100] - QueryId[abc]"
L_INSERT_DONE = (
    "INFO topic: hits-chaos partition: 0 batchSize: 100 push stream ms: 5 "
    "data ms: 3 send ms: 12 (QueryId: [abc])"
)
L_AFTER = (
    "INFO Write state record: StateRecord{topic='hits-chaos-0', "
    "state='AFTER_PROCESSING'}"
)
L_NOISE = "INFO some unrelated worker log line about rebalance"


class Spy:
    """A fire callable that records how many times it was invoked."""

    def __init__(self):
        self.count = 0

    def __call__(self):
        self.count += 1


# --------------------------------------------------------------------------- #
# classify_marker
# --------------------------------------------------------------------------- #
def test_classify_exactly_once_markers():
    assert wr.classify_marker(L_BEFORE, "exactly-once") == "W1"
    assert wr.classify_marker(L_INSERT, "exactly-once") == "W2"
    assert wr.classify_marker(L_INSERT_DONE, "exactly-once") == "W3"
    assert wr.classify_marker(L_AFTER, "exactly-once") == "post_after"
    assert wr.classify_marker(L_NOISE, "exactly-once") is None
    # a plain doLogic line is NOT a W1 marker in exactly-once (BEFORE is)
    assert wr.classify_marker(L_DOLOGIC, "exactly-once") is None


def test_classify_at_least_once_markers():
    # at-least-once has no state writes: doLogic opens W1, doInsert is W2
    assert wr.classify_marker(L_DOLOGIC, "at-least-once") == "W1"
    assert wr.classify_marker(L_INSERT, "at-least-once") == "W2"
    assert wr.classify_marker(L_INSERT_DONE, "at-least-once") == "W3"
    assert wr.classify_marker(L_NOISE, "at-least-once") is None


# --------------------------------------------------------------------------- #
# race() — window classification over synthetic streams
# --------------------------------------------------------------------------- #
def _race(lines, target, mode="exactly-once", **kw):
    spy = Spy()
    result = wr.race(lines, target=target, mode=mode, fire=spy, **kw)
    return result, spy


def test_race_lands_w1():
    stream = [L_DOLOGIC, L_BEFORE, L_INSERT, L_INSERT_DONE, L_AFTER]
    res, spy = _race(stream, "W1")
    assert res["fault_window"] == "W1"
    assert res["fired"] is True
    assert res["fallback"] is False
    assert spy.count == 1


def test_race_lands_w2():
    stream = [L_DOLOGIC, L_BEFORE, L_INSERT, L_INSERT_DONE, L_AFTER]
    res, spy = _race(stream, "W2")
    assert res["fault_window"] == "W2"
    assert res["fallback"] is False
    assert spy.count == 1


def test_race_lands_w3():
    stream = [L_DOLOGIC, L_BEFORE, L_INSERT, L_INSERT_DONE, L_AFTER]
    res, spy = _race(stream, "W3")
    assert res["fault_window"] == "W3"
    assert spy.count == 1


def test_race_records_post_after_when_target_window_already_passed():
    # target W1, but the stream only ever shows the AFTER marker (we started
    # tailing mid-batch): the window we wanted never re-opens; at EOF we record
    # the position we actually reached — post_after — not a false W1.
    stream = [L_AFTER]
    res, spy = _race(stream, "W1")
    assert res["fault_window"] == "post_after"
    assert res["fallback"] is True
    assert spy.count == 1


def test_race_marker_never_seen_is_na():
    stream = [L_NOISE, L_NOISE, L_NOISE]
    res, spy = _race(stream, "W2")
    assert res["fault_window"] == "na"
    assert res["fallback"] is True
    assert res["markers_seen"] == 0
    assert spy.count == 1


def test_race_at_least_once_w2_on_doinsert():
    stream = [L_DOLOGIC, L_INSERT, L_INSERT_DONE]
    res, spy = _race(stream, "W2", mode="at-least-once")
    assert res["fault_window"] == "W2"
    assert spy.count == 1


def test_race_fires_exactly_once_across_multiple_batches():
    # two full batch cycles; target W1 must fire only on the FIRST open.
    stream = [
        L_BEFORE, L_INSERT, L_INSERT_DONE, L_AFTER,
        L_BEFORE, L_INSERT, L_INSERT_DONE, L_AFTER,
    ]
    res, spy = _race(stream, "W1")
    assert res["fault_window"] == "W1"
    assert spy.count == 1, "the kill command must fire exactly once"


def test_race_interval_only_fallback_on_clock_timeout():
    # lines keep arriving but the target window never opens; once the marker
    # timeout elapses (fake clock), the racer fires anyway (interval-only) and
    # records the position it had actually reached.
    stream = [L_DOLOGIC, L_BEFORE, L_INSERT]  # target W3 never appears
    # start=0; the first three loop checks stay under the timeout so BEFORE/INSERT
    # are read, then the clock jumps past marker_timeout to trip the fallback.
    ticks = iter([0.0, 0.0, 0.0, 0.0, 100.0, 200.0])
    res, spy = _race(
        stream, "W3", marker_timeout=10.0, clock=lambda: next(ticks)
    )
    assert res["fired"] is True
    assert res["fallback"] is True
    assert res["fault_window"] in ("W1", "W2")  # the position actually reached
    assert spy.count == 1


def test_race_markers_seen_counted():
    stream = [L_DOLOGIC, L_BEFORE, L_INSERT, L_INSERT_DONE, L_AFTER]
    res, _ = _race(stream, "W3")
    # BEFORE, INSERT, INSERT_DONE all matched before we fired at W3
    assert res["markers_seen"] == 3


# --------------------------------------------------------------------------- #
# CLI end-to-end
# --------------------------------------------------------------------------- #
def _write_log(tmp_path, lines):
    p = tmp_path / "connector.log"
    p.write_text("\n".join(lines) + "\n")
    return p


def test_cli_fires_kill_command_exactly_once(tmp_path):
    logf = _write_log(tmp_path, [L_DOLOGIC, L_BEFORE, L_INSERT, L_INSERT_DONE, L_AFTER])
    fired = tmp_path / "fired.log"
    proc = subprocess.run(
        [
            sys.executable, SCRIPT,
            "--log-file", str(logf),
            "--target-window", "W1",
            "--mode", "exactly-once",
            "--kill-cmd", f"printf 'x\\n' >> {fired}",
        ],
        capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr
    doc = json.loads(proc.stdout)
    assert doc["fault_window"] == "W1"
    assert doc["fired"] is True
    assert fired.read_text().count("x") == 1, "kill command must fire exactly once"


def test_cli_na_when_no_markers(tmp_path):
    logf = _write_log(tmp_path, [L_NOISE, L_NOISE])
    fired = tmp_path / "fired.log"
    proc = subprocess.run(
        [
            sys.executable, SCRIPT,
            "--log-file", str(logf),
            "--target-window", "W2",
            "--kill-cmd", f"printf 'x\\n' >> {fired}",
        ],
        capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr
    doc = json.loads(proc.stdout)
    assert doc["fault_window"] == "na"
    assert fired.read_text().count("x") == 1


def test_cli_rejects_bad_window(tmp_path):
    logf = _write_log(tmp_path, [L_BEFORE])
    proc = subprocess.run(
        [
            sys.executable, SCRIPT,
            "--log-file", str(logf),
            "--target-window", "W9",
            "--kill-cmd", "true",
        ],
        capture_output=True, text=True,
    )
    assert proc.returncode != 0


if __name__ == "__main__":
    sys.exit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
