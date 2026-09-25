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
"""Crash-window racer for the chaos monkey loop (T9, spec §3.4).

Tails the connector's log stream, watches the state-transition marker grammar,
fires the supplied kill command inside a target crash window, and reports the
window it ACTUALLY landed in — recorded, never assumed (§3.4).

The crash windows bracket `Processing.doLogic()`
(`Processing.java:131-266`): setStateRecord(BEFORE) -> doInsert -> setStateRecord(AFTER).
  * W1  — after BEFORE, before the insert is issued
  * W2  — mid-insert (insert issued, response not yet in)
  * W3  — after the insert response, before AFTER
  * post_after — the AFTER state write has landed (batch committed)

Marker grammar (grounded in the checked-out sink source):
  * exactly-once: the KeeperStateProvider state writes are INFO
    "Write state record: ...BEFORE_PROCESSING/AFTER_PROCESSING"
    (`KeeperStateProvider.java:141`) and bracket the insert; the DEBUG
    "doInsert - Records" (`Processing.java:67`) marks W2 when DEBUG is on, and
    the INFO insert-completion line "topic: ... batchSize: ... push stream ms"
    (`ClickHouseWriter.java:1040` et al.) marks W3.
  * at-least-once: no state writes — the INFO "doLogic - Topic:"
    (`Processing.java:152`) opens W1 and the "doInsert" marker is W2 (§3.4). If
    no marker is ever seen the racer falls back to interval-only timing and
    records `na`.

The racer fires the kill command EXACTLY once. If the target window never opens
within `--marker-timeout` (or the stream ends first), it fires anyway
(interval-only fallback) and records the window it actually reached — `na` when
no marker was ever seen.

stdout: one JSON object (machine output). stderr: logging.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time

WINDOWS = ("W1", "W2", "W3")
EXIT_USAGE = 2

# --- marker regexes (case-sensitive substrings from the sink source) --------
RE_STATE_BEFORE = re.compile(r"Write state record.*BEFORE_PROCESSING")
RE_STATE_AFTER = re.compile(r"Write state record.*AFTER_PROCESSING")
RE_DO_INSERT = re.compile(r"doInsert - Records")
# insert completed (response received / INFO timing line), before AFTER:
RE_INSERT_DONE = re.compile(r"Response Summary - Written|batchSize:")
RE_DOLOGIC = re.compile(r"doLogic - Topic")


def log(msg: str) -> None:
    print(f"[window_racer] {msg}", file=sys.stderr, flush=True)


def classify_marker(line: str, mode: str) -> str | None:
    """Map a log line to the window it OPENS, or None if it is not a marker.

    Order matters: the BEFORE/AFTER state writes are the exactly-once bracket;
    doInsert is W2; the insert-completion line is W3. In at-least-once there are
    no state writes, so the INFO doLogic line opens W1 instead of BEFORE.
    """
    if RE_STATE_BEFORE.search(line):
        return "W1"
    if RE_STATE_AFTER.search(line):
        return "post_after"
    if RE_DO_INSERT.search(line):
        return "W2"
    if RE_INSERT_DONE.search(line):
        return "W3"
    if mode == "at-least-once" and RE_DOLOGIC.search(line):
        return "W1"
    return None


def race(
    lines,
    target: str,
    mode: str,
    fire,
    marker_timeout: float = 120.0,
    clock=None,
):
    """Consume `lines` (an iterable of str, optionally yielding None as a poll
    tick), firing `fire()` exactly once when the target window opens.

    Returns a result dict: fired, fault_window, target_window, mode,
    markers_seen, final_position, fallback.

    `clock` (a no-arg callable returning seconds) enables the interval-only
    fallback: if the target window has not opened within `marker_timeout`, fire
    anyway. When `clock` is None the fallback triggers only at stream EOF.
    """
    position = "na"
    markers_seen = 0
    fired = False
    fired_window: str | None = None
    fallback = False
    start = clock() if clock is not None else None
    it = iter(lines)

    while True:
        if not fired and clock is not None and (clock() - start) >= marker_timeout:
            fired_window = position if markers_seen else "na"
            fallback = True
            fire()
            fired = True
            break
        try:
            line = next(it)
        except StopIteration:
            break
        if line is None:  # poll tick with no data — loop to re-check the clock
            continue
        window = classify_marker(line, mode)
        if window is None:
            continue
        markers_seen += 1
        position = window
        if window == target:
            fired_window = window
            fire()
            fired = True
            break

    if not fired:
        # Stream ended before the target window opened: interval-only fallback.
        fired_window = position if markers_seen else "na"
        fallback = True
        fire()
        fired = True

    return {
        "fired": fired,
        "fault_window": fired_window,
        "target_window": target,
        "mode": mode,
        "markers_seen": markers_seen,
        "final_position": position,
        "fallback": fallback,
    }


def iter_stream(fileobj, poll: float):
    """Yield stripped lines from a live stream, yielding None on a poll tick so
    the caller can re-check the marker timeout even while the stream is idle.
    Ends (StopIteration) at EOF."""
    import select

    while True:
        try:
            ready, _, _ = select.select([fileobj], [], [], poll)
        except (ValueError, OSError):
            # not selectable (e.g. a closed fd) — fall back to a plain read
            ready = [fileobj]
        if not ready:
            yield None
            continue
        line = fileobj.readline()
        if line == "":
            return
        yield line.rstrip("\n")


def _make_fire(kill_cmd: str):
    """Build the one-shot fire callable that runs the kill command via a shell."""

    def _fire():
        log(f"firing kill command: {kill_cmd}")
        rc = subprocess.call(kill_cmd, shell=True)
        if rc != 0:
            log(f"kill command exited non-zero ({rc})")

    return _fire


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="window_racer.py",
        description="Fire a kill command inside a target crash window (§3.4).",
    )
    parser.add_argument("--target-window", required=True,
                        help="target crash window (W1|W2|W3)")
    parser.add_argument("--mode", default="exactly-once",
                        choices=["exactly-once", "at-least-once"])
    parser.add_argument("--kill-cmd", required=True,
                        help="shell command to fire inside the window")
    parser.add_argument("--log-file", default=None,
                        help="read the log stream from this file ('-' = stdin)")
    parser.add_argument("--logs-cmd", default=None,
                        help="spawn this shell command and tail its stdout "
                             "(e.g. a `kubectl logs -f` invocation)")
    parser.add_argument("--marker-timeout", type=float, default=120.0,
                        help="interval-only fallback timeout in seconds")
    parser.add_argument("--poll", type=float, default=0.2,
                        help="stream read poll interval in seconds")
    parser.add_argument("--out", default="-",
                        help="write the result JSON here ('-' = stdout)")
    args = parser.parse_args(argv)

    if args.target_window not in WINDOWS:
        log(f"invalid --target-window {args.target_window!r}; expected one of "
            + ",".join(WINDOWS))
        return EXIT_USAGE
    if not args.log_file and not args.logs_cmd:
        log("one of --log-file or --logs-cmd is required")
        return EXIT_USAGE

    fire = _make_fire(args.kill_cmd)
    proc = None
    fileobj = None
    opened = False
    try:
        if args.logs_cmd:
            log(f"tailing log stream: {args.logs_cmd}")
            proc = subprocess.Popen(
                args.logs_cmd, shell=True, stdout=subprocess.PIPE, text=True
            )
            fileobj = proc.stdout
        elif args.log_file == "-":
            fileobj = sys.stdin
        else:
            fileobj = open(args.log_file, "r")
            opened = True
        stream = iter_stream(fileobj, args.poll)
        result = race(
            stream,
            target=args.target_window,
            mode=args.mode,
            fire=fire,
            marker_timeout=args.marker_timeout,
            clock=time.monotonic,
        )
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
        if opened and fileobj is not None:
            fileobj.close()

    payload = json.dumps(result, sort_keys=True)
    if args.out == "-":
        print(payload)
    else:
        with open(args.out, "w") as fh:
            fh.write(payload + "\n")
    log(f"landed fault_window={result['fault_window']} "
        f"(target={result['target_window']}, fallback={result['fallback']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
