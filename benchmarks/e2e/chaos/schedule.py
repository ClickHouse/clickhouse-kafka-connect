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
"""Pre-generate the whole monkey run's fault schedule from a seed (IC-5, §3.3).

This is the pure, replayable heart of the monkey loop: given a seed it emits the
ENTIRE run's fault sequence + inter-fault intervals + target crash windows, so a
fixed seed reproduces the run exactly (spec §3.3 "Determinism"). The realized
crash *window* landed at runtime depends on live timing and is recorded
separately (`fault_window`, §3.4) — here we only emit the *target* window.

Runtime concurrency caps (§3.3: ≤1 CH pod down, ≤1 fault in the default profile)
are enforced downstream by DELAYING a scheduled fault until the cap clears —
never by re-rolling or skipping. So this script emits the pure seeded sequence
and nothing more; only realized timings drift at runtime (recorded per round).

Determinism contract: identical CLI input ⇒ byte-identical stdout. The RNG is
`random.Random(seed)` and NOTHING else. `--aggressive` / `--quorum-loss` change
only the allowed fault set and the caps metadata — never the shape of the RNG
stream (the per-round draw order: interval, fault, window). For a given flag
combination the output is fully reproducible.

CLI:
  --seed S            (required) integer seed; the whole run's replay key
  --rounds R          number of rounds (default 20; must be >= 1)
  --t-min a --t-max b inter-fault interval bounds in seconds (default 30 / 180)
  --faults C1,C2,..   allowed fault set (default C1,C2,C3,C4,C5); empty or
                      unknown token is rejected loudly (exit 2)
  --aggressive        opt-in §3.3 profile: raises the concurrency cap metadata
                      (still <= 1 CH pod down); sequence unchanged
  --quorum-loss       opt-in §3.7 experiment: the allowed set becomes the single
                      `quorum_loss` fault and the caps metadata reflect the
                      2-keeper kill; the RNG stream shape is unchanged

stdout: one JSON object (machine output). stderr: logging / errors.
"""
from __future__ import annotations

import argparse
import json
import random
import sys

KNOWN_FAULTS = ("C1", "C2", "C3", "C4", "C5")
QUORUM_LOSS_FAULT = "quorum_loss"
# Crash windows around Processing.doLogic() (§3.4): W1 (after BEFORE, pre-insert),
# W2 (mid-insert), W3 (after insert, pre-AFTER).
WINDOWS = ("W1", "W2", "W3")

EXIT_USAGE = 2


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def parse_faults(raw: str) -> list[str]:
    """Parse a comma-separated fault list. Rejects empty and unknown tokens loudly."""
    faults = [tok.strip() for tok in raw.split(",")]
    faults = [tok for tok in faults if tok]
    if not faults:
        raise ValueError("empty fault list: --faults must name at least one fault")
    unknown = [tok for tok in faults if tok not in KNOWN_FAULTS]
    if unknown:
        raise ValueError(
            "unknown fault(s) "
            + ",".join(unknown)
            + "; known faults are "
            + ",".join(KNOWN_FAULTS)
        )
    return faults


def resolve_allowed(faults: list[str], quorum_loss: bool) -> list[str]:
    """The set `rng.choice` draws from. Quorum-loss (§3.7) is a dedicated
    experiment: every round is a `quorum_loss` injection."""
    if quorum_loss:
        return [QUORUM_LOSS_FAULT]
    return list(faults)


def resolve_caps(aggressive: bool, quorum_loss: bool) -> dict:
    """Concurrency-cap metadata (§3.3 / §3.7). Enforced downstream by delaying,
    not by this script — recorded here so the realized run can honor it."""
    if quorum_loss:
        # §3.7: deliberately breaks the 3-node quorum by killing 2 keeper pods,
        # overriding the default <=1-CH-kill cap.
        return {"max_concurrent_faults": 1, "max_ch_pods_down": 2, "quorum_loss": True}
    if aggressive:
        # §3.3: concurrent Connect+CH kill allowed, still <= 1 CH pod down.
        return {"max_concurrent_faults": 2, "max_ch_pods_down": 1, "quorum_loss": False}
    return {"max_concurrent_faults": 1, "max_ch_pods_down": 1, "quorum_loss": False}


def build_rounds(
    seed: int, rounds: int, t_min: float, t_max: float, allowed: list[str]
) -> list[dict]:
    """Pure seeded sequence. Per-round draw order is fixed (interval, fault,
    window) so the RNG stream shape never depends on the flag combination."""
    rng = random.Random(seed)
    out = []
    for r in range(1, rounds + 1):
        raw_wait = rng.uniform(t_min, t_max)
        # Round for readability; clamp guards fractional bounds against a
        # round() that would step just outside [t_min, t_max].
        wait = min(t_max, max(t_min, round(raw_wait, 1)))
        fault = rng.choice(allowed)
        window = rng.choice(WINDOWS)
        out.append(
            {
                "round": r,
                "wait_seconds": wait,
                "fault_type": fault,
                "target_window": window,
            }
        )
    return out


def build_output(
    seed: int,
    rounds: int,
    t_min: float,
    t_max: float,
    faults: list[str],
    quorum_loss: bool,
    aggressive: bool,
) -> dict:
    if rounds < 1:
        raise ValueError("--rounds must be >= 1")
    if t_min > t_max:
        raise ValueError(f"--t-min ({t_min}) must be <= --t-max ({t_max})")
    if t_min < 0:
        raise ValueError("--t-min must be >= 0")
    allowed = resolve_allowed(faults, quorum_loss)
    return {
        "seed": seed,
        "profile": "aggressive" if aggressive else "default",
        "faults_enabled": allowed,
        "caps": resolve_caps(aggressive, quorum_loss),
        "rounds": build_rounds(seed, rounds, t_min, t_max, allowed),
    }


def render(output: dict) -> str:
    """Deterministic JSON. sort_keys makes stdout byte-identical regardless of
    dict construction order."""
    return json.dumps(output, sort_keys=True, ensure_ascii=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="schedule.py",
        description="Pre-generate a seeded monkey fault schedule (IC-5, §3.3).",
    )
    parser.add_argument("--seed", type=int, required=True, help="integer seed (replay key)")
    parser.add_argument("--rounds", type=int, default=20, help="number of rounds (>= 1)")
    parser.add_argument("--t-min", type=float, default=30.0, help="min inter-fault interval (s)")
    parser.add_argument("--t-max", type=float, default=180.0, help="max inter-fault interval (s)")
    parser.add_argument(
        "--faults",
        default=",".join(KNOWN_FAULTS),
        help="comma-separated allowed fault set (default C1,C2,C3,C4,C5)",
    )
    parser.add_argument("--aggressive", action="store_true", help="§3.3 aggressive cap profile")
    parser.add_argument("--quorum-loss", action="store_true", help="§3.7 quorum-loss experiment")
    args = parser.parse_args(argv)

    try:
        faults = parse_faults(args.faults)
        output = build_output(
            seed=args.seed,
            rounds=args.rounds,
            t_min=args.t_min,
            t_max=args.t_max,
            faults=faults,
            quorum_loss=args.quorum_loss,
            aggressive=args.aggressive,
        )
    except ValueError as exc:
        log(f"schedule.py: {exc}")
        return EXIT_USAGE

    print(render(output))
    return 0


if __name__ == "__main__":
    sys.exit(main())
