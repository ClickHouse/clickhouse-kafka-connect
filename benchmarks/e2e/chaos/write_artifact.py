#!/usr/bin/env python3
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
"""Assemble the per-run chaos result artifact (IC-9 / spec §11).

This is the chaos-path replacement for the pair path's perf.* capture + DWH
export (decision §10.2): it writes ONE JSON file per run and returns its path.
It NEVER inserts into perf.*, never exports to a DWH, and never touches the
network — it is a pure assembler over three inputs:

  (a) run-level fields  — the §4 runtime-key vocabulary, read from the
      environment (one env var per field, UPPER_CASED) or passed in directly.
  (b) the rounds JSONL  — one IC-4 round record per line (order preserved).
  (c) the oracle JSON   — the object emitted by `check_integrity.py --direct`
      (IC-6): rows/unique/duplicate/loss/dlq_depth + verdict.

It HARD-FAILS (ValueError) on any missing mandatory field, a malformed round
line, or an outcome/run_conclusion/verdict/enum value outside its vocabulary —
a corrupt result must be loud, never silently written.

The artifact schema is IC-9 verbatim. Two deliberate transforms:
  - `integrity_ok` is DERIVED (`verdict == "PASS"`), never taken from input.
  - each round is PROJECTED to the nine IC-9 round fields; IC-4's diagnostic
    `evidence` object is dropped so the artifact matches IC-9 exactly.

stdout = the written artifact path (machine output); stderr = logging.

Required env (one per §4 run-level key), consumed by main():
  CHAOS_ID, CHAOS_MODE, CHAOS_SEED, CHAOS_ROUNDS, ROUNDS_COMPLETED,
  DELIVERY_MODE, EXACTLY_ONCE, CONNECTOR, ENVIRONMENT_CLASS,
  IGNORE_PARTITIONS_WHEN_BATCHING, CH_VERSION, CH_IMAGE_DIGEST, ARM_IMAGE,
  PRODUCER_IMAGE, OUTCOME, RUN_CONCLUSION
Args: --rounds <rounds.jsonl>  --oracle <oracle.json>  --out <dir>

Run: python3 chaos/write_artifact.py --rounds R.jsonl --oracle O.json --out DIR
"""
import argparse
import json
import os
import sys

# --- Vocabularies (IC-6, IC-9, spec §4) ------------------------------------ #
# Verdicts: chaos_verdict() yields the first three (IC-6); CHECK_ERROR is the
# exit-3 read-failure class check_integrity.py --direct may emit, and the
# artifact is written on every path (IC-3), so it is a valid recorded verdict.
VERDICTS = frozenset({
    "PASS", "MISMATCH", "UNVERIFIED_FAULT_NOT_OBSERVED", "CHECK_ERROR",
})
# Run-level outcome (maps to IC-3 exit classes 0/1/3). The design writes
# outcome='failed' (§3.6a) and classifies infra stalls as 'integrity_unverified'
# (§3.6a/§5); 'passed' is the clean complement.
OUTCOMES = frozenset({"passed", "failed", "integrity_unverified"})
RUN_CONCLUSIONS = frozenset({"quiesced", "t_recover_timeout", "t_settle_timeout"})
CHAOS_MODES = frozenset({"monkey", "smoke", "quorum_loss"})
DELIVERY_MODES = frozenset({"at_least_once", "exactly_once"})
BINARY = frozenset({"0", "1"})

# --- Mandatory field sets --------------------------------------------------- #
# Run-level fields (env/args). Order here defines their order in the artifact.
RUN_FIELDS = (
    "chaos_id", "chaos_mode", "chaos_seed", "chaos_rounds", "rounds_completed",
    "delivery_mode", "exactly_once", "connector", "environment_class",
    "ignore_partitions_when_batching", "ch_version", "ch_image_digest",
    "arm_image", "producer_image", "outcome", "run_conclusion",
)
# Oracle fields (IC-6 check_integrity.py --direct stdout). 'reason' is emitted
# by the oracle but is NOT part of IC-9, so it is not carried into the artifact.
ORACLE_FIELDS = (
    "rows_expected", "rows_delivered", "unique_delivered", "unique_expected",
    "duplicate_rows", "loss", "dlq_depth", "verdict",
)
# Per-round fields (IC-9 rounds[]). IC-4's 'evidence' is intentionally excluded.
ROUND_FIELDS = (
    "round", "fault_type", "fault_window", "inject_ts", "recovery_seconds",
    "task_restart_count", "insert_errors_during_fault", "ch_dedup_dropped_blocks",
    "fault_observed",
)

# Enumerated run-level fields → their allowed value sets.
_ENUM_RUN_FIELDS = {
    "chaos_mode": CHAOS_MODES,
    "delivery_mode": DELIVERY_MODES,
    "exactly_once": BINARY,
    "ignore_partitions_when_batching": BINARY,
    "outcome": OUTCOMES,
    "run_conclusion": RUN_CONCLUSIONS,
}


def _require_present(source: dict, fields, ctx: str) -> None:
    """Raise ValueError if any field is absent or None."""
    missing = [f for f in fields if source.get(f) is None]
    if missing:
        raise ValueError(
            f"{ctx}: missing mandatory field(s): {', '.join(sorted(missing))}"
        )


def _require_in_vocab(value, vocab, field: str) -> None:
    if value not in vocab:
        raise ValueError(
            f"{field}={value!r} is outside the allowed vocabulary "
            f"{{{', '.join(sorted(vocab))}}}"
        )


def parse_round_line(line: str, lineno=None) -> dict:
    """Parse one IC-4 round JSONL line and project it to the nine IC-9 round
    fields. Raises ValueError on malformed JSON or a missing mandatory field."""
    where = f" (line {lineno})" if lineno is not None else ""
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ValueError(f"malformed round line{where}: {exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError(f"malformed round line{where}: not a JSON object")
    _require_present(obj, ROUND_FIELDS, f"round record{where}")
    return {k: obj[k] for k in ROUND_FIELDS}


def load_rounds(path: str) -> list:
    """Read the rounds JSONL, preserving file order. Blank lines are skipped;
    any non-blank malformed line hard-fails."""
    rounds = []
    with open(path, "r", encoding="utf-8") as fh:
        for i, raw in enumerate(fh, start=1):
            if raw.strip() == "":
                continue
            rounds.append(parse_round_line(raw, lineno=i))
    return rounds


def load_oracle(path: str) -> dict:
    """Read the oracle JSON (IC-6) and return only the IC-9 oracle fields."""
    with open(path, "r", encoding="utf-8") as fh:
        obj = json.load(fh)
    if not isinstance(obj, dict):
        raise ValueError(f"oracle JSON at {path} is not a JSON object")
    _require_present(obj, ORACLE_FIELDS, "oracle JSON")
    return {k: obj[k] for k in ORACLE_FIELDS}


def artifact_filename(chaos_id: str, chaos_mode: str, exactly_once: str) -> str:
    """chaos-result-<chaos_id>-<mode>-eo<0|1>.json (IC-9)."""
    _require_in_vocab(chaos_mode, CHAOS_MODES, "chaos_mode")
    _require_in_vocab(exactly_once, BINARY, "exactly_once")
    return f"chaos-result-{chaos_id}-{chaos_mode}-eo{exactly_once}.json"


def assemble_artifact(run_fields: dict, oracle: dict, rounds: list) -> dict:
    """Build the IC-9 artifact dict from the three sources. Pure; does not
    mutate its inputs. Hard-fails on any missing mandatory field or bad enum."""
    _require_present(run_fields, RUN_FIELDS, "run-level fields")
    _require_present(oracle, ORACLE_FIELDS, "oracle JSON")
    for field, vocab in _ENUM_RUN_FIELDS.items():
        _require_in_vocab(run_fields[field], vocab, field)
    _require_in_vocab(oracle["verdict"], VERDICTS, "verdict")

    # Rounds arrive as full IC-4 records (from load_rounds, already projected)
    # or raw dicts (from direct callers); project + validate defensively.
    projected = []
    for idx, r in enumerate(rounds, start=1):
        _require_present(r, ROUND_FIELDS, f"round[{idx}]")
        projected.append({k: r[k] for k in ROUND_FIELDS})

    artifact = {
        "chaos_id": run_fields["chaos_id"],
        "chaos_mode": run_fields["chaos_mode"],
        "chaos_seed": run_fields["chaos_seed"],
        "chaos_rounds": run_fields["chaos_rounds"],
        "rounds_completed": run_fields["rounds_completed"],
        "delivery_mode": run_fields["delivery_mode"],
        "exactly_once": run_fields["exactly_once"],
        "connector": run_fields["connector"],
        "environment_class": run_fields["environment_class"],
        "ignore_partitions_when_batching": run_fields["ignore_partitions_when_batching"],
        "ch_version": run_fields["ch_version"],
        "ch_image_digest": run_fields["ch_image_digest"],
        "arm_image": run_fields["arm_image"],
        "producer_image": run_fields["producer_image"],
        "rows_expected": oracle["rows_expected"],
        "rows_delivered": oracle["rows_delivered"],
        "unique_delivered": oracle["unique_delivered"],
        "unique_expected": oracle["unique_expected"],
        "duplicate_rows": oracle["duplicate_rows"],
        "loss": oracle["loss"],
        "dlq_depth": oracle["dlq_depth"],
        "integrity_ok": oracle["verdict"] == "PASS",
        "verdict": oracle["verdict"],
        "outcome": run_fields["outcome"],
        "run_conclusion": run_fields["run_conclusion"],
        "rounds": projected,
    }
    return artifact


def write_artifact_file(artifact: dict, out_dir: str) -> str:
    """Write the artifact JSON under out_dir and return its path."""
    fname = artifact_filename(
        artifact["chaos_id"], artifact["chaos_mode"], artifact["exactly_once"]
    )
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, fname)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(artifact, fh, indent=2, sort_keys=False)
        fh.write("\n")
    return path


def _run_fields_from_env() -> dict:
    """Read the §4 run-level fields from the environment (UPPER_CASED keys).
    A field absent from the environment is left absent so the assembler's
    mandatory-field check reports it."""
    out = {}
    for field in RUN_FIELDS:
        val = os.environ.get(field.upper())
        if val is not None:
            out[field] = val
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Assemble the per-run chaos result artifact (IC-9).")
    parser.add_argument("--rounds", required=True,
                        help="rounds JSONL path (IC-4)")
    parser.add_argument("--oracle", required=True,
                        help="oracle JSON path (IC-6, check_integrity --direct)")
    parser.add_argument("--out", required=True,
                        help="output directory for the result artifact")
    args = parser.parse_args(argv)

    try:
        run_fields = _run_fields_from_env()
        oracle = load_oracle(args.oracle)
        rounds = load_rounds(args.rounds)
        artifact = assemble_artifact(run_fields, oracle, rounds)
        path = write_artifact_file(artifact, args.out)
    except (ValueError, OSError) as exc:
        print(f"write_artifact FAILED: {type(exc).__name__}: {exc}",
              file=sys.stderr)
        return 2

    print(f"wrote chaos result artifact: {path}", file=sys.stderr)
    print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
