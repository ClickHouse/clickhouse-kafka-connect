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
"""Unit tests for chaos/write_artifact.py (T6 / IC-9 / spec §11).

The writer assembles the per-run chaos result JSON from three sources — run-level
fields (env/args), the rounds JSONL (IC-4), and the oracle JSON (IC-6) — and MUST
hard-fail on any missing mandatory field, malformed round line, or an
outcome/run_conclusion/verdict value outside the pinned vocabulary. It never
writes perf.* and never exports (decision §10.2).

These tests are self-contained (no shared conftest, no __init__.py) and touch no
network / AWS / docker (Phase-1). They drive the pure assembler functions plus a
disk round-trip through main().

Run: python3 -m pytest benchmarks/e2e/chaos/tests/test_write_artifact.py -q
"""
import copy
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import write_artifact as wa  # noqa: E402


# --------------------------------------------------------------------------- #
# Fixtures / valid inputs
# --------------------------------------------------------------------------- #
def _run_fields():
    return {
        "chaos_id": "2026-07-16-01",
        "chaos_mode": "monkey",
        "chaos_seed": "42",
        "chaos_rounds": "20",
        "rounds_completed": "3",
        "delivery_mode": "exactly_once",
        "exactly_once": "1",
        "connector": "kafka-connect-chaos",
        "environment_class": "self_hosted",
        "ignore_partitions_when_batching": "0",
        "ch_version": "latest",
        "ch_image_digest": "clickhouse/clickhouse-server@sha256:deadbeef",
        "arm_image": "1234.dkr.ecr/arm@sha256:aaaa",
        "producer_image": "1234.dkr.ecr/producer@sha256:bbbb",
        "outcome": "passed",
        "run_conclusion": "quiesced",
    }


def _oracle():
    return {
        "rows_delivered": 2000000,
        "rows_expected": 2000000,
        "unique_delivered": 1500000,
        "unique_expected": 1500000,
        "duplicate_rows": 0,
        "loss": 0,
        "dlq_depth": 0,
        "verdict": "PASS",
        "reason": "exact match; fault observed",
    }


def _round(n, fault="C1", window="W1"):
    return {
        "round": n,
        "fault_type": fault,
        "fault_window": window,
        "inject_ts": f"2026-07-16T10:0{n}:00Z",
        "recovery_seconds": 12.5 + n,
        "task_restart_count": 1,
        "insert_errors_during_fault": 0,
        "ch_dedup_dropped_blocks": 0,
        "fault_observed": True,
        # IC-4 carries an extra diagnostic 'evidence' object the artifact drops.
        "evidence": {"kind": "pod_recreated", "detail": f"uid changed r{n}"},
    }


def _projected_round(n, fault="C1", window="W1"):
    r = _round(n, fault, window)
    return {k: r[k] for k in wa.ROUND_FIELDS}


# --------------------------------------------------------------------------- #
# Golden full artifact
# --------------------------------------------------------------------------- #
def test_golden_full_artifact():
    art = wa.assemble_artifact(_run_fields(), _oracle(),
                               [_round(1), _round(2, "C4", "W2")])
    expected = {
        "chaos_id": "2026-07-16-01",
        "chaos_mode": "monkey",
        "chaos_seed": "42",
        "chaos_rounds": "20",
        "rounds_completed": "3",
        "delivery_mode": "exactly_once",
        "exactly_once": "1",
        "connector": "kafka-connect-chaos",
        "environment_class": "self_hosted",
        "ignore_partitions_when_batching": "0",
        "ch_version": "latest",
        "ch_image_digest": "clickhouse/clickhouse-server@sha256:deadbeef",
        "arm_image": "1234.dkr.ecr/arm@sha256:aaaa",
        "producer_image": "1234.dkr.ecr/producer@sha256:bbbb",
        "rows_expected": 2000000,
        "rows_delivered": 2000000,
        "unique_delivered": 1500000,
        "unique_expected": 1500000,
        "duplicate_rows": 0,
        "loss": 0,
        "dlq_depth": 0,
        "integrity_ok": True,
        "verdict": "PASS",
        "outcome": "passed",
        "run_conclusion": "quiesced",
        "rounds": [_projected_round(1), _projected_round(2, "C4", "W2")],
    }
    assert art == expected


def test_all_section4_runtime_keys_present():
    art = wa.assemble_artifact(_run_fields(), _oracle(), [_round(1)])
    for key in ("connector", "chaos_id", "chaos_mode", "chaos_seed",
                "chaos_rounds", "rounds_completed", "delivery_mode",
                "exactly_once", "environment_class",
                "ignore_partitions_when_batching", "run_conclusion",
                "dlq_depth"):
        assert key in art, f"§4 runtime key {key} missing from artifact"


# --------------------------------------------------------------------------- #
# Mandatory-field omission — every one fails loudly
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("field", sorted(wa.RUN_FIELDS))
def test_missing_run_field_fails_loudly(field):
    rf = _run_fields()
    del rf[field]
    with pytest.raises(ValueError) as ei:
        wa.assemble_artifact(rf, _oracle(), [_round(1)])
    assert field in str(ei.value)


@pytest.mark.parametrize("field", sorted(wa.RUN_FIELDS))
def test_none_run_field_fails_loudly(field):
    rf = _run_fields()
    rf[field] = None
    with pytest.raises(ValueError) as ei:
        wa.assemble_artifact(rf, _oracle(), [_round(1)])
    assert field in str(ei.value)


@pytest.mark.parametrize("field", sorted(wa.ORACLE_FIELDS))
def test_missing_oracle_field_fails_loudly(field):
    orc = _oracle()
    del orc[field]
    with pytest.raises(ValueError) as ei:
        wa.assemble_artifact(_run_fields(), orc, [_round(1)])
    assert field in str(ei.value)


@pytest.mark.parametrize("field", sorted(wa.ROUND_FIELDS))
def test_missing_round_field_fails_loudly(field):
    bad = _round(1)
    del bad[field]
    with pytest.raises(ValueError) as ei:
        wa.assemble_artifact(_run_fields(), _oracle(), [bad])
    assert field in str(ei.value)


# --------------------------------------------------------------------------- #
# Malformed round lines (JSONL parsing)
# --------------------------------------------------------------------------- #
def test_parse_round_line_rejects_malformed_json():
    with pytest.raises(ValueError) as ei:
        wa.parse_round_line("{not valid json", lineno=7)
    assert "malformed" in str(ei.value).lower()
    assert "7" in str(ei.value)


def test_parse_round_line_projects_to_nine_fields():
    proj = wa.parse_round_line(json.dumps(_round(1)))
    assert set(proj.keys()) == set(wa.ROUND_FIELDS)
    assert "evidence" not in proj


def test_load_rounds_rejects_malformed_line(tmp_path):
    p = tmp_path / "rounds.jsonl"
    p.write_text(json.dumps(_round(1)) + "\n" + "GARBAGE\n")
    with pytest.raises(ValueError):
        wa.load_rounds(str(p))


def test_load_rounds_skips_blank_lines(tmp_path):
    p = tmp_path / "rounds.jsonl"
    p.write_text(json.dumps(_round(1)) + "\n\n   \n" + json.dumps(_round(2)) + "\n")
    rounds = wa.load_rounds(str(p))
    assert [r["round"] for r in rounds] == [1, 2]


# --------------------------------------------------------------------------- #
# Rounds ordering preserved (never sorted)
# --------------------------------------------------------------------------- #
def test_rounds_ordering_preserved(tmp_path):
    p = tmp_path / "rounds.jsonl"
    # Deliberately out of numeric order.
    p.write_text("\n".join(json.dumps(_round(n)) for n in (3, 1, 2)) + "\n")
    rounds = wa.load_rounds(str(p))
    assert [r["round"] for r in rounds] == [3, 1, 2]
    art = wa.assemble_artifact(_run_fields(), _oracle(), rounds)
    assert [r["round"] for r in art["rounds"]] == [3, 1, 2]


def test_rounds_may_be_empty():
    art = wa.assemble_artifact(_run_fields(), _oracle(), [])
    assert art["rounds"] == []


# --------------------------------------------------------------------------- #
# Vocabulary enforcement: verdict / outcome / run_conclusion (+ enum fields)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("verdict",
                         sorted(wa.VERDICTS))
def test_valid_verdicts_accepted(verdict):
    orc = _oracle()
    orc["verdict"] = verdict
    art = wa.assemble_artifact(_run_fields(), orc, [_round(1)])
    assert art["verdict"] == verdict


def test_bad_verdict_rejected():
    orc = _oracle()
    orc["verdict"] = "TOTALLY_FINE"
    with pytest.raises(ValueError) as ei:
        wa.assemble_artifact(_run_fields(), orc, [_round(1)])
    assert "verdict" in str(ei.value)


@pytest.mark.parametrize("outcome", sorted(wa.OUTCOMES))
def test_valid_outcomes_accepted(outcome):
    rf = _run_fields()
    rf["outcome"] = outcome
    art = wa.assemble_artifact(rf, _oracle(), [_round(1)])
    assert art["outcome"] == outcome


def test_bad_outcome_rejected():
    rf = _run_fields()
    rf["outcome"] = "kinda_passed"
    with pytest.raises(ValueError) as ei:
        wa.assemble_artifact(rf, _oracle(), [_round(1)])
    assert "outcome" in str(ei.value)


@pytest.mark.parametrize("rc", sorted(wa.RUN_CONCLUSIONS))
def test_valid_run_conclusions_accepted(rc):
    rf = _run_fields()
    rf["run_conclusion"] = rc
    art = wa.assemble_artifact(rf, _oracle(), [_round(1)])
    assert art["run_conclusion"] == rc


def test_bad_run_conclusion_rejected():
    rf = _run_fields()
    rf["run_conclusion"] = "gave_up"
    with pytest.raises(ValueError) as ei:
        wa.assemble_artifact(rf, _oracle(), [_round(1)])
    assert "run_conclusion" in str(ei.value)


def test_bad_chaos_mode_rejected():
    rf = _run_fields()
    rf["chaos_mode"] = "chaosmonkey"
    with pytest.raises(ValueError) as ei:
        wa.assemble_artifact(rf, _oracle(), [_round(1)])
    assert "chaos_mode" in str(ei.value)


def test_bad_delivery_mode_rejected():
    rf = _run_fields()
    rf["delivery_mode"] = "at_most_once"
    with pytest.raises(ValueError) as ei:
        wa.assemble_artifact(rf, _oracle(), [_round(1)])
    assert "delivery_mode" in str(ei.value)


@pytest.mark.parametrize("eo", ["2", "true", "", "yes"])
def test_bad_exactly_once_rejected(eo):
    rf = _run_fields()
    rf["exactly_once"] = eo
    with pytest.raises(ValueError) as ei:
        wa.assemble_artifact(rf, _oracle(), [_round(1)])
    assert "exactly_once" in str(ei.value)


def test_bad_ipwb_rejected():
    rf = _run_fields()
    rf["ignore_partitions_when_batching"] = "maybe"
    with pytest.raises(ValueError):
        wa.assemble_artifact(rf, _oracle(), [_round(1)])


# --------------------------------------------------------------------------- #
# integrity_ok is DERIVED from verdict, not accepted from input
# --------------------------------------------------------------------------- #
def test_integrity_ok_true_only_on_pass():
    orc = _oracle()
    orc["verdict"] = "PASS"
    assert wa.assemble_artifact(_run_fields(), orc, [])["integrity_ok"] is True


@pytest.mark.parametrize("verdict",
                         ["MISMATCH", "UNVERIFIED_FAULT_NOT_OBSERVED",
                          "CHECK_ERROR"])
def test_integrity_ok_false_on_non_pass(verdict):
    orc = _oracle()
    orc["verdict"] = verdict
    assert wa.assemble_artifact(_run_fields(), orc, [])["integrity_ok"] is False


# --------------------------------------------------------------------------- #
# Filename format: chaos-result-<chaos_id>-<mode>-eo<0|1>.json
# --------------------------------------------------------------------------- #
def test_filename_format():
    assert (wa.artifact_filename("2026-07-16-01", "monkey", "1")
            == "chaos-result-2026-07-16-01-monkey-eo1.json")
    assert (wa.artifact_filename("d-9", "smoke", "0")
            == "chaos-result-d-9-smoke-eo0.json")
    assert (wa.artifact_filename("q-1", "quorum_loss", "1")
            == "chaos-result-q-1-quorum_loss-eo1.json")


def test_filename_rejects_bad_exactly_once():
    with pytest.raises(ValueError):
        wa.artifact_filename("id", "monkey", "9")


def test_filename_rejects_bad_mode():
    with pytest.raises(ValueError):
        wa.artifact_filename("id", "nope", "0")


# --------------------------------------------------------------------------- #
# Disk round-trip via main() — reads env + files, writes the artifact.
# --------------------------------------------------------------------------- #
def _write_inputs(tmp_path, rounds=None, oracle=None):
    rounds = rounds if rounds is not None else [_round(1), _round(2)]
    oracle = oracle if oracle is not None else _oracle()
    rp = tmp_path / "rounds.jsonl"
    rp.write_text("\n".join(json.dumps(r) for r in rounds) + "\n")
    op = tmp_path / "oracle.json"
    op.write_text(json.dumps(oracle))
    return str(rp), str(op)


def _set_env(monkeypatch, rf):
    for k, v in rf.items():
        if v is None:
            monkeypatch.delenv(k.upper(), raising=False)
        else:
            monkeypatch.setenv(k.upper(), v)


def test_main_writes_artifact_file(tmp_path, monkeypatch, capsys):
    rp, op = _write_inputs(tmp_path)
    _set_env(monkeypatch, _run_fields())
    out_dir = tmp_path / "artifacts"
    rc = wa.main(["--rounds", rp, "--oracle", op, "--out", str(out_dir)])
    assert rc == 0
    printed = capsys.readouterr().out.strip()
    expected_path = out_dir / "chaos-result-2026-07-16-01-monkey-eo1.json"
    assert printed == str(expected_path)
    assert expected_path.exists()
    art = json.loads(expected_path.read_text())
    assert art["integrity_ok"] is True
    assert art["verdict"] == "PASS"
    assert [r["round"] for r in art["rounds"]] == [1, 2]
    assert "evidence" not in art["rounds"][0]


def test_main_hard_fails_on_missing_env(tmp_path, monkeypatch, capsys):
    rp, op = _write_inputs(tmp_path)
    rf = _run_fields()
    rf["chaos_id"] = None  # unset -> missing mandatory field
    _set_env(monkeypatch, rf)
    rc = wa.main(["--rounds", rp, "--oracle", op, "--out", str(tmp_path / "a")])
    assert rc != 0
    assert "chaos_id" in capsys.readouterr().err


def test_main_hard_fails_on_bad_verdict(tmp_path, monkeypatch, capsys):
    orc = _oracle()
    orc["verdict"] = "NONSENSE"
    rp, op = _write_inputs(tmp_path, oracle=orc)
    _set_env(monkeypatch, _run_fields())
    rc = wa.main(["--rounds", rp, "--oracle", op, "--out", str(tmp_path / "a")])
    assert rc != 0
    assert "verdict" in capsys.readouterr().err


def test_main_never_touches_perf_or_export(tmp_path, monkeypatch):
    # The writer module must not reference perf.* tables or the DWH export path
    # (decision §10.2). Source-text guard.
    src = open(os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "write_artifact.py")).read()
    # No perf.* table writes, no DWH export bridge, no SQL INSERT reachable.
    assert "perf.metrics" not in src
    assert "perf.runs" not in src
    assert "export_metrics_to_dwh" not in src
    assert "INSERT INTO" not in src.upper()


# --------------------------------------------------------------------------- #
# The assembler must not mutate its inputs.
# --------------------------------------------------------------------------- #
def test_inputs_not_mutated():
    rf, orc, rounds = _run_fields(), _oracle(), [_round(1)]
    rf_c, orc_c, rounds_c = copy.deepcopy(rf), copy.deepcopy(orc), copy.deepcopy(rounds)
    wa.assemble_artifact(rf, orc, rounds)
    assert rf == rf_c and orc == orc_c and rounds == rounds_c


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
