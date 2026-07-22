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
"""Offline validation for the chaos GitHub Actions workflow (task T12 / IC-3).

No cluster, no creds, no kubeconfig, no network, and the workflow is NOT
dispatched (Phase-1). Everything here is structural: the workflow YAML parses,
`on:` is workflow_dispatch ONLY (never scheduled — cadence is on-demand, §7),
the `concurrency:` group is byte-equal to the nightly pair workflow's so a chaos
run can never overlap a pair (§4 rule 1, CI level), the artifact-upload and
teardown steps carry `if: always()` (run_pair cleanup convention), and the run
step invokes `orchestration/chaos_run.sh` (IC-3 entrypoint). `actionlint` runs
only when installed (skipped gracefully otherwise).
"""
import os
import shutil
import subprocess

import pytest

yaml = pytest.importorskip("yaml")

HERE = os.path.dirname(__file__)
REPO = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
WORKFLOWS = os.path.join(REPO, ".github", "workflows")
CHAOS_WF = os.path.join(WORKFLOWS, "benchmark-chaos.yml")
NIGHTLY_WF = os.path.join(WORKFLOWS, "benchmark-nightly.yml")


def _load(path):
    with open(path) as fh:
        return yaml.safe_load(fh)


def _text(path):
    with open(path) as fh:
        return fh.read()


def _on(doc):
    # YAML 1.1 (PyYAML) folds the bare key `on` to the boolean True; accept both.
    if "on" in doc:
        return doc["on"]
    return doc[True]


def _steps(doc):
    jobs = doc["jobs"]
    steps = []
    for job in jobs.values():
        steps.extend(job.get("steps", []) or [])
    return steps


def test_workflow_file_exists():
    assert os.path.isfile(CHAOS_WF), f"missing workflow: {CHAOS_WF}"


def test_workflow_yaml_parses():
    doc = _load(CHAOS_WF)
    assert isinstance(doc, dict)
    assert "jobs" in doc and doc["jobs"], "workflow defines no jobs"


def test_trigger_is_workflow_dispatch_only():
    on = _on(_load(CHAOS_WF))
    # `on:` may be a bare scalar, a list, or a mapping; normalize to the key set.
    if isinstance(on, str):
        keys = {on}
    elif isinstance(on, list):
        keys = set(on)
    elif isinstance(on, dict):
        keys = set(on.keys())
    else:
        pytest.fail(f"unexpected `on:` shape: {type(on)!r}")
    assert keys == {"workflow_dispatch"}, (
        f"chaos is on-demand ONLY (§7): `on:` must be exactly workflow_dispatch, got {keys}"
    )


def test_not_scheduled():
    # Belt-and-suspenders against a `schedule:`/`cron` trigger slipping back in
    # (§7). Structural: inspect the parsed `on:` block, not prose in comments.
    on = _on(_load(CHAOS_WF))
    on_repr = yaml.safe_dump(on)
    assert "schedule" not in on_repr, "chaos must not carry a schedule trigger (§7)"
    assert "cron" not in on_repr, "chaos must not carry a cron trigger (§7)"


def test_dispatch_inputs_present_with_defaults():
    on = _on(_load(CHAOS_WF))
    assert isinstance(on, dict), "`on:` must be a mapping carrying workflow_dispatch inputs"
    inputs = (on["workflow_dispatch"] or {}).get("inputs", {})
    for name in ("mode", "seed", "rounds", "exactly_once", "chaos_ch_version", "quorum_loss"):
        assert name in inputs, f"missing workflow_dispatch input: {name}"
    assert str(inputs["chaos_ch_version"].get("default")) == "latest", (
        "chaos_ch_version must default to 'latest' (IC-3 / decision §10.3)"
    )


def test_concurrency_group_equals_nightly():
    chaos = _load(CHAOS_WF)
    nightly = _load(NIGHTLY_WF)
    chaos_group = chaos["concurrency"]["group"]
    nightly_group = nightly["concurrency"]["group"]
    assert chaos_group == nightly_group, (
        "chaos MUST share the nightly pair's concurrency group so the two can "
        f"never overlap (§4 rule 1); chaos={chaos_group!r} nightly={nightly_group!r}"
    )


def test_run_step_invokes_chaos_run_sh():
    body = _text(CHAOS_WF)
    assert "orchestration/chaos_run.sh" in body, (
        "the run step must invoke orchestration/chaos_run.sh (IC-3 entrypoint)"
    )


def test_artifact_upload_step_is_always():
    steps = _steps(_load(CHAOS_WF))
    upload = [s for s in steps if "upload-artifact" in str(s.get("uses", ""))]
    assert upload, "no upload-artifact step found"
    for s in upload:
        assert str(s.get("if", "")).strip() == "always()", (
            f"upload-artifact step must carry `if: always()`; got {s.get('if')!r}"
        )


def test_teardown_steps_are_always():
    steps = _steps(_load(CHAOS_WF))
    # Teardown / scale-down cleanup steps: the ones whose commands undo cluster
    # state. Identify them by name keyword and require `if: always()` on each.
    cleanup = [
        s for s in steps
        if any(kw in str(s.get("name", "")).lower()
               for kw in ("teardown", "scale", "delete", "cleanup"))
    ]
    assert cleanup, "no teardown / scale-down cleanup step found"
    for s in cleanup:
        assert str(s.get("if", "")).strip() == "always()", (
            f"cleanup step {s.get('name')!r} must carry `if: always()`"
        )


def test_artifact_paths_cover_result_rounds_and_logs():
    steps = _steps(_load(CHAOS_WF))
    upload = [s for s in steps if "upload-artifact" in str(s.get("uses", ""))]
    paths = "\n".join(str((s.get("with") or {}).get("path", "")) for s in upload)
    assert "chaos" in paths, (
        "the upload step must collect the chaos artifacts (chaos-result-*.json + "
        "rounds JSONL + logs)"
    )


def test_teardown_step_invokes_reap():
    """T12/T15: the if: always() teardown step must invoke chaos_run.sh --reap so
    the CI-timeout/SIGKILL gap and the local-kill gap share one code path."""
    steps = _steps(_load(CHAOS_WF))
    reap = [s for s in steps if "--reap" in str(s.get("run", ""))]
    assert reap, "no teardown step invokes chaos_run.sh --reap (T12/T15)"
    for s in reap:
        assert "chaos_run.sh --reap" in str(s.get("run", ""))
        assert str(s.get("if", "")).strip() == "always()", (
            f"reap teardown step {s.get('name')!r} must carry `if: always()`"
        )


def test_actionlint_clean_if_installed():
    exe = shutil.which("actionlint")
    if not exe:
        pytest.skip("actionlint not installed")
    res = subprocess.run([exe, CHAOS_WF], capture_output=True, text=True)
    assert res.returncode == 0, f"actionlint failed:\n{res.stdout}\n{res.stderr}"
