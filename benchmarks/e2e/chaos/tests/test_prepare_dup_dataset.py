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
"""Tests for chaos/prepare_dup_dataset.py (T13, spec §5 + decision §10.5).

The dup-bearing dataset is the fixture that lets the oracle both PASS and FAIL:
a carved prefix of full ClickBench ``hits`` already contains duplicate
``WatchID``s, so ``unique_watchids (U) < rows (N)``. The connector must deliver
N rows with exactly U distinct WatchIDs — the ``uniqueness`` leg of the §5
formula (``unique_delivered == SOURCE_UNIQUE_EXPECTED``). These tests pin the
tool that computes N/U EXACTLY and stages the prefix in a stable, replayable
file+row order (the same ordering producer.py uses to stream it).

Run under the producer venv (has pyarrow):
    producer/venv/bin/python -m pytest \
        benchmarks/e2e/chaos/tests/test_prepare_dup_dataset.py -q
"""

import json
import os
import subprocess
import sys

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

CHAOS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, CHAOS_DIR)
import prepare_dup_dataset as pdd  # noqa: E402  (module under test)


# --- fixture data ------------------------------------------------------------
# Two source files with KNOWN duplicate WatchIDs. list_dataset_files sorts
# lexicographically, so the global carve order is a.parquet then b.parquet.
FILE_A_WATCHIDS = [10, 10, 20, 30]          # rows=4, distinct={10,20,30}
FILE_B_WATCHIDS = [30, 40, 40, 50, 50]      # rows=5, distinct={30,40,50}
# Global (a then b): 9 rows, distinct union {10,20,30,40,50} => U=5 < N=9.
GLOBAL_WATCHIDS = FILE_A_WATCHIDS + FILE_B_WATCHIDS


def _write_parquet(path, watchids, watchid_col="WatchID"):
    """Minimal hits-shaped parquet: a WatchID (Int64) + a filler String col."""
    n = len(watchids)
    table = pa.table(
        {
            watchid_col: pa.array(watchids, type=pa.int64()),
            "Title": pa.array([f"t{i}" for i in range(n)], type=pa.string()),
        }
    )
    pq.write_table(table, path)


def _build_source(dirpath):
    """Two-file dup-bearing source; returns the directory URI."""
    _write_parquet(os.path.join(dirpath, "a.parquet"), FILE_A_WATCHIDS)
    _write_parquet(os.path.join(dirpath, "b.parquet"), FILE_B_WATCHIDS)
    return dirpath


def _staged_watchids(out_dir):
    """Read back the staged prefix in its stable (sorted-name) order."""
    dataset = ds.dataset(out_dir, format="parquet")
    ordered = sorted(dataset.files)
    seq = []
    for f in ordered:
        seq.extend(
            ds.dataset(f, format="parquet")
            .to_table()
            .column("WatchID")
            .to_pylist()
        )
    return seq


# --- exact N/U ---------------------------------------------------------------

def test_exact_counts_full_carve(tmp_path):
    """Carving every row yields exact rows=N and unique_watchids=U, with U<N."""
    src = str(tmp_path / "src")
    os.makedirs(src, exist_ok=True)
    _build_source(src)
    out = str(tmp_path / "staged")

    manifest = pdd.carve(src, rows=len(GLOBAL_WATCHIDS), out_dir=out)

    assert manifest["rows"] == 9
    assert manifest["unique_watchids"] == 5
    # The whole point of the dup dataset: U < N (spec §5 uniqueness leg).
    assert manifest["unique_watchids"] < manifest["rows"]
    assert manifest["source"] == src
    assert manifest["files"] == ["part-00000.parquet", "part-00001.parquet"]


def test_carve_across_file_boundary(tmp_path):
    """K spanning into the second file: exact counts + first-K row order."""
    src = str(tmp_path / "src")
    os.makedirs(src, exist_ok=True)
    _build_source(src)
    out = str(tmp_path / "staged")

    manifest = pdd.carve(src, rows=6, out_dir=out)

    assert manifest["rows"] == 6
    # First 6 in global order: [10,10,20,30, 30,40] => distinct {10,20,30,40}=4.
    assert manifest["unique_watchids"] == 4
    assert manifest["unique_watchids"] < manifest["rows"]
    # Two output files: all of a (4) + first 2 of b.
    assert manifest["files"] == ["part-00000.parquet", "part-00001.parquet"]
    assert _staged_watchids(out) == GLOBAL_WATCHIDS[:6]


def test_carve_within_first_file(tmp_path):
    """K smaller than the first file: single staged file, exact counts."""
    src = str(tmp_path / "src")
    os.makedirs(src, exist_ok=True)
    _build_source(src)
    out = str(tmp_path / "staged")

    manifest = pdd.carve(src, rows=3, out_dir=out)

    assert manifest["rows"] == 3
    assert manifest["unique_watchids"] == 2  # [10,10,20]
    assert manifest["files"] == ["part-00000.parquet"]
    assert _staged_watchids(out) == GLOBAL_WATCHIDS[:3]


# --- fail loud ---------------------------------------------------------------

def test_k_exceeds_available_fails_loud(tmp_path):
    """K > available rows must fail loudly, not silently under-carve."""
    src = str(tmp_path / "src")
    os.makedirs(src, exist_ok=True)
    _build_source(src)
    out = str(tmp_path / "staged")

    with pytest.raises(ValueError, match="only has 9"):
        pdd.carve(src, rows=100, out_dir=out)


def test_missing_watchid_column_fails_loud(tmp_path):
    """A source without the uniqueness column cannot be measured — fail loud."""
    src = str(tmp_path / "src")
    os.makedirs(src, exist_ok=True)
    table = pa.table({"NotWatchID": pa.array([1, 2, 3], type=pa.int64())})
    pq.write_table(table, os.path.join(src, "a.parquet"))
    out = str(tmp_path / "staged")

    with pytest.raises(ValueError, match="WatchID"):
        pdd.carve(src, rows=2, out_dir=out)


def test_non_positive_rows_fails_loud(tmp_path):
    src = str(tmp_path / "src")
    os.makedirs(src, exist_ok=True)
    _build_source(src)
    out = str(tmp_path / "staged")

    with pytest.raises(ValueError):
        pdd.carve(src, rows=0, out_dir=out)


# --- ordering stability ------------------------------------------------------

def test_ordering_stability_two_runs_identical_manifest(tmp_path):
    """Two runs over the same source produce byte-identical manifest content."""
    src = str(tmp_path / "src")
    os.makedirs(src, exist_ok=True)
    _build_source(src)
    out1 = str(tmp_path / "staged1")
    out2 = str(tmp_path / "staged2")

    m1 = pdd.carve(src, rows=7, out_dir=out1)
    m2 = pdd.carve(src, rows=7, out_dir=out2)

    # source differs only if out dirs are baked in — they are not.
    assert m1 == m2
    # And the staged row sequence is identical + equals the first-K global order.
    assert _staged_watchids(out1) == _staged_watchids(out2)
    assert _staged_watchids(out1) == GLOBAL_WATCHIDS[:7]


# --- CLI ---------------------------------------------------------------------

def test_cli_emits_manifest_and_writes_file(tmp_path):
    """main(): manifest JSON on stdout + a manifest file written; U<N."""
    src = str(tmp_path / "src")
    os.makedirs(src, exist_ok=True)
    _build_source(src)
    out = str(tmp_path / "staged")
    manifest_path = str(tmp_path / "dup.manifest.json")

    result = subprocess.run(
        [
            sys.executable,
            os.path.join(CHAOS_DIR, "prepare_dup_dataset.py"),
            "--source", src,
            "--rows", "9",
            "--out", out,
            "--manifest", manifest_path,
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

    # stdout's last line is the machine-readable manifest.
    stdout_manifest = json.loads(result.stdout.strip().splitlines()[-1])
    assert stdout_manifest["rows"] == 9
    assert stdout_manifest["unique_watchids"] == 5
    assert stdout_manifest["unique_watchids"] < stdout_manifest["rows"]

    # The manifest file matches stdout and lives OUTSIDE the parquet prefix.
    assert os.path.exists(manifest_path)
    with open(manifest_path) as f:
        file_manifest = json.load(f)
    assert file_manifest == stdout_manifest
    assert not os.path.exists(os.path.join(out, "dup.manifest.json"))


def test_cli_k_too_large_exits_nonzero(tmp_path):
    src = str(tmp_path / "src")
    os.makedirs(src, exist_ok=True)
    _build_source(src)
    result = subprocess.run(
        [
            sys.executable,
            os.path.join(CHAOS_DIR, "prepare_dup_dataset.py"),
            "--source", src,
            "--rows", "1000",
            "--out", str(tmp_path / "staged"),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "only has" in result.stderr
