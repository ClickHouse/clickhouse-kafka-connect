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
"""Carve a dup-bearing prefix from a parquet source (chaos T13, §5 / §10.5).

The chaos oracle (§5) both PASSes and FAILs; the dup-bearing dataset is what
proves it *can* pass on non-trivial data. Full ClickBench ``hits`` already
contains duplicate ``WatchID``s (decision §10.5 — no synthesis), so a carved
prefix of the first K rows has ``unique_watchids (U) < rows (N)``. The connector
must deliver N rows with EXACTLY U distinct WatchIDs; ``SOURCE_UNIQUE_EXPECTED``
= U is the constant chaos_run.sh feeds the oracle for its ``uniqueness`` leg
(``unique_delivered == SOURCE_UNIQUE_EXPECTED``).

This tool:
  * lists the source's parquet files in producer.py's EXACT deterministic order
    (``list_dataset_files`` — imported, not copied — so the staged prefix is
    streamed by the producer in the identical file+row sequence),
  * carves the first K rows (whole files until the last, sliced) into a staged
    prefix of ``part-NNNNN.parquet`` files (names sort into carve order, so a
    consumer rebuilding the dataset from the prefix replays the same sequence),
  * computes ``rows=N`` and ``unique_watchids=U`` EXACTLY (a running set over
    the carved WatchIDs — the pure-Python mirror of ``uniqExact(WatchID)``),
  * writes a manifest JSON ``{source, rows, unique_watchids, files[]}``.

K larger than the source's row count is a LOUD error — never a silent
under-carve (that would hand the oracle a wrong ``SOURCE_UNIQUE_EXPECTED``).

Works on any pyarrow-dataset URI (local path or ``s3://``). Phase-1 runs it only
on local fixtures; S3 staging is the later live phase.

Conventions (mirrors capture/*.py, producer.py): env/args in this docstring,
stdout = the machine-readable manifest (last line), stderr = human logging.

Run (has pyarrow via the producer venv):
    producer/venv/bin/python chaos/prepare_dup_dataset.py \
        --source <parquet-uri> --rows K --out <staged-dir> [--manifest PATH]
"""

import argparse
import json
import os
import sys

import pyarrow.dataset as ds
import pyarrow.parquet as pq

# Reuse producer.py's file-ordering verbatim (do NOT copy it): the staged prefix
# must be produced in the identical order the streaming producer will read it.
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "producer"))
from producer import list_dataset_files  # noqa: E402

WATCHID_COL = "WatchID"


def log(msg):
    print(f"[prepare_dup_dataset] {msg}", file=sys.stderr, flush=True)


def carve(source, rows, out_dir, watchid_col=WATCHID_COL):
    """Carve the first ``rows`` rows of ``source`` into ``out_dir``.

    Returns the manifest dict ``{source, rows, unique_watchids, files[]}``.
    ``files`` are basenames within ``out_dir`` (the staged prefix), so the
    manifest is location-independent. Raises ``ValueError`` on any loud failure
    (non-positive K, missing WatchID column, K > available rows).
    """
    if rows <= 0:
        raise ValueError(f"--rows must be positive, got {rows}")

    dataset = ds.dataset(source, format="parquet")
    if watchid_col not in dataset.schema.names:
        raise ValueError(
            f"source {source!r} has no {watchid_col!r} column — cannot compute "
            f"unique_watchids; columns={list(dataset.schema.names)[:12]}"
        )

    files = list_dataset_files(dataset)
    if not files:
        raise ValueError(f"source {source!r} contains no parquet files")

    os.makedirs(out_dir, exist_ok=True)

    remaining = rows
    total = 0
    unique = set()  # exact distinct WatchIDs — the uniqExact(WatchID) mirror
    out_files = []

    for src in files:
        if remaining <= 0:
            break
        # filesystem= is REQUIRED for S3 sources: .files yields bucket-relative
        # paths, so a rebuilt dataset must carry the parent's filesystem or
        # pyarrow resolves them against local disk (producer.py, pair-5 note).
        fds = ds.dataset(src, format="parquet", filesystem=dataset.filesystem)
        out_name = f"part-{len(out_files):05d}.parquet"
        out_path = os.path.join(out_dir, out_name)
        writer = None
        try:
            for batch in fds.to_batches(
                batch_readahead=1, fragment_readahead=1
            ):
                if remaining <= 0:
                    break
                if batch.num_rows > remaining:
                    batch = batch.slice(0, remaining)
                if batch.num_rows == 0:
                    continue
                if writer is None:
                    writer = pq.ParquetWriter(out_path, batch.schema)
                writer.write_batch(batch)
                for w in batch.column(watchid_col).to_pylist():
                    unique.add(w)
                total += batch.num_rows
                remaining -= batch.num_rows
        finally:
            if writer is not None:
                writer.close()
        if writer is not None:
            out_files.append(out_name)
            log(f"staged {out_name} <- {src}")

    if total < rows:
        raise ValueError(
            f"requested {rows} rows but source {source!r} only has {total} "
            f"across {len(files)} file(s) — refusing to carve a short prefix "
            f"(would yield a wrong SOURCE_UNIQUE_EXPECTED)"
        )

    manifest = {
        "source": str(source),
        "rows": total,
        "unique_watchids": len(unique),
        "files": out_files,
    }
    log(
        f"carved rows={manifest['rows']} unique_watchids="
        f"{manifest['unique_watchids']} into {len(out_files)} file(s) at "
        f"{out_dir}"
    )
    return manifest


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Carve a dup-bearing prefix from a parquet source (chaos T13)"
    )
    p.add_argument("--source", required=True,
                   help="parquet-dataset URI (local path or s3://)")
    p.add_argument("--rows", type=int, required=True,
                   help="number of rows (K) to carve, in stable file+row order")
    p.add_argument("--out", required=True,
                   help="staged prefix directory for the carved parquet files")
    p.add_argument("--manifest", default=None,
                   help="manifest JSON path (default: <out>.manifest.json, a "
                        "SIBLING of the prefix so it is not read as parquet)")
    p.add_argument("--watchid-col", default=WATCHID_COL,
                   help=f"uniqueness column (default {WATCHID_COL})")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    manifest_path = args.manifest or (args.out.rstrip("/\\") + ".manifest.json")
    try:
        manifest = carve(args.source, args.rows, args.out,
                         watchid_col=args.watchid_col)
    except ValueError as e:
        print(f"[prepare_dup_dataset:error] {e}", file=sys.stderr, flush=True)
        sys.exit(1)

    with open(manifest_path, "w") as f:
        json.dump(manifest, f)
    log(f"wrote manifest {manifest_path}")

    # The manifest is ALWAYS the last stdout line, machine-readable JSON.
    print(json.dumps(manifest), flush=True)


if __name__ == "__main__":
    main()
