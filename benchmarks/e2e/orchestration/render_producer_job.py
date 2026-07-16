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
"""Render the producer Job (task 31): patch the committed producer/job.yaml with
the built image ref, an IRSA serviceAccountName for S3 read (overseer directive
e), the resolved PARQUET_SOURCE, and the SHARD_COUNT (parallel preload) —
WITHOUT editing the committed job.yaml.

We do NOT modify benchmarks/e2e/producer/job.yaml (another component's file); we
read it, patch a copy, and write the result for `kubectl apply`.

Parallel sharded preload: the producer is an INDEXED Job of SHARD_COUNT pods,
each producing a disjoint stride-N slice of the parquet files. This renderer
patches spec.completions AND spec.parallelism to SHARD_COUNT and sets the
container SHARD_COUNT env to the SAME value — all three must agree or the shard
partition is wrong (JOB_COMPLETION_INDEX runs 0..completions-1, and producer.py
strides by SHARD_COUNT). completionMode=Indexed is asserted (it is authored in
job.yaml; we never silently downgrade to a non-indexed Job, which would give
every pod the whole dataset).
"""
import argparse
import sys

import yaml


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", required=True, help="path to producer/job.yaml")
    ap.add_argument("--image", required=True, help="built producer image ref")
    ap.add_argument("--service-account", required=True,
                    help="IRSA serviceAccountName granting S3 read")
    ap.add_argument("--parquet-source", required=True, help="resolved PARQUET_SOURCE")
    ap.add_argument("--shard-count", type=int, default=3,
                    help="parallel producer shards; patches completions == "
                         "parallelism == SHARD_COUNT env (default 3)")
    ap.add_argument("--out", required=True, help="output path ('-' for stdout)")
    args = ap.parse_args()

    if args.shard_count < 1:
        print(f"ERROR: --shard-count must be >= 1 (got {args.shard_count})",
              file=sys.stderr)
        return 1

    with open(args.job) as f:
        doc = yaml.safe_load(f)

    # Indexed Job invariant: completions == parallelism == SHARD_COUNT. Never
    # downgrade completionMode (a NonIndexed Job would run N identical pods,
    # each producing the WHOLE dataset — N-fold duplication of end offsets).
    job_spec = doc["spec"]
    if job_spec.get("completionMode") != "Indexed":
        print("ERROR: producer job.yaml must declare completionMode: Indexed "
              f"(found {job_spec.get('completionMode')!r})", file=sys.stderr)
        return 1
    job_spec["completions"] = args.shard_count
    job_spec["parallelism"] = args.shard_count

    spec = doc["spec"]["template"]["spec"]
    # IRSA service account (S3 read). The IAM role itself is a provisioning TODO
    # (documented in README); we only wire the SA name here.
    spec["serviceAccountName"] = args.service_account

    containers = spec["containers"]
    if len(containers) != 1:
        print("ERROR: expected exactly one container in the producer Job",
              file=sys.stderr)
        return 1
    c = containers[0]
    c["image"] = args.image

    # Override PARQUET_SOURCE + SHARD_COUNT in the container env (leave the rest
    # as authored). SHARD_COUNT must match the patched completions/parallelism;
    # JOB_COMPLETION_INDEX is injected by K8s per pod and MUST NOT be set here.
    env = c.setdefault("env", [])
    overrides = {
        "PARQUET_SOURCE": args.parquet_source,
        "SHARD_COUNT": str(args.shard_count),
    }
    seen = set()
    for e in env:
        name = e.get("name")
        if name in overrides:
            e["value"] = overrides[name]
            e.pop("valueFrom", None)
            seen.add(name)
    for name, value in overrides.items():
        if name not in seen:
            env.append({"name": name, "value": value})

    out = yaml.safe_dump(doc, sort_keys=False)
    if args.out == "-":
        sys.stdout.write(out)
    else:
        with open(args.out, "w") as f:
            f.write(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
