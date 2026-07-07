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
e), and the resolved PARQUET_SOURCE — WITHOUT editing the committed job.yaml.

We do NOT modify benchmarks/e2e/producer/job.yaml (another component's file); we
read it, patch a copy, and write the result for `kubectl apply`.
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
    ap.add_argument("--out", required=True, help="output path ('-' for stdout)")
    args = ap.parse_args()

    with open(args.job) as f:
        doc = yaml.safe_load(f)

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

    # Override PARQUET_SOURCE in the container env (leave the rest as authored).
    env = c.setdefault("env", [])
    found = False
    for e in env:
        if e.get("name") == "PARQUET_SOURCE":
            e["value"] = args.parquet_source
            e.pop("valueFrom", None)
            found = True
            break
    if not found:
        env.append({"name": "PARQUET_SOURCE", "value": args.parquet_source})

    out = yaml.safe_dump(doc, sort_keys=False)
    if args.out == "-":
        sys.stdout.write(out)
    else:
        with open(args.out, "w") as f:
            f.write(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
