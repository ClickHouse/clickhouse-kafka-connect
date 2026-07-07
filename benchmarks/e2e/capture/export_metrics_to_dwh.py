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
"""Export the perf.* metric tables to the analytics DWH S3 bucket as Parquet.

Ported from spark-clickhouse-connector benchmarks/scripts/export_metrics_to_dwh.py.
Assumes the cross-account write role and has ClickHouse stream each perf.* table
straight to the bucket via the s3() table function (one prefix per table). The
DWH side (ClickPipe / s3queue) ingests the files into the warehouse.

GATING: the orchestrator runs this ONLY AFTER insert_run_record.py succeeds (which
itself runs only after full metrics capture). A run whose metrics were rolled
back, or whose runs row was never written, is never exported — the export is the
last, gated step of the discipline described in PORTING.md.

Per-run export (RUN_ID set) writes one file per table named by run_id, so the
DWH picks up each run exactly once and never re-pulls history. With RUN_ID unset
it does a full-table backfill (timestamped file).

Required env:
  METRICS_CH_HOST, METRICS_CH_USER, METRICS_CH_PASSWORD  (ClickHouse with perf.*)
  DWH_ROLE_ARN     cross-account role to assume (e.g. the connectors metrics role)
  DWH_BUCKET       target S3 bucket (NOT hardcoded — see README. The shared Spark
                   bucket 'connectors-load-testing-metrics' vs an own bucket, and
                   whether a us-east-2 scratch bucket is used, is a pending region
                   decision; the bucket is a required parameter so no wrong bucket
                   is baked into the code.)
Optional env:
  DWH_BUCKET_REGION  auto-resolved from the bucket if unset
  RUN_ID             export only this run's rows (incremental); else full backfill

Ambient AWS credentials (e.g. the GitHub Actions OIDC role) must be allowed to
assume DWH_ROLE_ARN. Nothing here writes credentials to disk or logs.
"""
import os
import urllib.request
import urllib.error
import time

import boto3

import ch_common

TABLES = ["runs", "metrics", "ch_inserts"]  # perf.<table>


def resolve_region(bucket: str) -> str:
    region = os.environ.get("DWH_BUCKET_REGION")
    if region:
        return region
    # S3 returns the bucket's region in this header even on an unauthenticated
    # request, so we can resolve it without any extra IAM permission.
    req = urllib.request.Request(f"https://{bucket}.s3.amazonaws.com", method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.headers.get("x-amz-bucket-region") or "us-east-1"
    except urllib.error.HTTPError as e:
        return e.headers.get("x-amz-bucket-region") or "us-east-1"


def main() -> None:
    role_arn = ch_common.require("DWH_ROLE_ARN")
    # Bucket is a REQUIRED parameter (no hardcoded default) — see module docstring.
    bucket = ch_common.require("DWH_BUCKET")
    run_id = os.environ.get("RUN_ID")
    region = resolve_region(bucket)

    creds = boto3.client("sts").assume_role(
        RoleArn=role_arn, RoleSessionName="connectors-metrics-export"
    )["Credentials"]

    ch = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    tag = run_id or time.strftime("full-%Y%m%dT%H%M%SZ", time.gmtime())

    for table in TABLES:
        key = f"{table}/{tag}.parquet"
        url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
        where = "WHERE run_id = {run_id:String}" if run_id else ""
        # Credentials are bound as query parameters, never interpolated into SQL.
        sql = (
            "INSERT INTO FUNCTION s3({url:String}, {ak:String}, {sk:String}, {tok:String}, 'Parquet') "
            f"SELECT * FROM perf.{table} {where}"
        )
        params = {
            "url": url,
            "ak": creds["AccessKeyId"],
            "sk": creds["SecretAccessKey"],
            "tok": creds["SessionToken"],
        }
        if run_id:
            params["run_id"] = run_id
        # The DWH role is write-only (PutObject, no GetObject/ListBucket).
        # ClickHouse's s3() insert otherwise HEADs the key first, which 403s on a
        # write-only role. Overwrite mode does a plain PutObject with no probe.
        ch.command(sql, parameters=params,
                   settings={"s3_truncate_on_insert": 1, "s3_create_new_file_on_insert": 0})
        print(f"exported perf.{table} -> s3://{bucket}/{key}")

    print("done")


if __name__ == "__main__":
    main()
