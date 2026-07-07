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
"""Insert the perf.runs row for one (arm, tier) Kafka Connect sink drain.

Ported from spark-clickhouse-connector benchmarks/scripts/insert_run_record.py.
This runs ONLY AFTER metrics capture has fully succeeded (the gating discipline
in PORTING.md): a perf.runs row is never written for a run whose metrics are
incomplete, and the DWH export is gated on this insert succeeding.

Kafka adaptations (contract §1, plan §3/§9):
  * CONNECTOR defaults to 'kafka-connect' (written to perf.runs.connector).
  * The runtime map MUST carry the mandatory scope keys target_region and
    environment_class. They are sourced from the repo config (config.env,
    surfaced as TARGET_REGION / ENVIRONMENT_CLASS in the env) — NEVER hardcoded
    inline here — and this script HARD-FAILS if either is missing/empty.
  * run_id / pair_id come from lib_runid.sh. Per contract §1.2 a '…-nogit'
    identifier is USELESS (does not pin the commit under test); this script
    HARD-FAILS (no emit) if RUN_ID or PAIR_ID ends in '-nogit' or is 'nogit'.
  * The orchestrator passes arm / tier / pair_id and the §1.4 config-echo keys
    through the RUNTIME JSON passthrough (accepted verbatim into the map).

Required env: METRICS_CH_HOST, METRICS_CH_USER, METRICS_CH_PASSWORD,
              RUN_ID, RUN_START, RUN_END, GIT_SHA, CONNECTOR_VERSION,
              TARGET_REGION, ENVIRONMENT_CLASS
Optional env: CONNECTOR (default 'kafka-connect'), RUN_PROFILE (default ''),
              PAIR_ID (checked for the nogit guard if set),
              RUNTIME (JSON object of connector/runtime attributes written
              verbatim into perf.runs.runtime Map(String,String); the
              orchestrator populates it with arm, tier, pair_id, the §1.4 shared
              config keys — batch_size, write_parallelism, async_insert,
              partition_scheme, dataset — and Kafka-namespaced keys such as
              kafka_connect_version / consumer_max_poll_records).
"""
import json
import os
import sys

import ch_common


def fail_on_nogit(name: str, value: str) -> None:
    """Contract §1.2: MUST fail rather than emit a nogit run_id/pair_id."""
    if value == "nogit" or value.endswith("-nogit"):
        print(
            f"ERROR: {name}={value!r} does not pin the commit under test "
            f"(git rev-parse failed at run-id generation). Per contract §1.2 a "
            f"'nogit' identifier is useless as a pair key — refusing to emit a "
            f"perf.runs row. Fix the git checkout and re-run.",
            file=sys.stderr,
        )
        sys.exit(1)


def require_nonempty(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(
            f"ERROR: {name} is mandatory (contract §1.1 scope key) and must be "
            f"set from repo config (config.env), never hardcoded per run.",
            file=sys.stderr,
        )
        sys.exit(1)
    return value


def main() -> None:
    run_id = ch_common.require("RUN_ID")
    fail_on_nogit("RUN_ID", run_id)
    pair_id = os.environ.get("PAIR_ID", "").strip()
    if pair_id:
        fail_on_nogit("PAIR_ID", pair_id)

    # Mandatory scope keys (contract §1.1). Sourced from config, hard-checked.
    target_region = require_nonempty("TARGET_REGION")
    environment_class = require_nonempty("ENVIRONMENT_CLASS")

    # Connector-specific runtime attributes go into a generic Map column, so
    # adding a connector never needs a schema change.
    runtime = json.loads(os.environ.get("RUNTIME", "{}"))
    # The scope keys are contract-mandatory: inject them from config so they are
    # present even if the orchestrator's RUNTIME JSON omitted them, and so they
    # can never be silently hardcoded to a wrong value in the passthrough.
    runtime.setdefault("target_region", target_region)
    runtime.setdefault("environment_class", environment_class)

    target = ch_common.get_client("TARGET_CH_HOST", "TARGET_CH_USER", "TARGET_CH_PASSWORD")
    clickhouse_version = target.query("SELECT version()").result_rows[0][0]

    metrics = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    metrics.command(
        """
        INSERT INTO perf.runs
          (run_id, run_started_at, run_ended_at, git_sha,
           connector, run_profile, connector_version,
           clickhouse_version, runtime)
        VALUES
          ({run_id:String},
           parseDateTimeBestEffort({run_start:String}),
           parseDateTimeBestEffort({run_end:String}),
           {git_sha:String},
           {connector:String}, {run_profile:String}, {connector_version:String},
           {clickhouse_version:String},
           mapFromArrays({runtime_keys:Array(String)}, {runtime_values:Array(String)}))
        """,
        parameters={
            "run_id": run_id,
            "run_start": ch_common.require("RUN_START"),
            "run_end": ch_common.require("RUN_END"),
            "git_sha": ch_common.require("GIT_SHA"),
            "connector": os.environ.get("CONNECTOR", "kafka-connect"),
            "run_profile": os.environ.get("RUN_PROFILE", ""),
            "connector_version": ch_common.require("CONNECTOR_VERSION"),
            "clickhouse_version": clickhouse_version,
            "runtime_keys": list(runtime.keys()),
            "runtime_values": [str(v) for v in runtime.values()],
        },
    )
    print(f"inserted run record {run_id} (CH {clickhouse_version}, "
          f"connector={os.environ.get('CONNECTOR', 'kafka-connect')})")


if __name__ == "__main__":
    main()
