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
"""Compute the FULL pair cost once and record it as run_cost_usd (contract §2.1
amendment / overseer directive d): the shared infra (EKS nodes + broker EBS) is
provisioned once per pair, so the whole pair cost is charged ONCE, on the
first-run arm's row; the other arm omits the metric.

cost = node_hours x nodes x m6i.large_usd_per_hr        (bench compute)
     + node_hours x connect_nodes x connect_usd_per_hr  (dedicated connect compute)
     + broker_ebs_gb x ebs_usd_per_gb_mo x (hours/730)   (per-run 70Gi gp3 PVC)

The dedicated Connect nodegroup (2026-07-08 rebuild: the worker was CPU-bound on
the shared m6i.large) is a SECOND instance type (m6i.xlarge), so its node-hours
are billed at their own rate. --connect-nodes defaults to 0 so pre-rebuild
callers that pass only the m6i.large term still compute the same cost.

node_hours ~= now - PAIR_RUN_START (scale-up -> teardown window). No AWS Pricing
API call at run time (plan "no new AWS calls"); prices are passed in from
run_pair.sh's small in-repo table and bumped deliberately.

This inserts directly into perf.metrics (like the Spark compute_run_cost.py) so
it lands with the run's other metrics before insert_run_record and export.

Required env: METRICS_CH_HOST, METRICS_CH_USER, METRICS_CH_PASSWORD.
"""
import argparse
import sys
from datetime import datetime, timezone

# Reuse the capture pipeline's ch_common (same env-only creds contract).
sys.path.insert(0, __file__.rsplit("/", 2)[0] + "/capture")
import ch_common  # noqa: E402


def parse_ts(s: str) -> datetime:
    # RUN_START form: 2026-07-07T04:15:32 (UTC, no tz suffix).
    return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True, help="first-run arm's run_id")
    ap.add_argument("--pair-start", required=True, help="PAIR_RUN_START (UTC ISO)")
    ap.add_argument("--nodes", type=int, required=True,
                    help="m6i.large (bench-ng) node count")
    ap.add_argument("--node-usd-per-hr", type=float, required=True,
                    help="m6i.large on-demand rate")
    # Dedicated Connect nodegroup (m6i.xlarge). Defaults keep pre-rebuild
    # single-instance-type callers computing exactly the same cost.
    ap.add_argument("--connect-nodes", type=int, default=0,
                    help="m6i.xlarge (connect-ng) node count")
    ap.add_argument("--connect-usd-per-hr", type=float, default=0.0,
                    help="m6i.xlarge on-demand rate")
    ap.add_argument("--ebs-gb", type=float, required=True)
    ap.add_argument("--ebs-usd-per-gb-mo", type=float, required=True)
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    hours = max(0.0, (now - parse_ts(args.pair_start)).total_seconds() / 3600.0)

    bench_compute = hours * args.nodes * args.node_usd_per_hr
    connect_compute = hours * args.connect_nodes * args.connect_usd_per_hr
    compute = bench_compute + connect_compute
    ebs = args.ebs_gb * args.ebs_usd_per_gb_mo * (hours / 730.0)
    cost = compute + ebs

    print(f"run_cost_usd (pair) for {args.run_id}: "
          f"{args.nodes} m6i.large + {args.connect_nodes} m6i.xlarge x {hours:.3f}h "
          f"(${bench_compute:.4f} bench + ${connect_compute:.4f} connect) "
          f"= ${compute:.4f} compute + ${ebs:.4f} EBS = ${cost:.4f}",
          file=sys.stderr)

    client = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER",
                                  "METRICS_CH_PASSWORD")
    # Pre-delete any prior run_cost_usd for this run_id so a retry is clean.
    client.command(
        "ALTER TABLE perf.metrics DELETE WHERE run_id = {run_id:String} "
        "AND metric_name = 'run_cost_usd'",
        parameters={"run_id": args.run_id}, settings={"mutations_sync": 1},
    )
    client.command(
        "INSERT INTO perf.metrics (run_id, metric_name, unit, value) "
        "VALUES ({run_id:String}, 'run_cost_usd', 'usd', {cost:Float64})",
        parameters={"run_id": args.run_id, "cost": cost},
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
