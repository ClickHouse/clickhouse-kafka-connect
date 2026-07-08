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
"""Render the KafkaConnector CR from kafkaconnector.json.tmpl for one (arm,tier).

Substitutes ${CH_TABLE} (per-tier target: hits_null Tier 0 | hits Tier 1) and
sets the per-(arm,tier) consumer group. The ${env:CH_*} placeholders are left
UNTOUCHED — they are resolved by the Connect worker from the externalConfiguration
Secret at runtime, so credentials never appear in the rendered file or any log.

CFG_MAX_POLL_RECORDS (env, review F2): when set, overrides the template's
consumer.override.max.poll.records. run_pair.sh exports the SAME env var into
the runtime echo (build_runtime_json), so the deployed connector and the
recorded provenance agree BY CONSTRUCTION — previously the env var was
echo-only, so setting it wrote one value into perf.runs while the connector
silently ran the template's (false provenance). Unset => template value.

The template carries `//`-prefixed documentation keys; those are stripped from
the emitted CR (Connect would ignore them, but a clean CR is easier to review).

Output is a KafkaConnector CR (YAML/JSON both accepted by kubectl apply).
"""
import argparse
import json
import os
import sys


def strip_doc_keys(obj):
    """Recursively drop keys that start with '//' (template documentation)."""
    if isinstance(obj, dict):
        return {k: strip_doc_keys(v) for k, v in obj.items()
                if not (isinstance(k, str) and k.startswith("//"))}
    if isinstance(obj, list):
        return [strip_doc_keys(v) for v in obj]
    return obj


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", required=True)
    ap.add_argument("--ch-table", required=True, help="hits_null (T0) | hits (T1)")
    ap.add_argument("--group", required=True, help="consumer group id")
    ap.add_argument("--out", required=True, help="output path ('-' for stdout)")
    args = ap.parse_args()

    with open(args.template) as f:
        doc = json.load(f)

    doc = strip_doc_keys(doc)
    cfg = doc["spec"]["config"]

    # ${CH_TABLE} substitution in topic2TableMap (only placeholder in config).
    cfg["topic2TableMap"] = cfg["topic2TableMap"].replace("${CH_TABLE}", args.ch_table)

    # Per-(arm,tier) consumer group so each drain starts fresh at offset 0.
    # Kafka Connect sink consumer group id = "connect-<connector-name>" by
    # default; override it explicitly so a fresh group means a fresh offset-0
    # drain even if the connector name is reused across tiers.
    cfg["consumer.override.group.id"] = args.group

    # Poll-size knob (review F2): the SAME env var run_pair.sh echoes into the
    # runtime map drives the deployed value, so provenance cannot lie. Validate
    # it is a positive integer — a typo here must fail the render, not deploy.
    max_poll = os.environ.get("CFG_MAX_POLL_RECORDS")
    if max_poll:
        if not max_poll.isdigit() or int(max_poll) <= 0:
            print(f"CFG_MAX_POLL_RECORDS must be a positive integer, got {max_poll!r}",
                  file=sys.stderr)
            return 1
        cfg["consumer.override.max.poll.records"] = max_poll

    out = json.dumps(doc, indent=2, sort_keys=False)
    if args.out == "-":
        sys.stdout.write(out + "\n")
    else:
        with open(args.out, "w") as f:
            f.write(out + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
