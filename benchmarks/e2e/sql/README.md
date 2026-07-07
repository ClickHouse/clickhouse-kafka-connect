# Kafka Connect sink benchmark v2 — target bootstrap SQL

Bootstrap DDL for the Kafka benchmark's **own dedicated ClickHouse Cloud target**
(plan: `docs/benchmark-v2-plan.md`, §4 Tiers, §5 anatomy, §9 schema impact).

The target service (3 vCPU / 12 GiB, 1 replica) is the same spec as the Spark
benchmark's target so the two connectors are directly comparable (dashboard Tab 5)
while never contending. The `clickbench.hits` DDL and the `perf.*` schema are ported
from the Spark benchmark and **must stay byte-for-byte schema-identical** — that is
the cross-connector comparability contract.

## Target service

A dedicated Cloud service has been provisioned (human step, done): **3 vCPU /
12 GiB, 1 replica, region `us-east-2`** (co-located with the EKS cluster so the
drain path is intra-region), on the `clickhouse-staging.com` Cloud environment.

Connection details are **env-only** — the host, user, and password must NEVER be
written into any file (committed or not). Pass them via environment variables:

```
TARGET_CH_HOST=...   # service hostname (port 8443, HTTPS)
TARGET_CH_USER=...
TARGET_CH_PASSWORD=...
curl -sS --user "$TARGET_CH_USER:$TARGET_CH_PASSWORD" \
  --data-binary "<one SQL statement>" "https://$TARGET_CH_HOST:8443"
```

The HTTP interface takes one statement per request, so apply the files below one
at a time, in order.

Remaining human follow-ups:

1. Provision the CI secrets (`KAFKA_*` names below) with the service identity and
   the benchmark user's credentials.
2. Run `bootstrap/01_create_benchmark_user.sql` on the service with a real
   password (it stays authored-only, placeholder in place, until then).

## Files & run order

Run against the live service (e.g. `clickhouse client` or `clickhouse-connect`),
in this order:

| # | File | What |
|---|------|------|
| 1 | `clickbench/01_create_database.sql` | `CREATE DATABASE clickbench` |
| 2 | `clickbench/02_create_hits.sql` | Tier 1 target — 105-column `clickbench.hits` MergeTree (identical to Spark) |
| 3 | `clickbench/03_create_hits_null.sql` | Tier 0 target — identical 105 columns, `ENGINE = Null` (`clickbench.hits_null`) |
| 4 | `perf/01_create_database.sql` | `CREATE DATABASE perf` (metrics landing) |
| 5 | `perf/02_create_runs.sql` | `perf.runs` (one row per run; per-connector attrs in `runtime` map) |
| 6 | `perf/03_create_metrics.sql` | `perf.metrics` (tall/narrow per-run scalars) |
| 7 | `perf/04_create_ch_inserts.sql` | `perf.ch_inserts` (per-insert raw stats) |
| 8 | `bootstrap/01_create_benchmark_user.sql` | Benchmark role + user with least-privilege grants. **Cloud-only** (see file header); replace the password placeholder from the `KAFKA_TARGET_CH_PASSWORD` secret. |

The `perf/` files here are only the `0*_create_*` bootstrap tables. The capture /
insert queries (`10*..17*` in the Spark benchmark) are **task 30's** job and are not
ported here.

## CI secrets

The Spark benchmark uses `CLICKBENCH_TARGET_CH_*` for its target and
`CLICKBENCH_METRICS_CH_*` for the `perf.*` landing. Mirror them with a `KAFKA_`
prefix so the two benchmarks' secrets never collide:

| Spark secret | Kafka secret (this benchmark) | Purpose |
|---|---|---|
| `CLICKBENCH_TARGET_CH_HOST` | `KAFKA_TARGET_CH_HOST` | dedicated target hostname |
| `CLICKBENCH_TARGET_CH_PORT` | `KAFKA_TARGET_CH_PORT` | target port (secure native/HTTPS) |
| `CLICKBENCH_TARGET_CH_USER` | `KAFKA_TARGET_CH_USER` | benchmark user (`kafka_benchmark`) |
| `CLICKBENCH_TARGET_CH_PASSWORD` | `KAFKA_TARGET_CH_PASSWORD` | benchmark user password |
| `CLICKBENCH_METRICS_CH_HOST` | `KAFKA_METRICS_CH_HOST` | `perf.*` landing host (may equal the target host) |
| `CLICKBENCH_METRICS_CH_USER` | `KAFKA_METRICS_CH_USER` | `perf.*` landing user |
| `CLICKBENCH_METRICS_CH_PASSWORD` | `KAFKA_METRICS_CH_PASSWORD` | `perf.*` landing password |

If the `perf.*` landing lives on the same service as the target (simplest, as the
bootstrap user already has `perf.*` grants), the `KAFKA_METRICS_CH_*` secrets can
point at the same host/user as `KAFKA_TARGET_CH_*`.
