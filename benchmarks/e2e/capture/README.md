# Kafka Connect sink benchmark v2 — CH-side capture pipeline (task 30)

Ports the ClickHouse-side capture / integrity / export / rollback pipeline from
the Spark benchmark (`spark-clickhouse-connector/benchmarks/`). See
[`PORTING.md`](./PORTING.md) for the per-source-file disposition, the
pinned-name mapping, the query_log filter, and the gating/rollback discipline.

- **Scripts** live here (`benchmarks/e2e/capture/`).
- **Capture SQL** lives in `../sql/capture/` (files `11`–`23`; `23` is
  tier-0-only — see the order of operations below).
- Bootstrap DDL + the byte-locked `perf.*` schema live in `../sql/` (see
  `../sql/README.md`).

## Layout

| File | Role |
|---|---|
| `config.env` | Non-secret repo config. **`source` it first.** Carries the contract §1.1 mandatory scope keys (`TARGET_REGION=us-east-2`, `ENVIRONMENT_CLASS=staging`, `COMPUTE_REGION=us-east-2`), `CONNECTOR=kafka-connect`, the `QUERY_LOG_USER` query_log filter user, and the default target db/table. |
| `lib_runid.sh` | Generates `RUN_ID`/`RUN_START`. HARD-FAILS on a `-nogit` id (contract §1.2). `source` it. |
| `ch_common.py` | clickhouse-connect helper (host/user/password from env). |
| `run_metrics_sql.py <file>` | Runs one parameterized capture SQL file against the perf.* service. |
| `truncate_target.py` | Resets the Tier 1 target before a measured drain. |
| `wait_for_settle.py` | Polls until merges settle; prints the settle-end timestamp. |
| `insert_run_record.py` | Writes the `perf.runs` row (gated, after full capture). |
| `rollback_run_metrics.py` | Deletes a run_id's rows from all perf tables on failure. |
| `check_integrity.py` | Reads back the integrity verdict; fails the run on a mismatch. |
| `export_metrics_to_dwh.py` | Streams perf.* to the DWH S3 bucket (gated, last). |
| `integrity_math.py` + `tests/` | Pure-Python mirror of the integrity math + pytest. |

## Order of operations (the gating discipline — see PORTING.md)

```
source config.env ; source lib_runid.sh          # RUN_ID/scope keys in env
RUN_ID=$RUN_ID-<arm>-t<tier>                       # per (arm,tier) row (contract §1.2)

# pre-drain (before the connector starts, no RUN_END yet):
python3 run_metrics_sql.py ../sql/capture/21_pre_run_covariates.sql
python3 truncate_target.py                         # Tier 1 only

# ... orchestrator/poller runs the drain, sets RUN_END = lag-0 timestamp ...
SETTLE_END=$(python3 wait_for_settle.py)           # SETTLE_STATUS_FILE => timed-out flag

# gated capture (numeric order; 15 before 19); on ANY failure -> rollback_run_metrics.py
# 20 (integrity) runs on tier 1 only; 23 (ch_insert_cpu_share_tier0 parse-watch)
# runs on tier 0 only — the orchestrator (run_pair.sh) gates both by tier.
for f in 11 12 13 14 15 16 17 18 19 20 22 23 ; do
  python3 run_metrics_sql.py ../sql/capture/${f}_*.sql || { python3 rollback_run_metrics.py; exit 1; }
done

python3 insert_run_record.py    || { python3 rollback_run_metrics.py; exit 1; }  # runs row AFTER metrics
python3 export_metrics_to_dwh.py                                                   # gated on runs row
python3 check_integrity.py                                                         # decides verdict LAST
```

## Environment

Secrets are **env-only** (never in any file). See `../sql/README.md` for the
`KAFKA_*` CI-secret names. The capture scripts read:

- `METRICS_CH_HOST` / `METRICS_CH_USER` / `METRICS_CH_PASSWORD` — the `perf.*` landing.
- `TARGET_CH_HOST` / `TARGET_CH_USER` / `TARGET_CH_PASSWORD` — the drain target.
- `CH_DATABASE` / `CH_TABLE` — target db/table (config.env default; orchestrator overrides `CH_TABLE` per tier: `hits` Tier 1, `hits_null` Tier 0).
- `RUN_ID` / `RUN_START` / `RUN_END` / `SETTLE_END` / `SETTLE_SECONDS` / `SETTLE_TIMED_OUT`.
- `QUERY_LOG_USER` — query_log filter user (config.env).
- `SOURCE_ROWS_EXPECTED` / `SOURCE_UNIQUE_EXPECTED` — integrity ground truth (producer committed-offset count + staged uniqExact(WatchID)).
- `GIT_SHA` / `CONNECTOR_VERSION` / `RUNTIME` (JSON) — for the runs row.
- `TARGET_REGION` / `ENVIRONMENT_CLASS` / `COMPUTE_REGION` — mandatory scope keys (config.env).
- `DWH_ROLE_ARN` / `DWH_BUCKET` (+ optional `DWH_BUCKET_REGION`) — export.

## DWH bucket — pending decision (not hardcoded)

`export_metrics_to_dwh.py` requires `DWH_BUCKET` explicitly (no baked-in default,
unlike the Spark script's `connectors-load-testing-metrics`). Whether the Kafka
benchmark reuses the shared Spark DWH bucket, uses its own, and whether a
us-east-2-local scratch bucket is used (to keep the S3→ClickPipe path
intra-region with the us-east-2 target) is a **pending region decision**. Set the
bucket via the CI env once decided; nothing here bakes in a bucket name.

## Live validation is deferred to task 31

There are no target credentials in the CI/dev sandbox, so the capture SQL and the
query_log filter (`has(tables) [+ user=kafka_benchmark]`) are **not validated
against a live target here**. Task 31's first manual end-to-end run validates:
the query_log filter actually matches the sink's inserts, `remoteSecure` reaches
the target on 9440, and the grants in `../sql/bootstrap/02_additional_capture_grants.sql`
are sufficient. This is a known, accepted gap.
