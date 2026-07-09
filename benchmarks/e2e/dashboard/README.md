# Benchmark v2 — Kafka dashboard verdict artifacts (#33)

This directory owns the **verdict map** and its **fixture-based acceptance** for the
Kafka Connect sink Benchmark v2 dashboard (Tab 1, plan `docs/benchmark-v2-plan.md`
§8; verdict semantics `docs/benchmark-v2-contract.md` §3, **Amendment 2026-07-09b**
— conformed in #33 Stage A2 per the manager's option-B ruling).

## The standing rule (principal mandate)

> **Any verdict-emitting artifact requires fixture-based acceptance before it ships.**

The Spark dashboard shipped verdict logic that mislabeled `0/0` (no-data) and
good-direction excursions as `REGRESSION`. The Kafka side builds the fix in from
day one: the verdict SQL (`sql/v_kc_pair_ratios.sql`) is accepted against a
synthetic truth-table fixture (`sql/fixture_verdict_truth_table.sql`) by
`test_verdicts.sh` **before** it can ship. No verdict artifact merges without a
green run of that test.

## Files

| File | Role |
|------|------|
| `sql/v_kc_pair_ratios.sql` | The H/P pair self-join + verdict map (CANONICAL copy). |
| `sql/fixture_verdict_truth_table.sql` | Synthetic `perf.runs`/`perf.metrics` rows covering every verdict branch. Idempotent (delete-by-prefix first). |
| `test_verdicts.sh` | The acceptance test: builds `perf.*` from the byte-locked DDL, applies the fixture, creates the view, asserts every case's verdict == expectation. Exits non-zero on any mismatch. |
| `superset/v_kc_pair_ratios.sql` | The DWH twin #33 deploys to Superset (same verdict bytes + DWH table names, connector scope, presentation columns). Accepted by `superset/verify_verdict_dwh.sh`. |
| `superset/` | The full #33 dashboard build package (datasets, chart specs, pre-checks) — see `superset/DASHBOARD.md`. |

## Verdict map (contract §3, Amendment 2026-07-09b)

Precedence (PINNED): **FAIL (integrity) > FLAG > NO_DATA / TRIPWIRE / band verdicts
/ OK**. FAIL is honoured upstream (failed-outcome runs never reach the view);
**FLAG beats NO_DATA** — a flagged pair is FLAGGED even when its ratio is
NULL/0-denominator or its tripwire is armed. (This REVERSES the pre-amendment
NO_DATA-first ordering; the fixture pins it.)

Banded metrics:

| Condition | Verdict |
|-----------|---------|
| either arm's run flagged (`runtime['flagged']='1'`) | `FLAGGED` (excluded from bands) |
| ratio is NULL (a side's metric missing) **or** pinned value = 0 (0-denominator) | `NO_DATA` |
| ratio outside the calibrated band, **good** direction | `IMPROVEMENT` |
| ratio outside the calibrated band, **bad** direction | `REGRESSION` |
| otherwise | `OK` |

Tripwire metric (`parts_per_insert`): flagged ⇒ `FLAGGED`; head value NULL/absent ⇒
`NO_DATA`; head value exactly `1.0` ⇒ `OK`; **any** deviation ⇒ `TRIPWIRE`
(structural invariant break — batches spraying across partitions; **alerts
regardless of calibration state**). The pinned value and ratio are ignored.

Direction turns "outside band" into good/bad:

- `higher_better`: `ratio > 1+band` → IMPROVEMENT; `ratio < 1−band` → REGRESSION
- `lower_better`: `ratio < 1−band` → IMPROVEMENT; `ratio > 1+band` → REGRESSION

### Gated metrics (Kafka registry)

| Metric | Tier | Direction | Rule |
|--------|------|-----------|------|
| `null_drain_rows_per_sec` | 0 | `higher_better` | banded **±8.5%** (calibrated, contract §3) |
| `connect_cpu_seconds_per_Mrows` | 0 | `lower_better` | banded **±6%** (cpu-per-Mrows family band — see the spelling note) |
| `drain_rows_per_sec` | 1 | `higher_better` | banded **±8.5%** (calibrated, contract §3) |
| `parts_per_insert` | 1 | (tripwire) | **TRIPWIRE**: head `== 1.0` ⇒ OK, else TRIPWIRE |

NOT gated (deliberately absent from the registry — no verdict rows):

- **`merge_amplification`** — DEMOTED to watch-only by the amendment (per-run merge
  noise, no pairing dividend; would manufacture false regressions). Stays a
  reported covariate (`v_kc_runs`, Tab 2 trend). The fixture asserts zero rows.
- **`ch_avg_rows_per_insert`** — the pre-amendment `>=50k` threshold gate is GONE:
  the amended gate composition ("replaces the prior flat-band gated set") does not
  include it; it is a plain §2.1 covariate. The fixture asserts zero rows.
- `settle_seconds` / `settle_timed_out` / `merge_pool_peak_pct` — covariates (unchanged).

**`connect_cpu_seconds_per_Mrows` band spelling (RATIFIED — Amendment
2026-07-09f, contract at `bd249f2`):** the pinned band table now names the Kafka
client-cpu spelling in the cpu-per-Mrows family row (±6%), and the Tier-0 gate
composition marks `serialize_seconds_per_Mrows` as Spark-specific ("a pipeline
gates only the metrics it emits") — both exactly as this registry implements.

## Alerting (contract §3)

The view exposes `alert_now = (verdict='TRIPWIRE') OR (verdict='REGRESSION' AND
alerts_enabled)`. During the calibration hold only integrity failures and the
parts TRIPWIRE alert; band REGRESSIONs arm at `alerts_enabled = 1`
(`comparable_pairs >= 20`). Alert queries **MUST** gate on `alert_now = 1` and
never re-derive the predicate — that is how dashboard and alerts drift apart.

## Fixture matrix and expected verdicts

Carriers: `null_drain_rows_per_sec` (higher_better, T0, ±8.5%),
`connect_cpu_seconds_per_Mrows` (lower_better, T0, ±6%), `drain_rows_per_sec`
band edges (T1, ±8.5%), `parts_per_insert` tripwire (T1), plus watch-only /
degated exclusion rows. Ratio = head / pinned. Full truth table in the header of
`sql/fixture_verdict_truth_table.sql` (31 named verdict cells + zero-row
exclusions); highlights:

| Fixture pair | Tier | Metric | Expected |
|--------------|------|--------|----------|
| `fixture-hb-090/100/110-unflagged` | 0 | null_drain_rows_per_sec | `REGRESSION` / `OK` / `IMPROVEMENT` |
| `fixture-hb-null/zerodenom-unflagged` | 0 | null_drain_rows_per_sec | `NO_DATA` |
| `fixture-hb-{090,100,110}-flagged` | 0 | null_drain_rows_per_sec | `FLAGGED` |
| `fixture-hb-null-flagged` | 0 | null_drain_rows_per_sec | `FLAGGED` (**FLAG > NO_DATA**) |
| `fixture-drain-{1080,0920}-unflagged` | 1 | drain_rows_per_sec | `OK` (edges just INSIDE ±8.5%) |
| `fixture-drain-1090-unflagged` | 1 | drain_rows_per_sec | `IMPROVEMENT` (edge just outside, hb) |
| `fixture-drain-0910-unflagged` | 1 | drain_rows_per_sec | `REGRESSION` (edge just outside, hb) |
| `fixture-cpu-090/100/110-unflagged` | 0 | connect_cpu_seconds_per_Mrows | `IMPROVEMENT` / `OK` / `REGRESSION` (lb) |
| `fixture-cpu-{1050,0950}-unflagged` | 0 | connect_cpu_seconds_per_Mrows | `OK` (edges just INSIDE ±6%) |
| `fixture-cpu-1070-unflagged` | 0 | connect_cpu_seconds_per_Mrows | `REGRESSION` (edge outside, bad dir) |
| `fixture-cpu-0930-unflagged` | 0 | connect_cpu_seconds_per_Mrows | `IMPROVEMENT` (edge outside, good dir) |
| `fixture-tw-ok-unflagged` | 1 | parts_per_insert | `OK` (head = 1.0) |
| `fixture-tw-armed-{high,low}-unflagged` | 1 | parts_per_insert | `TRIPWIRE` (head 1.02 / 0.98) |
| `fixture-tw-null-unflagged` | 1 | parts_per_insert | `NO_DATA` (head absent) |
| `fixture-tw-armed-flagged` | 1 | parts_per_insert | `FLAGGED` (**FLAG > armed tripwire**) |
| `fixture-tw-ok-pinnedmissing-unflagged` | 1 | parts_per_insert | `OK` (tripwire ignores pinned) |
| `fixture-watch-pair-unflagged` | 1 | merge_amplification, ch_avg_rows_per_insert | **NO ROWS** (watch-only / degated), despite excursion-sized values |
| `fixture-failed-run` | 1 | parts_per_insert | **excluded** — `runtime['outcome']='failed'`; produces no row |

Structural asserts on top of the named cells: fixture-exclusion guard present AND
effective, failed-run excluded by outcome value, idempotency, calibration hold
(all provisional, band alerts disabled), watch-only/degated zero-row, and the
alert rule (TRIPWIRE `alert_now=1` while provisional; provisional REGRESSIONs
`alert_now=0`).

## Calibration hold

Until a `(tier, metric)` has **≥ 20 unflagged, comparable pairs** its verdict is
**provisional** ("calibrating, n=X/20") and band alerts are suppressed. Columns:
`provisional`, `alerts_enabled`, `alert_now`. Comparability: banded = ratio
present; tripwire = head value present. The verdict value itself is always
computed; only band alerting is gated — the TRIPWIRE alerts through the hold.
At ~12 pairs the bands recalibrate from trailing-window statistics (contract §3
recalibration rule).

## How #33's Superset virtual dataset consumes this

Issue #33 registers `superset/v_kc_pair_ratios.sql` — the DWH twin whose verdict
bytes are identical to the canonical copy (only table names, the
`connector='kafka-connect'` scope and presentation columns differ; see its
header). Tab 1's gated-metrics table reads `(tier, metric_name, head_value,
pinned_value, ratio, band, is_tripwire, verdict, alert_now)` directly; verdicts
drive the tiles and the excursion/flagged-run log. Alert queries share the same
view and filter `alert_now = 1`, so dashboard and alerts cannot drift (plan §8).
`superset/verify_verdict_dwh.sh` re-runs the full fixture acceptance against the
DWH body, so the twin is held to the same 31/31 bar.

Two correctness points the view bakes in (both covered by the fixture):

- `SETTINGS join_use_nulls = 1` — an absent metric surfaces as NULL (→ NO_DATA), not
  the Float64 default 0 (which would false-verdict as REGRESSION / 0-denominator).
- Failed runs are excluded **by outcome value** (`runtime['outcome']='failed'`),
  never by key absence — legacy rows have no `outcome` key and default to success
  (contract §1.3).

## Running the acceptance tests

```
benchmarks/e2e/dashboard/test_verdicts.sh                # canonical view
benchmarks/e2e/dashboard/superset/verify_verdict_dwh.sh  # DWH twin
```

Requires `clickhouse` (clickhouse-local) on `PATH` (or `CLICKHOUSE_BIN` set). Both
build `perf.*` from the byte-locked DDL at `benchmarks/e2e/sql/perf/`, apply the
fixture, create the view, and assert all 31 verdict cases plus the structural
guards. Exit non-zero on any mismatch.
