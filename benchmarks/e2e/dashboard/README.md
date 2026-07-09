# Benchmark v2 — Kafka dashboard verdict artifacts (#33 prep)

This directory owns the **verdict map** and its **fixture-based acceptance** for the
Kafka Connect sink Benchmark v2 dashboard (Tab 1, plan `docs/benchmark-v2-plan.md`
§8; verdict semantics `docs/benchmark-v2-contract.md` §3).

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
| `sql/v_kc_pair_ratios.sql` | The H/P pair self-join + verdict map. **#33's Superset virtual dataset consumes this file VERBATIM** (see below). |
| `sql/fixture_verdict_truth_table.sql` | Synthetic `perf.runs`/`perf.metrics` rows covering every verdict branch. Idempotent (delete-by-prefix first). |
| `test_verdicts.sh` | The acceptance test: builds `perf.*` from the byte-locked DDL, applies the fixture, creates the view, asserts every case's verdict == expectation. Exits non-zero on any mismatch. |

## Verdict map (per gated metric)

Applied identically to every gated metric. Precedence: **NO_DATA → FLAGGED →
band/threshold → OK**.

| Condition | Verdict |
|-----------|---------|
| ratio is NULL (a side's metric missing) **or** pinned value = 0 (0-denominator) | `NO_DATA` |
| either arm's run is flagged (`runtime['flagged']='1'`) | `FLAGGED` (excluded from bands) |
| ratio excursion beyond band, **good** direction | `IMPROVEMENT` |
| ratio excursion beyond band, **bad** direction | `REGRESSION` |
| otherwise | `OK` |

Direction turns "beyond band" into good/bad:

- `higher_better`: `ratio > 1+band` → IMPROVEMENT; `ratio < 1−band` → REGRESSION
- `lower_better`: `ratio < 1−band` → IMPROVEMENT; `ratio > 1+band` → REGRESSION

### Gated metrics and directions

| Metric | Tier | Direction | Band / rule |
|--------|------|-----------|-------------|
| `null_drain_rows_per_sec` | 0 | `higher_better` | ±3% |
| `connect_cpu_seconds_per_Mrows` | 0 | `lower_better` | ±3% |
| `drain_rows_per_sec` | 1 | `higher_better` | ±5% |
| `parts_per_insert` | 1 | `lower_better` | ±5% |
| `merge_amplification` | 1 | `lower_better` | ±5% |
| `ch_avg_rows_per_insert` | 1 | `higher_better` | **threshold**: HEAD `>= 50000` → OK else REGRESSION (plan §6 batching contract, not a ratio band) |

**Direction spelling assumption:** the literal strings `higher_better` /
`lower_better` are used pending a contract amendment the principal flagged as
possible. They are centralized in the `gated` CTE of `v_kc_pair_ratios.sql`, so an
amendment is a one-line change. See the report/handoff note.

## Fixture matrix and expected verdicts

Direction carriers: `null_drain_rows_per_sec` (higher_better, T0, ±3%) and
`parts_per_insert` (lower_better, T1, ±5%); threshold carrier
`ch_avg_rows_per_insert`. Ratio = head / pinned (`0.90`→head 90, `1.00`→100,
`1.10`→110, pinned 100).

| Fixture pair | Tier | Metric | Expected |
|--------------|------|--------|----------|
| `fixture-hb-090-unflagged` | 0 | null_drain_rows_per_sec | `REGRESSION` |
| `fixture-hb-100-unflagged` | 0 | null_drain_rows_per_sec | `OK` |
| `fixture-hb-110-unflagged` | 0 | null_drain_rows_per_sec | `IMPROVEMENT` |
| `fixture-hb-null-unflagged` | 0 | null_drain_rows_per_sec | `NO_DATA` (pinned side missing) |
| `fixture-hb-zerodenom-unflagged` | 0 | null_drain_rows_per_sec | `NO_DATA` (pinned = 0) |
| `fixture-hb-090-flagged` | 0 | null_drain_rows_per_sec | `FLAGGED` |
| `fixture-hb-100-flagged` | 0 | null_drain_rows_per_sec | `FLAGGED` |
| `fixture-hb-110-flagged` | 0 | null_drain_rows_per_sec | `FLAGGED` |
| `fixture-lb-090-unflagged` | 1 | parts_per_insert | `IMPROVEMENT` |
| `fixture-lb-100-unflagged` | 1 | parts_per_insert | `OK` |
| `fixture-lb-110-unflagged` | 1 | parts_per_insert | `REGRESSION` |
| `fixture-lb-null-unflagged` | 1 | parts_per_insert | `NO_DATA` (pinned side missing) |
| `fixture-lb-zerodenom-unflagged` | 1 | parts_per_insert | `NO_DATA` (pinned = 0) |
| `fixture-lb-090-flagged` | 1 | parts_per_insert | `FLAGGED` |
| `fixture-lb-100-flagged` | 1 | parts_per_insert | `FLAGGED` |
| `fixture-lb-110-flagged` | 1 | parts_per_insert | `FLAGGED` |
| `fixture-thr-above-unflagged` | 1 | ch_avg_rows_per_insert | `OK` (head 60000) |
| `fixture-thr-below-unflagged` | 1 | ch_avg_rows_per_insert | `REGRESSION` (head 40000) |
| `fixture-thr-null-unflagged` | 1 | ch_avg_rows_per_insert | `NO_DATA` (head metric missing) |
| `fixture-thr-above-flagged` | 1 | ch_avg_rows_per_insert | `FLAGGED` |
| `fixture-failed-run` | 1 | parts_per_insert | **excluded** — `runtime['outcome']='failed'`; produces no row |

`FLAGGED` cases are built on comparable ratios (0.90/1.00/1.10) so the FLAGGED
override — not NO_DATA — is what is being asserted. `NO_DATA` beats `FLAGGED` in the
precedence, which is why the missing/0-denominator cases are kept unflagged.

## Calibration hold

Plan §8 bands are "recalibrated from the first ~20 pairs". Until a `(tier, metric)`
has **≥ 20 unflagged, comparable pairs**, its verdict is **provisional** and band
alerts are **suppressed**. The view encodes this as two columns:

- `provisional` (`UInt8`) — 1 while `comparable_pairs < 20`.
- `alerts_enabled` (`UInt8`) — `NOT provisional`. Tab 1 alert queries (GitHub
  issue / Slack on band excursion or integrity failure) **MUST** gate on
  `alerts_enabled = 1`.

The verdict value itself is always computed; only alerting is gated. (Every fixture
`(tier, metric)` has 1 comparable pair, so all fixture rows are provisional with
alerts disabled — the test asserts this.)

## How #33's Superset virtual dataset consumes this

Issue #33 (Tab 1 + alerts) registers `v_kc_pair_ratios.sql` as a Superset **virtual
dataset verbatim** — the `CREATE OR REPLACE VIEW perf.v_kc_pair_ratios AS <body>` is
the dataset's SQL, unmodified. Tab 1's gated-metrics table reads
`(tier, metric_name, head_value, pinned_value, ratio, band, verdict)` directly; the
`REGRESSION`/`IMPROVEMENT`/`OK`/`NO_DATA`/`FLAGGED` verdict drives the tile coloring
and the excursion/flagged-run log. Alert queries share the same view and filter
`verdict = 'REGRESSION' AND alerts_enabled = 1`, so dashboard and alerts cannot
drift apart (plan §8). Because it is consumed verbatim, the acceptance test runs the
**same bytes** that ship.

Two correctness points the view bakes in (both covered by the fixture):

- `SETTINGS join_use_nulls = 1` — an absent metric surfaces as NULL (→ NO_DATA), not
  the Float64 default 0 (which would false-verdict as REGRESSION / 0-denominator).
- Failed runs are excluded **by outcome value** (`runtime['outcome']='failed'`),
  never by key absence — legacy rows have no `outcome` key and default to success
  (contract §1.3).

## Running the acceptance test

```
benchmarks/e2e/dashboard/test_verdicts.sh
```

Requires `clickhouse` (clickhouse-local) on `PATH` (or `CLICKHOUSE_BIN` set). Builds
`perf.*` from the byte-locked DDL at `benchmarks/e2e/sql/perf/`, applies the fixture,
creates the view, and asserts all 20 verdict cases plus the structural guards
(fixture-exclusion present and effective, failed-run excluded, idempotency,
calibration hold). Exits non-zero on any mismatch.
