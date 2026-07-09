# Benchmark v2 — Kafka dashboard build package (Tab 1 REGRESSION)

Issue #33. This directory is the **build package** for the Kafka Connect sink
Benchmark v2 dashboard, Tab 1, on the **production DWH Superset**. It is the SQL +
chart specs + acceptance harness that Stage B applies to Superset via the
authorized mechanism. Nothing here mutates an existing asset — the build is
**additive-only** (new datasets, new charts, one new dashboard). The existing
Spark dashboard (424) and the shared `DWH` database connection (uuid `dc93cd97`)
are reused **read-only**.

## Mechanism (discovered from the Spark v2 build)

The Spark dashboard was built by a **Superset REST API script**
(`benchmarks/dashboard/superset/build_superset.py` + `describe_charts.py`), not by
hand. It:

- authenticates `POST /api/v1/security/login` (Bearer token) + a CSRF token from
  `/api/v1/security/csrf_token/`, then
- registers **virtual datasets** (`POST /api/v1/dataset/` with `database`, `schema`,
  `table_name`, and a raw `sql` body — the view body verbatim),
- creates **charts** (`POST /api/v1/chart/`, viz params as JSON) and a
  **dashboard** (`POST /api/v1/dashboard/` with a `position_json` grid), and
- sets each chart's `description` via `PUT /api/v1/chart/{id}` (`describe_charts.py`).

That script targeted a **local** Superset (`localhost:8088`, admin/admin) against a
`clickhousedb://…@host:8443/perf?secure=true` connection it created. **On the
production DWH the auth path and connection differ**: Superset is per-user auth
(no shared admin/admin), and the datasets must be bound to the **existing** `DWH`
connection (uuid `dc93cd97`) — we do NOT create a database connection. So Stage B
uses whichever mechanism the manager authorizes:

- **Browser (chrome-devtools MCP)** — log into the DWH Superset as the current
  user, create the datasets/charts/dashboard through the UI, pasting each view
  body and each chart description from this package; or
- **Script/API** — the same REST calls as `build_superset.py`, but authenticated as
  the current user and pointed at the DWH, reusing `dc93cd97` (no `POST /database/`).

Either way the **dataset SQL and chart descriptions are the bytes in this
directory**, so the dashboard matches what was accepted here.

### Two `build_superset.py` bugs (identified; NOT replicated here)

Both are already fixed in the Spark source with a `BUGFIX:` comment, and this
package avoids re-introducing them:

1. **Metric label == column name → ClickHouse Code 215.** When a chart's metric
   label equalled the grouped/ordered column, Superset emitted
   `MAX(col) AS col … ORDER BY col`, which the ClickHouse new analyzer rejects
   ("not under aggregate and not in GROUP BY"). Fix: give the metric a distinct
   alias (`MAX(col)`), never `label == column`. (`build_superset.py` ~L118-123.)
   → Our `tab1_charts.json` never binds a metric alias equal to its column.
2. **Charts placed twice in `position_json`.** `made` can carry the same chart id
   more than once on an idempotent re-run (find-by-`slice_name` reuse), and the
   layout loop appended every entry, so each chart landed twice in the grid. Fix:
   de-duplicate by chart id, first-seen order, before building the layout.
   (`build_superset.py` ~L138-143.) → Stage B must de-dupe chart ids before laying
   out the grid (documented for both the API and browser paths).

## DWH table names (exact — from the Spark v2 dataset SQL)

The DWH mirror (ClickPipe) of the metrics `perf.*` schema:

| Local (`perf.*`) | DWH mirror (this package reads these) |
|---|---|
| `perf.runs` | `raw_connectors_load_testing.runs` |
| `perf.metrics` | `raw_connectors_load_testing.metrics` |

(Grounded in `benchmarks/dashboard/v2/v_runs_enriched.sql` and `v_pair_ratios.sql`
headers: "DWH mirror: raw_connectors_load_testing.{runs,metrics}".) The DWH also
carries a `ch_inserts` mirror; Tab 1 does not use it.

## Datasets (virtual, on the `DWH` connection — additive)

| Dataset SQL | Role | Feeds |
|---|---|---|
| `v_kc_runs.sql` | base fact: one row per (arm,tier) kafka run, runtime unnested + metrics pivoted, integrity/headline_ok derived | integrity + validity tiles, latest-pair scoping |
| `v_kc_pair_ratios.sql` | **verdict view** — DWH twin of the fixture-accepted `../sql/v_kc_pair_ratios.sql`, verdict map preserved byte-for-byte + `pair_ts`/`pair_seq`/`flag_reason` for charts | verdict tiles, gated table, ratio trends, pairs-in-band |
| `v_kc_flagged_log.sql` | flagged + failed run log (audit trail of runs excluded from ratios) | excursion/flagged log |

All three are Kafka-scoped (`connector='kafka-connect'`) and exclude the reserved
fixture connector (`__verdict_fixture__`). The connector scope is by **value**, not
instance, so the Tab 5 cross-connector matched-dataset rule (#36) is not precluded.

## Dataset → chart wiring (Tab 1, 8 charts — `tab1_charts.json`)

```
Row 1 (5 tiles):
  kc_t1_tile_tier0_verdict   <- v_kc_pair_ratios (tier=0, pair_seq=1)
  kc_t1_tile_tier1_verdict   <- v_kc_pair_ratios (tier=1, pair_seq=1)
  kc_t1_tile_integrity       <- v_kc_runs (latest pair)
  kc_t1_tile_validity        <- v_kc_runs (latest pair, flagged count)
  kc_t1_tile_pairs_in_band   <- v_kc_pair_ratios (unflagged, OK/IMPROVEMENT ÷ min(n,20))
Row 2:
  kc_t1_latest_pair_gated_table <- v_kc_pair_ratios (pair_seq=1, full gated detail)
Row 3 (2 trend lines):
  kc_t1_ratio_trend_tier0    <- v_kc_pair_ratios (tier=0, band ±3% shaded)
  kc_t1_ratio_trend_tier1    <- v_kc_pair_ratios (tier=1, band ±5% shaded)
Row 4:
  kc_t1_excursion_flagged_log <- v_kc_flagged_log
```

Every chart spec carries a `description` (what it shows / how to read it /
direction of goodness) — required by the #33 directive. The verdict tiles and the
gated table read `verdict`/`provisional` straight from `v_kc_pair_ratios`, and the
alert queries (future) share the same view filtered `verdict='REGRESSION' AND
alerts_enabled=1`, so **dashboard and alerts cannot drift** (plan §8).

`drain_rate_stability` is **not charted** (principal ruling, #33 constraint 3 —
commit-cadence-dominated pending refinement).

## ⚠ Contract-drift decision (BLOCKING for Stage B — needs a principal ruling)

The verdict map shipped here is the one that **passed fixture acceptance 20/20**
(`../sql/v_kc_pair_ratios.sql`, commit `3b7cc59`) and #33 instructs "PRESERVE the
verdict semantics exactly". That map is the **OLD contract**: flat bands (T0 ±3% /
T1 ±5%), `parts_per_insert` as a lower_better ratio band, `merge_amplification`
gated, `ch_avg_rows_per_insert` as a >=50k threshold.

Contract §3 was **since re-vendored** (commits `22ed529` / `f2c008f` / `85c5e73`,
Amendment 2026-07-09b) to:

- **calibrated per-metric bands** (same on both tiers): throughput ±9%, drain/null
  ±8.5%, cpu ±6%, serialize ±8.5%;
- **`merge_amplification` DEMOTED to watch-only** (not gated);
- **`parts_per_insert` = binary TRIPWIRE** (head==1.0 ⇒ OK, else TRIPWIRE), not a
  ratio band.

The Spark side already implements the amendment
(`benchmarks/dashboard/v2/v_verdict_fixture_check.sql`). Shipping the old map on a
contract-current dashboard is exactly the "verdict drift" the acceptance apparatus
exists to catch. **Decision owner: principal.** Options: (a) ship the accepted old
map now (what this package does), amend later; (b) adopt the amendment first —
requires a NEW fixture covering the tripwire + calibrated bands and a fresh 20/20
run before ship. Adopting is a one-place change in the `gated` CTE of
`v_kc_pair_ratios.sql` plus band constants; the trend-chart band shading in
`tab1_charts.json` would change with it. **Stage B is paused on this ruling.**

## Acceptance checklist

Stage A (done):

- [x] Baseline: `../test_verdicts.sh` → 20/20 on `perf.*` (accepted map).
- [x] DWH adaptation: `verify_verdict_dwh.sh` → 20/20 against the DWH view body
      (table names swapped back to `perf.*`; verdict multiIf / gated registry /
      calibration columns are byte-identical to the accepted view — the
      adaptation is table-name + connector scope + presentation columns only).
- [x] `v_kc_runs.sql`, `v_kc_flagged_log.sql`, `v_kc_pair_ratios.sql` all create
      and run against clickhouse-local (perf.* substitution), returning 0 rows
      without error when no kafka rows exist; a seeded real pair produces correct
      verdicts, `pair_ts`, `pair_seq=1`, `provisional=1` (n=1 calibration hold).

Stage B (gated on the browser-window GO + the contract-drift ruling):

- [ ] `visibility_precheck.sql` Q1/Q1b: the 8 kafka run_ids (2 pairs × 2 arms × 2
      tiers) present on the DWH; 4 runs per pair. ABORT if absent.
- [ ] Q2/Q2b/Q2c: pair-1 (`2026-07-08T11-52-46Z-e140231`) quarantined
      (`flagged='1'`, `flag_reason` contains `instrument_resize`, 3 corrected rows
      `batch_size='100000'`); 0 un-quarantined rows.
- [ ] Q3/Q3b: pair-2 (`2026-07-09T10-04-24Z-e4b1f7a`) clean
      (`mapContains(runtime,'flagged')=0`, not failed); 0 non-clean rows.
- [ ] Create the 3 datasets on `DWH` (`dc93cd97`), do NOT create a DB connection.
- [ ] If the mechanism allows, re-run fixture acceptance against the REAL created
      dataset SQL.
- [ ] Build the 8 Tab-1 charts (de-dupe chart ids before layout — bug #2), each
      with its `description`, then the dashboard.
- [ ] Q4: pair-2 present in `v_kc_pair_ratios` (provisional=1); pair-1 ABSENT from
      ratios but present in `v_kc_flagged_log` (log_class=FLAGGED).
- [ ] Verify every chart renders with pair-2 data; screenshot evidence.
- [ ] Confirm verdicts display "provisional — calibrating (n=X/20)" and no band
      alerts are wired while provisional.

## Files

| File | Purpose |
|---|---|
| `v_kc_runs.sql` | base-fact virtual dataset |
| `v_kc_pair_ratios.sql` | DWH-adapted verdict view (verbatim verdict map + presentation cols) |
| `v_kc_flagged_log.sql` | flagged/failed run log |
| `tab1_charts.json` | 8 Tab-1 chart specs, each with its description |
| `visibility_precheck.sql` | Stage-B pre-flight queries (run first; abort on failure) |
| `verify_verdict_dwh.sh` | fixture acceptance for the adapted verdict view (20/20) |
| `DASHBOARD.md` | this file |
