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
| `v_kc_runs.sql` | base fact: one row per (arm,tier) kafka run, runtime unnested + metrics pivoted, integrity/headline_ok derived; carries merge_amplification + ch_avg_rows_per_insert as covariates | integrity + validity tiles, latest-pair scoping |
| `v_kc_pair_ratios.sql` | **verdict view** — DWH twin of the fixture-accepted `../sql/v_kc_pair_ratios.sql` (Amendment 2026-07-09b, 31/31), verdict map preserved byte-for-byte + `pair_ts`/`pair_seq`/`flag_reason` presentation columns | verdict tiles, gated table, ratio trends, pairs-in-band, alert queries (`alert_now`) |
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
  kc_t1_ratio_trend_tier0    <- v_kc_pair_ratios (tier=0; per-metric bands:
                                null_drain ±8.5%, connect_cpu ±6%)
  kc_t1_ratio_trend_tier1    <- v_kc_pair_ratios (tier=1; drain ±8.5%;
                                parts_per_insert EXCLUDED — tripwire, not a ratio)
Row 4:
  kc_t1_excursion_flagged_log <- v_kc_flagged_log
```

Every chart spec carries a `description` (what it shows / how to read it /
direction of goodness) — required by the #33 directive. The verdict tiles and the
gated table read `verdict`/`provisional`/`alert_now` straight from
`v_kc_pair_ratios`, and the alert queries (future) share the same view filtered
`alert_now = 1`, so **dashboard and alerts cannot drift** (plan §8). Per contract
§3 the TRIPWIRE alerts even during the calibration hold; band REGRESSIONs arm at
`alerts_enabled = 1`.

`drain_rate_stability` is **not charted** (principal ruling, #33 constraint 3 —
commit-cadence-dominated pending refinement).

## Contract level: Amendment 2026-07-09b (drift RESOLVED — ruling option B)

The Stage-A package initially carried the pre-amendment map (flat T0 ±3% / T1 ±5%,
merge_amplification gated, ch_avg >=50k threshold; 20/20 accepted at commit
`3b7cc59`). The manager ruled **option B**: conform to the ratified contract
BEFORE Stage B. Stage A2 delivered that: calibrated per-metric bands
(null_drain/drain ±8.5%, connect_cpu ±6%), `parts_per_insert` as a binary
TRIPWIRE, `merge_amplification` watch-only (no verdict row; reported on Tab 2),
`ch_avg_rows_per_insert` degated (covariate in `v_kc_runs`), precedence
FLAG > NO_DATA, and the `alert_now` rule. The fixture was extended to 31 named
cells (+ zero-row exclusion asserts) and BOTH acceptance suites re-ran green —
see `../README.md` for the full amended map and truth table.

Items still FLAGGED for upstream (not resolved here):

- `connect_cpu_seconds_per_Mrows` is absent from the contract's pinned band
  table (only `ch_insert_cpu_seconds_per_Mrows`/`cpu_seconds_per_Mrows` appear);
  gated at the family ±6% pending a one-line contract amendment.
- The contract §3 Tier-0 gate composition names `serialize_seconds_per_Mrows`,
  which the Kafka pipeline does not emit (Spark event-log specific) — the wording
  is Spark-centric; the Kafka registry omits it rather than gate a perpetual
  NO_DATA row.
- Spark's shipped `v_verdict_fixture_check.sql` cannot represent NO_DATA for an
  absent HEAD tripwire metric (its INNER-JOIN shape drops the row entirely),
  where the contract says NULL/absent parts ⇒ NO_DATA — a Spark-side gap, noted
  not fixed.

## Acceptance checklist

Stage A + A2 (done):

- [x] Stage A baseline: `../test_verdicts.sh` → 20/20 on the pre-amendment map.
- [x] Stage A2 conformance: canonical + fixture + tests updated to Amendment
      2026-07-09b; `../test_verdicts.sh` → **31/31** + structural asserts
      (watch-only/degated zero-row, FLAG>NO_DATA, tripwire alert rule).
- [x] DWH twin: `verify_verdict_dwh.sh` → **31/31** against the DWH view body
      (table names swapped back to `perf.*`; verdict multiIf / gated registry /
      calibration + alert columns byte-identical to the canonical view — the
      adaptation is table-name + connector scope + presentation columns only).
- [x] `v_kc_runs.sql`, `v_kc_flagged_log.sql`, `v_kc_pair_ratios.sql` all create
      and run against clickhouse-local (perf.* substitution), returning 0 rows
      without error when no kafka rows exist; a seeded real pair produces correct
      verdicts, `pair_ts`, `pair_seq=1`, `provisional=1` (n=1 calibration hold).

Stage B (gated on the browser-window GO):

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
| `verify_verdict_dwh.sh` | fixture acceptance for the adapted verdict view (31/31) |
| `DASHBOARD.md` | this file |
