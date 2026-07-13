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

Follow-ups: the two contract gaps A2 flagged (connect_cpu band spelling; the
Spark-centric serialize wording) were RATIFIED upstream by Amendment 2026-07-09f
(contract re-vendored at `bd249f2`) — the registry here already matched, so no
code change resulted. Still flagged, not resolved:

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

---

# Tab 4 — RUN DRILL (issue #34)

Additive extension of this package with the Tab 4 "RUN DRILL" build (dashboard
**432**, prod DWH Superset). Tab 4 is the **one-pair diagnostic**: pick a pair, see
its headline numbers, the full HEAD-vs-pinned metric table, and the streaming-sink
shape (drain curve, remaining lag, per-insert latency, batch-size distribution).
Strictly **additive** — no existing chart, dataset, or the Tab-1 content is modified.

The drill is **descriptive, not a gate**: it carries NO verdict/band/alert column.
Tab 1's `v_kc_pair_ratios` remains the single source of verdict + alert truth, so
this tab cannot drift from the alerting logic.

## Datasets (Tab 4 — additive; `DWH` connection `dc93cd97`, no DB connection created)

| Dataset SQL | New/Reused | Role | Feeds |
|---|---|---|---|
| `v_kc_runs.sql` | **REUSED** (from Tab 1, unchanged) | base fact per run | 5 big-number tiles + config panel |
| `v_kc_run_drill.sql` | **NEW** | arm-comparison long view: per (pair,tier,metric) HEAD/pinned/ratio/delta%, ALL metrics, no verdict | ARM COMPARISON table + the tab's `pair_id` filter |
| `v_kc_drain_curve.sql` | **NEW** | per-minute drain shape derived from `ch_inserts`: rows/s, cumulative, remaining_lag, minute_index, arm | DRAIN CURVE + REMAINING LAG (H vs P) |
| `v_kc_inserts_drill.sql` | **NEW** | raw per-insert grain from `ch_inserts`: seq, elapsed_s, duration_ms, written_rows, arm | per-insert latency sequence + batch-size distribution |

Reuse note: the big-number row (rows verified / drain_s / dup=0 / guards OK / cost)
and the covariates/config panel are built on the EXISTING `v_kc_runs` — no new view.
Only the all-metrics arm table and the two `ch_inserts`-derived views are new. All
three new views are Kafka-scoped (`connector='kafka-connect'`), exclude the fixture
connector, exclude failed-outcome runs by value, and carry the same NULL-safe
`pair_ts`/`pair_seq` presentation columns as `v_kc_pair_ratios`.

### DWH `ch_inserts` mirror (used by Tab 4; Tab 1 did not use it)

The DWH also carries the `ch_inserts` mirror (`04_create_ch_inserts.sql` — per-insert
`event_time`, `query_duration_ms`, `written_rows`, ...). Task-34 data reality:
~380-420 rows per run for all 3 clean pairs (+ pair 1 partially), so the drain/lag
curves and the per-insert drill are fully buildable from existing capture — **no new
table** (plan §9: "Drain/lag curves → derived from existing `perf.ch_inserts` … at
query time").

| Local (`perf.*`) | DWH mirror |
|---|---|
| `perf.ch_inserts` | `raw_connectors_load_testing.ch_inserts` |

### Drain-curve bucketing SQL approach (`v_kc_drain_curve`)

Exactly the plan §8 data-foundation sketch ("ch_inserts bucketed per minute:
sum(written_rows)/60; cumulative sum vs rows_expected → remaining-lag curve"):

- `minute = toStartOfMinute(event_time)` — 60s bucket key per run.
- `minute_rows = sum(written_rows)` within the bucket.
- `rows_per_sec = minute_rows / 60.0` — the plan's "/60".
- `cumulative_rows = sum(minute_rows) OVER (PARTITION BY run_id ORDER BY minute ROWS
  UNBOUNDED PRECEDING → CURRENT ROW)` — monotone running sum.
- `remaining_lag = rows_expected − cumulative_rows`, with `rows_expected` LEFT-joined
  from the run's `rows_expected` metric (argMax by `recorded_at`); NULL-safe (a run
  with no `rows_expected` → NULL lag, curve simply omits the lag line).
- `minute_index = intDiv(minute − min(minute) OVER (PARTITION BY run_id), 60)` — a
  0-based "minutes since drain start" so the HEAD and pinned curves of one pair
  overlay on a common x-axis even when their wall-clock start times differ.

The last bucket is partial (<60s wall clock) → its rows/s is a lower bound
(documented; the curve is a shape read, not a gated metric).

## Deployed flagged predicate (deployment deviation — REPLICATED)

The **deployed** Tab-1 datasets on the DWH test the validity flag as
`runtime['flagged'] IN ('1','true')` (the harness has emitted both spellings),
whereas the **repo** Tab-1 SQL tests `= '1'`. Tab 4 sits beside the deployed assets,
so the three new views **replicate the deployed predicate** `IN ('1','true')` for
consistency. Each new view carries a header note flagging this. On Tab 4 `flagged` is
only presentation/context (a drilled pair renders even when flagged — that is the
point of a drill); Tab 1's `v_kc_pair_ratios` still owns verdict/exclusion truth, so
this deviation does not touch any gate.

> **Coordinator note:** this is an intentional match to the live deployment, NOT a
> repo bug. If/when the Tab-1 deployed datasets are re-normalized to `= '1'`, align
> Tab 4 in the same pass.

## Pair filter (tab-wide)

The whole tab is scoped by ONE Superset **native filter** on `pair_id`:

- Filter column: `pair_id`, target dataset `v_kc_run_drill` (all four Tab-4 datasets
  carry `pair_id` with identical spelling per contract §1.2, so one filter scopes the
  whole tab).
- **Default = the latest pair**: set the filter's default value to the newest
  `pair_id` (max `pair_ts`, i.e. `pair_seq = 1`) at build time. Tile queries also fall
  back to the latest pair (`argMax(pair_id, …)`) so an unset filter still shows the
  newest pair.

## Layout grid (Tab 4 — 12-wide, `tab4_charts.json`)

```
[ Filter: pair_id (native, default = latest pair) ]
Row 1 (5 big-number tiles, width 2 each):
  kc_t4_tile_rows_verified   <- v_kc_runs        (sum rows_delivered)
  kc_t4_tile_drain_s         <- v_kc_drain_curve (head t1 drain span)
  kc_t4_tile_dup_zero        <- v_kc_runs        (duplicate_rows=0?)
  kc_t4_tile_guards_ok       <- v_kc_runs        (flagged count)
  kc_t4_tile_cost            <- v_kc_runs        (sum run_cost_usd, per-pair)
Row 2 (width 12):
  kc_t4_arm_comparison       <- v_kc_run_drill   (all metrics H|P|ratio|delta%)
Row 3 (2 x width 6):
  kc_t4_drain_curve          <- v_kc_drain_curve (rows/s per min, H vs P)
  kc_t4_remaining_lag        <- v_kc_drain_curve (expected-cumulative, H vs P)
Row 4:
  kc_t4_insert_latency_sequence  (w6) <- v_kc_inserts_drill (duration_ms vs elapsed_s)
  kc_t4_batch_size_distribution  (w4) <- v_kc_inserts_drill (written_rows histogram)
  kc_t4_config_panel             (w2) <- v_kc_runs          (batch_size, versions, git_sha)
Row 5 (width 12):
  kc_t4_per_sample_lag_placeholder <- markdown (planned; pending perf.samples)
```

12 charts. `kc_t4_per_sample_lag_placeholder` is a **markdown** panel (dataset:
null): the plan's richer per-sample / per-partition poller lag curves are OUT of
scope (samples are runner artifacts, not in the DWH — plan §9), documented honestly
rather than shown as an empty chart.

## Tabs conversion (dashboard 432 — the coordinator executes; ADDITIVE-ONLY)

Dashboard 432 currently holds the Tab-1 content as a flat (non-tabbed) layout. To add
Tab 4 without touching Tab 1:

1. **Convert 432's layout to TABS.** In the dashboard `position_json`, wrap the
   existing top-level layout under a new `TABS` → `TAB` node. That first tab becomes
   the **default tab** and MUST contain the **existing, unmodified** Tab-1 chart
   nodes (same slice ids, same grid positions — move the subtree wholesale; do not
   re-create or re-position any existing chart).
2. **Add a second `TAB` node** titled "Tab 4 — RUN DRILL" and lay out the 12 Tab-4
   charts per the grid above. De-dupe chart ids before appending to the layout
   (build_superset.py bug #2 — a chart id must appear once in `position_json`).
3. **Add the native `pair_id` filter** scoped to the Tab-4 tab, default = latest pair.
4. Create the 3 NEW datasets first (`v_kc_run_drill`, `v_kc_drain_curve`,
   `v_kc_inserts_drill`) on `DWH` (`dc93cd97`); reuse the existing `v_kc_runs`
   dataset id (do NOT create a second `v_kc_runs`).

**Additive-only invariants (MUST hold):** no existing dataset SQL is edited; no
existing chart's `slice_name`/query/position changes; the Tab-1 subtree is moved into
the default tab byte-for-byte; only NEW nodes are appended. Avoid build_superset.py
bug #1 (never bind a metric alias equal to its column) in the new chart params.

## Acceptance checklist (Tab 4 — Stage B)

Prep (done, this package):

- [x] `v_kc_run_drill.sql`, `v_kc_drain_curve.sql`, `v_kc_inserts_drill.sql` created;
      each CREATEs + SELECTs against clickhouse-local (perf.* swap) with 0 rows and
      no error when empty (`verify_tab4.sh`).
- [x] `verify_tab4.sh` render-shape acceptance green: arm-view H/P shape + NULL-safe
      ratio; the pair-4 head-t1 `drain_rows_per_sec` ratio lands at **1.032**
      (delta_pct +3.2); legacy-name coalesce folds `ch_parts_per_insert`→
      `parts_per_insert`; drain bucketing math (rows/s=minute_rows/60, monotone
      cumulative, remaining_lag→0, 0-based dense minute_index); inserts grain = one
      row per insert with per-run seq; fixture/non-kafka/failed excluded.
- [x] `tab4_charts.json` parses; all 12 charts carry a `description` (#42 standard).
- [x] Existing `verify_verdict_dwh.sh` still **31/31** (contract-sync untouched).

Stage B (gated on the coordinator's GO):

- [ ] Create the 3 new datasets on `DWH` (`dc93cd97`); reuse `v_kc_runs` (no
      duplicate); do NOT create a DB connection.
- [ ] Convert 432 to TABS: existing Tab-1 content = default tab, untouched; Tab 4 =
      new tab. Confirm every existing Tab-1 chart still renders unchanged.
- [ ] Add the native `pair_id` filter (default = latest pair) scoped to Tab 4.
- [ ] Build the 12 Tab-4 charts (de-dupe chart ids before layout — bug #2), each with
      its `description`.
- [ ] Each chart renders with **pair-2 / pair-3 / pair-4** data selected in the
      filter (the 3 clean pairs); the arm table shows the head-t1 code advantage
      (drain_rows_per_sec ratio ≈ 1.032) on the relevant pair.
- [ ] Drilling the **quarantined pair 1** renders too (flagged shows in guards tile +
      arm table flag_reason; curves render from its partial ch_inserts) — a flagged
      pair is drillable by design.
- [ ] `kc_t4_per_sample_lag_placeholder` renders as **markdown** (not an empty chart).
- [ ] Existing Tab 1 datasets/charts and dashboard 424 (Spark) untouched; screenshot
      evidence of the two tabs.

## Files

| File | Purpose |
|---|---|
| `v_kc_runs.sql` | base-fact virtual dataset (shared: Tab 1 + Tab 4 tiles/config) |
| `v_kc_pair_ratios.sql` | DWH-adapted verdict view (verbatim verdict map + presentation cols) |
| `v_kc_flagged_log.sql` | flagged/failed run log |
| `v_kc_run_drill.sql` | **Tab 4** arm-comparison long view (all metrics H/P/delta, no verdict) |
| `v_kc_drain_curve.sql` | **Tab 4/2** per-minute drain shape + remaining lag from ch_inserts |
| `v_kc_inserts_drill.sql` | **Tab 4** raw per-insert grain (latency sequence + batch-size dist) |
| `tab1_charts.json` | 8 Tab-1 chart specs, each with its description |
| `tab4_charts.json` | **12 Tab-4** chart specs, each with its description |
| `visibility_precheck.sql` | Stage-B pre-flight queries (run first; abort on failure) |
| `verify_verdict_dwh.sh` | fixture acceptance for the adapted verdict view (31/31) |
| `verify_tab4.sh` | **Tab 4** render-shape acceptance (datasets parse + bucketing math) |
| `DASHBOARD.md` | this file |
