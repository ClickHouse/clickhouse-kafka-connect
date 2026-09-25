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

### VIZ-TYPE ALLOWLIST (standing rule — 2026-07-13)

Superset viz keys are **per-build**: a viz type registered in one deployment can be
**unregistered** in the DWH Superset build, in which case the chart throws a viz
**ERROR** on load instead of rendering (or showing an honest empty state). This bit
us on two batch-size charts and the coordinator fixed them live; the repo now
reproduces the fix. **Only use viz types known to be registered in the DWH build:**

- **Known-GOOD (registered + render-empty-graceful, safe to use freely):**
  `big_number_total`, `echarts_timeseries_line`, `echarts_timeseries`, `table`,
  `markdown`. A `table` or line renders an empty result as an empty table / "No
  data" — never a thrown viz error — so these are the default choice for a chart
  whose metric may be sparse or not-yet-emitted.
- **Known-UNREGISTERED (NEVER use — they throw a viz ERROR):** `histogram`,
  `dist_bar`. Any distribution/count-by-bucket need must be expressed as an
  **aggregate `table`** over a pre-binned dimension (see the batch-size charts:
  `written_rows_bucket` + `written_rows_bucket_sort` on `v_kc_inserts_drill`).
- **VERIFY-BEFORE-USE (registered but have thrown in some states — allowed for the
  existing charts that use them, but confirm on a fresh load before adding new
  ones):** `mixed_timeseries`, `echarts_timeseries_bar`, `echarts_timeseries_scatter`.

**Enforced:** `verify_tab23.sh` (guard #8) asserts every chart's `viz_type` in all
four deploy payloads is in this allowlist and proves the guard by planting a
`histogram` chart and asserting rejection. A future chart reaching for an
unregistered viz type FAILS the harness — this is the regression guard for the exact
bug above. Keep the allowlist in the guard and this section in sync.

### is_dttm on temporal columns (live-fix reconcile — 2026-07-13)

A time-series chart needs its x-axis column registered as a **temporal** column
(`is_dttm=true`) on the dataset, else the chart errors / cannot build the time grain.
The coordinator set `is_dttm` live on the 9 datasets' temporal columns; the deploy
payloads now carry a `columns[]` + `main_dttm_col` metadata block per temporal
dataset so a scripted rebuild reproduces it (not a repo-carryable live-only PUT
otherwise): `run_started_at` on the runs-derived views (`v_kc_runs`,
`v_kc_metric_trends`, `v_xconn`), `minute` on `v_kc_drain_curve`, `event_time` on
`v_kc_inserts_drill`. Non-temporal datasets (`v_kc_run_drill`, `v_kc_env_events`,
`v_xconn_efficiency`) are left as plain SQL — they back no temporal x-axis.

### FORMULA annotation layers (live-fix reconcile — 2026-07-13)

The two reference-line charts (`kc_t2_memory_vs_cap_trend` cap line;
`kc_t3_parse_watch_tier0` 50% threshold) use a FORMULA annotation layer. Each layer
must carry `showMarkers:false` + `hideLine:false` so the annotation renders as a
clean reference LINE (no point markers, line not suppressed) — the coordinator set
this live; the payloads now carry it.

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
  `pair_id` (max `pair_ts`, i.e. `pair_seq = 1`) at build time.
- **Latest-pair default is enforced in the QUERIES, not just the filter (fix #2).**
  Every Tab-4 chart carries an adhoc filter `pair_seq == 1` so a fresh load (nothing
  selected in the native filter) shows the latest pair, **never an all-pair
  aggregate**. This is the robust mechanism chosen over relying solely on a native
  filter default (see below). The native `pair_id` filter is the **interactive
  override** for drilling an older pair.

## Two live-deployment fixes (Task 34, dashboard 432 — redeploy)

The initial deploy of this tab on **432** (datasets 1541/1542/1543, charts 5780-5790)
surfaced two defects, corrected here for a clean redeploy:

### Fix #1 — batch-size distribution viz error (`kc_t4_batch_size_distribution`)

- **Symptom:** the chart threw a Superset viz error.
- **Root cause:** it used the **native `histogram` viz** (`all_columns_x=['written_rows']`,
  `link_length`, `groupby=['arm']`). Superset's native histogram is finicky and
  version-sensitive.
- **Fix (chosen: PRE-BIN + BAR chart, not the native histogram):** `v_kc_inserts_drill`
  now emits a deterministic `written_rows_bucket` (`0-10k` / `10-25k` / `25-50k` /
  `50-100k` / `100k+`, upper edge exclusive) plus `written_rows_bucket_sort` (the
  numeric lower edge, for size-ordered categories). The chart is now
  `echarts_timeseries_bar`: `x_axis = written_rows_bucket`, `metric = COUNT(*)`,
  `series = arm`, sorted by `written_rows_bucket_sort`. **Why:** pre-binning in the
  view is deterministic and version-proof — it renders identically on any Superset
  version, unlike the native histogram. The raw `written_rows` column is still
  surfaced (the latency sequence and ad-hoc analysis still read it); the bucket columns
  are purely **additive**, so Tab 2's existing head-arm histogram chart on the same
  view is unaffected (it simply ignores the new columns; the coordinator may migrate it
  to the same bar shape later, out of Task-34 scope).

### Fix #2 — tiles/arm-table aggregated across ALL pairs

- **Symptom:** the big-number tiles and the arm-comparison table showed data
  summed/mixed across all 4 pairs instead of the single drilled pair.
- **Root cause:** the chart specs' original `pair_id = (SELECT argMax(pair_id, …))`
  self-scoping **subqueries cannot be expressed as Superset adhoc filters**, so at
  deploy time they were dropped and every chart shipped with **no pair predicate**;
  with the native `pair_id` filter unset (no default wired), all pairs aggregated.
  Only `v_kc_run_drill` carried a `pair_seq` rank column — `v_kc_runs`,
  `v_kc_drain_curve` and `v_kc_inserts_drill` did **not**, so there was no plain
  column filter the deploy could use to self-scope.
- **Fix (chosen: SELF-SCOPING QUERIES via `pair_seq = 1`, not filter-default-only):**
  all three of those views were **ALTERed to add `pair_ts` + `pair_seq`** (`dense_rank`
  over `pair_ts DESC`, `1 = newest pair`) — the same NULL-safe expression
  `v_kc_pair_ratios` / `v_kc_run_drill` already use, so "latest pair" agrees tab-wide
  byte-for-byte. Every chart now carries an adhoc filter `pair_seq == 1`. **Why
  self-scoping over relying on the native filter default:** a fresh page load is
  correct (latest pair) **even if the native filter default is cleared or not wired**,
  which is exactly the failure that shipped. The native filter remains the interactive
  override. These are **additive columns** — existing Tab 1/2/3 consumers of `v_kc_runs`
  ignore them, so no existing chart changes.

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
  kc_t4_batch_size_distribution  (w4) <- v_kc_inserts_drill (written_rows_bucket BAR, per arm)
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
- [x] `verify_tab4.sh` render-shape acceptance green (**31/31**): arm-view H/P shape +
      NULL-safe ratio; the pair-4 head-t1 `drain_rows_per_sec` ratio lands at **1.032**
      (delta_pct +3.2); legacy-name coalesce folds `ch_parts_per_insert`→
      `parts_per_insert`; drain bucketing math (rows/s=minute_rows/60, monotone
      cumulative, remaining_lag→0, 0-based dense minute_index); inserts grain = one
      row per insert with per-run seq; fixture/non-kafka/failed excluded. **Fix-#1**:
      `written_rows_bucket` bins each edge correctly (5k→0-10k … 150k→100k+, upper
      edge exclusive at 100000) and `written_rows_bucket_sort` is monotone. **Fix-#2**:
      with two seeded pairs, a tile self-scoped to `pair_seq=1` returns the LATEST pair
      ALONE (pair4: 4 runs, 1 pair_id) on `v_kc_runs`, `v_kc_drain_curve` and
      `v_kc_inserts_drill` — not the 6-run all-pair aggregate.
- [x] `tab4_charts.json` parses; all 12 charts carry a `description` (#42 standard);
      the deploy payload `deploy/tab4_deploy.json` parses (fixes applied).
- [x] Other harnesses still green: `verify_tab23.sh` **17/17**, `verify_tab5.sh`
      **26/26**, `verify_verdict_dwh.sh` **31/31**; contract-sync PASSES (untouched).

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

## Coordinator RE-DEPLOY checklist (dashboard 432 — fixes #1 + #2, IN PLACE)

The tab is **already live** on 432 (datasets 1541/1542/1543, charts 5780-5790). This
is an **in-place fix**, NOT a re-create — do **not** recreate the tab, the datasets,
or the charts (their ids/positions stay). Apply, in order:

1. **ALTER the datasets (re-run the view SQL on `DWH`, connection `dc93cd97`) — do NOT
   drop/recreate the dataset objects, keep their Superset ids:**
   - `v_kc_inserts_drill` (id **1543**): re-run `v_kc_inserts_drill.sql` — adds
     `written_rows_bucket`, `written_rows_bucket_sort`, `pair_ts`, `pair_seq`. Then in
     Superset **edit the dataset → Sync columns from source** so the four new columns
     become selectable.
   - `v_kc_drain_curve` (id **1542**): re-run `v_kc_drain_curve.sql` — adds `pair_ts`,
     `pair_seq`. Sync columns.
   - `v_kc_run_drill` (id **1541**): re-run `v_kc_run_drill.sql` — unchanged shape
     (already had `pair_seq`); no re-sync needed unless the body drifted.
   - `v_kc_runs` (existing Tab-1 dataset, **reused**): re-run `v_kc_runs.sql` — adds
     `pair_ts`, `pair_seq` (additive; Tab 1/2/3 unaffected). Sync columns so the Tab-4
     tiles can filter on `pair_seq`. **Do not create a second `v_kc_runs`.**
2. **Edit charts IN PLACE (same slice ids 5780-5790):**
   - Every Tab-4 chart: add adhoc filter **`pair_seq = 1`** (fix #2). Tiles/tables in
     raw/aggregate mode, the two line charts, the scatter, and the bar all take it.
   - `kc_t4_batch_size_distribution` (fix #1): change **viz type `Histogram` →
     `Table` (aggregate)**. `histogram` AND `dist_bar`/`echarts_timeseries_bar` are all
     **unregistered** in this Superset build (VIZ-TYPE ALLOWLIST rule above) and throw
     a viz error, so the batch-size panel is an **aggregate table**: `Metric =
     COUNT(*)`, `Dimensions = arm, written_rows_bucket, written_rows_bucket_sort`,
     ordered by `written_rows_bucket_sort` ascending (so buckets read in size order;
     `written_rows_bucket_sort` hidden via column_config). Remove the old
     `all_columns_x` / `link_length` histogram config. (Supersedes the earlier
     "→ Bar Chart" instruction — the bar family also errors on this build.)
   - `kc_t4_tile_cost`: confirm it reads **`v_kc_run_drill`**
     (`sum(coalesce(head_value,0)+coalesce(pinned_value,0))`, `metric_name='run_cost_usd'`)
     — `run_cost_usd` is not a `v_kc_runs` column. (Already so in the live deploy;
     just add `pair_seq=1`.)
   - `kc_t4_arm_comparison`: columns stay `tier, metric_name, head_value, pinned_value,
     ratio, delta_pct, flagged, flag_reason` (raw mode; the `if(flagged,…)` computed
     column is not expressible — leave the real `flagged`+`flag_reason`).
3. **Native `pair_id` filter:** keep it (default = latest pair) as the interactive
   override. With `pair_seq=1` in the queries, a fresh load is already correct even if
   the default is cleared.
4. **Verify on 432:** fresh load shows the **latest pair only** on every tile + the arm
   table (not an all-pair aggregate); the batch-size chart renders as a per-arm bar
   over the five buckets with no viz error; selecting an older pair in the native
   filter drills that pair. The deploy payload `deploy/tab4_deploy.json` carries all of
   the above (patched viz + `pair_seq==1` filters + updated view SQL) for a scripted
   redeploy.

**Do NOT:** recreate the Tab-4 tab, delete/recreate datasets 1541/1542/1543 or charts
5780-5790, or touch Tab 1's charts (only `v_kc_runs`'s SQL is re-run, additively).

---

# Tab 2 — PERFORMANCE (issue #35)

Additive extension of this package with the Tab 2 "PERFORMANCE" build (dashboard
**432**, prod DWH Superset). Tab 2 is the **absolute history** view of the
**code-under-test (arm = head)**: how fast the current connector drains, what it
costs the server per row, and the full server-interaction + consume-side picture —
trended over the campaign. Strictly **additive** — no existing chart, dataset, or the
Tab-1/Tab-4 content is modified.

Tab 2 is **descriptive absolute history, NOT a gate**: it carries NO
verdict/band/alert column. Tab 1's `v_kc_pair_ratios` remains the single source of
verdict + alert truth, so Tab 2 cannot drift from the alerting logic.

## Arm scoping (plan §8 global-filter note)

The **whole tab is arm = head**. Every trend chart reads `v_kc_metric_trends`
filtered `arm='head'`; the tab carries a dashboard-level native filter pinning
`arm='head'` so no chart re-derives it. (Tab 3 is the arm=pinned twin.) The one
exception is the **cost/run** headline, which is per-PAIR by construction (§2.1
two-arm attribution charges the full pair cost to one arm), so it sums across the
pair and divides by pair count rather than arm-scoping — documented on the tile.

## Datasets (Tab 2 — additive; `DWH` connection `dc93cd97`, no DB connection created)

| Dataset SQL | New/Reused | Role | Feeds |
|---|---|---|---|
| `v_kc_metric_trends.sql` | **NEW** | tall per-run metric series (run × metric) with arm/tier/pair_ts/pair_seq + covariate scope columns; legacy ch_-names folded to pinned (§7) | every Tab-2 trend line (~12 charts) + all Tab-3 trends |
| `v_kc_runs.sql` | **REUSED** (Tab 1/4, unchanged) | one wide row per run | 3 headline big-number tiles (30d medians, cost/pair) |
| `v_kc_inserts_drill.sql` | **REUSED** (Tab 4, unchanged) | raw per-insert grain from ch_inserts | head-arm batch-size distribution |

Only **`v_kc_metric_trends`** is new for Tab 2. It is the tall (run × metric) shape
that lets ONE dataset feed ~12 trend charts and absorb new metric names with zero
DDL — see its header for why neither `v_kc_runs` (wide) nor `v_kc_run_drill` /
`v_kc_pair_ratios` (H-vs-P pair views) is the right grain for an absolute single-arm
history trend.

### Why a tall trends view (not one wide column per chart)

`v_kc_runs` is one wide row per run; a time-series over an arbitrary metric would need
a hard-coded column per chart and a base-view edit for every new metric (the
server-interaction family, consume-side JMX, gc_share, …). A tall `(run, metric)`
view scopes by `metric_name` and carries `unit`, so the same dataset renders every
trend and new metrics land as rows, not schema.

## Layout grid (Tab 2 — 12-wide, `tab2_charts.json`)

```
Row 1 (3 headline tiles, width 4 each):
  kc_t2_tile_med_drain_rps_30d      <- v_kc_runs (head t1, median 30d)
  kc_t2_tile_med_drain_seconds_30d  <- v_kc_runs (head t1, median 30d)
  kc_t2_tile_cost_per_run           <- v_kc_runs (per-PAIR: sum cost / #pairs)
Row 2 (TIER 0 · SINK PIPELINE, width 4 each):
  kc_t2_null_drain_trend            <- v_kc_metric_trends (null_drain_rows_per_sec)
  kc_t2_connect_cpu_gc_trend        <- v_kc_metric_trends (connect_cpu + gc_time_share)
  kc_t2_sink_overhead_decomposition_placeholder <- MARKDOWN (needs sink_overhead_share)
Row 3 (TIER 1 · THROUGHPUT & COST-PER-ROW, 2×6):
  kc_t2_verified_drain_trend        <- v_kc_metric_trends (drain_rows_per_sec, integrity marks)
  kc_t2_parts_merge_dual_axis       <- v_kc_metric_trends (parts_per_insert + merge_amplification)
Row 4 (2×6):
  kc_t2_drain_rate_stability_placeholder <- MARKDOWN (excluded; pre-fix values not comparable)
  kc_t2_partition_skew_trend        <- v_kc_metric_trends (partition_skew)
Row 5 (SERVER INTERACTION, width 4 each):
  kc_t2_insert_p50_p99_trend        <- v_kc_metric_trends (ch_insert_duration_p50/p99_ms)
  kc_t2_delayed_fraction_trend      <- v_kc_metric_trends (inserts_delayed_fraction)
  kc_t2_error_counts_241_252_trend  <- v_kc_metric_trends (ch_memory_limit / ch_too_many_parts_errors)
Row 6 (width 4 each):
  kc_t2_merge_pool_pct_trend        <- v_kc_metric_trends (merge_pool_peak_pct)
  kc_t2_memory_vs_cap_trend         <- v_kc_metric_trends (ch_peak_server_memory_bytes + cap line)
  kc_t2_batch_size_distribution_head <- v_kc_inserts_drill (COUNT by written_rows_bucket, aggregate TABLE, head)
Row 7 (CONSUME SIDE, 2×6):
  kc_t2_fetch_latency_consumed_rate_trend <- v_kc_metric_trends (fetch_latency_avg + records_consumed_rate)
  kc_t2_put_batch_time_trend        <- v_kc_metric_trends (put_batch_avg_time_ms)
```

18 chart entries = **16 charts + 2 honest markdown placeholders** (matching the plan's
"~16 charts" plus the two deferred slots below).

### The two Tab-2 placeholder slots (honest markdown, not empty charts)

1. **SINK OVERHEAD DECOMPOSITION** (`kc_t2_sink_overhead_decomposition_placeholder`)
   — the plan's centerpiece stacked-area (server insert time vs sink-pipeline time).
   Needs the `sink_overhead_share` metric =
   `(put_batch_time − ch_insert_duration) / put_batch_time`, a JMX × query_log join
   that is **not yet computed** (deferred since #29). Rendered as markdown until it
   lands (#35 constraint 4).
2. **`drain_rate_stability` trend** (`kc_t2_drain_rate_stability_placeholder`) —
   **excluded from charts** (principal ruling, #35 constraint 2). The bucketing fix
   (`d2aff99`) landed, but values **before 2026-07-13** are 10s-bucket and **not
   comparable** to the post-fix metric; trending them would draw a false step.
   Markdown placeholder until post-fix pairs accumulate.

### Watch-only, covariate, and sighted-gate notes (must show in descriptions)

- **`merge_amplification`** is **WATCH-ONLY** (contract §2/§3): charted + trended on
  the parts/merge dual-axis, but carries **no verdict and does not gate** — its
  description says *watch-only*, not gated. `parts_per_insert` IS the Tier-1 tripwire
  (verdict on Tab 1).
- **`connect_cpu_seconds_per_Mrows`** (and `ch_insert_cpu_share_tier0`) exist only
  from **pair 4 (2026-07-12)** onward — the sighted-gate Tier-0 build landed then
  (#35 constraint 5). Their trend lines **start 2026-07-12**; the descriptions say so.
- The server-interaction + consume-side charts are **diagnostic covariates**, not
  gates — each description says so and gives the direction of goodness.
- **Calibration caveat (n=3):** the 30d-median headline tiles are noisy at n=3 clean
  pairs; every tile description carries *provisional until n≈20* (#35 constraint 9).

### Empty-metric analysis — the error class was is_dttm/viz-type, NOT empty metrics (2026-07-13)

The ~3 Tab-2 viz ERRORs were the **is_dttm / unregistered-viz-type** class above, not
an empty-series class. A **line** chart on a metric with zero rows renders **"No
data"** gracefully — it does **not** throw — so line charts on not-yet-emitted metrics
are NOT converted to placeholders in this pass. Recording the metric-emission audit
here so it stays documented and a future "No data" panel is understood, not
re-diagnosed:

- **Server-side metrics are EMITTED (capture SQL, confirmed in code):**
  `ch_insert_duration_p50_ms`/`_p99_ms` (`sql/capture/11_insert_from_query_log.sql`),
  `ch_memory_limit_errors`/`ch_too_many_parts_errors` (16/11), `inserts_delayed_fraction`
  + `merge_pool_peak_pct` (16), `ch_peak_server_memory_bytes` (17),
  `parts_per_insert` + `merge_amplification` (13). So charts #8/#11/#12/#13/#14/#15
  have (or will have) rows — **keep as-is**.
- **Client JMX trio is AT-RISK (FOLLOW-UP WATCH):** `put_batch_avg_time_ms`,
  `fetch_latency_avg`, `records_consumed_rate` are computed by
  `poller/finalizer.py` (`compute_put_batch_avg_time_ms`/`_fetch_latency_avg`/
  `_records_consumed_rate`) but return **`None` when the consumer/sink JMX prometheus
  endpoint was not scraped**, and the inserter **skips `None`** — so they have rows
  ONLY if JMX was wired during the pair. Same for `connect_cpu_seconds_per_Mrows` /
  `gc_time_share` (kubelet/JMX; connect_cpu additionally has a 2026-07-12 series
  start). Charts #5, #17, #18 are the ones to watch. **Action if the coordinator's
  live `SELECT DISTINCT metric_name` confirms ZERO rows** for any of these across all
  pairs: convert that chart to an honest markdown "awaiting metric: `<name>` (emitted
  once JMX/kubelet scraping lands)" placeholder — the #42 standard, same style as the
  `sink_overhead_share` and `drain_rate_stability` placeholders. Until then they stay
  as line charts (graceful "No data"), not placeholders. This is a **follow-up watch,
  not a fix in this commit.**

## Acceptance checklist (Tab 2 — Stage B)

Prep (done, this package):

- [x] `v_kc_metric_trends.sql` created; CREATEs + SELECTs against clickhouse-local
      (perf.* swap), 0 rows + no error when empty (`verify_tab23.sh`).
- [x] `verify_tab23.sh` green: trends returns the pair-4 `connect_cpu_seconds_per_Mrows`
      value on the **right arm/tier (head/t0)**; legacy `ch_parts_per_insert` folds to
      `parts_per_insert` (no ch_-prefixed leak); conformant capture-family names pass
      through; both arms exposed; fixture/failed/non-kafka excluded.
- [x] `tab2_charts.json` parses; all 18 entries carry a `description` (#42 standard);
      the 2 placeholder slots render as markdown.
- [x] Existing `verify_verdict_dwh.sh` (31/31) and `verify_tab4.sh` (17/17) still green.

Stage B (gated on the coordinator's GO):

- [ ] Create `v_kc_metric_trends` on `DWH` (`dc93cd97`); reuse `v_kc_runs` +
      `v_kc_inserts_drill` (no duplicates); do NOT create a DB connection.
- [ ] Add Tab 2 as a new `TAB` node on 432 (existing tabs untouched); pin `arm='head'`
      via a Tab-2-scoped native filter.
- [ ] Build the 16 charts + 2 markdown placeholders (de-dupe chart ids before layout —
      bug #2; never bind a metric alias == column — bug #1), each with its `description`.
- [ ] Verify every trend renders with the 3 clean pairs (n=3); the two placeholders
      render as **markdown**, not empty charts.
- [ ] Confirm merge_amplification reads as **watch-only** in its description, and the
      connect_cpu / parse-watch series **start 2026-07-12** (no back-fill, no zeros).
- [ ] Existing Tab 1/Tab 4 datasets/charts and dashboard 424 (Spark) untouched;
      screenshot evidence.

---

# Tab 3 — ENVIRONMENT (issue #35)

Additive extension with the Tab 3 "ENVIRONMENT" build (dashboard **432**). Tab 3 is
the **instrument-health** view of the **stable reference (arm = pinned)**: is the
benchmark's own measuring apparatus steady, and what environment events (CH upgrades,
restarts, memory pressure) moved it? Strictly **additive**; carries **no
verdict/alert** truth.

## Arm scoping

The **whole tab is arm = pinned**. The pinned arm is the fixed reference build, so
its trends should be flat — any movement is the ENVIRONMENT, not the code. Every
trend reads `v_kc_metric_trends` filtered `arm='pinned'`; `v_kc_env_events` is
pinned-only by construction. **Head rows never appear on Tab 3** (asserted in
`verify_tab23.sh`).

## Datasets (Tab 3 — additive)

| Dataset SQL | New/Reused | Role | Feeds |
|---|---|---|---|
| `v_kc_metric_trends.sql` | **REUSED** (from Tab 2) | tall per-run metric series | pinned drain-rate, pre-run RSS + active parts, parse-watch, CoV inputs |
| `v_kc_env_events.sql` | **NEW** | kafka-local environment-annotation source derived inline from consecutive PINNED runs, carrying the §4 scope tuple | annotation overlay on the pinned drain trend + CH version timeline |
| `v_kc_runs.sql` | **REUSED** | one wide row per run | throughput-vs-uptime scatter + per-tier CoV noise gauge |

Only **`v_kc_env_events`** is new for Tab 3.

## Annotation layer — `v_env_annotations` does not exist yet (derived inline)

The plan §8 sketches a **shared** `v_env_annotations` across Spark and Kafka. **That
shared view does not exist yet for Kafka** (#35 constraint 6). Per **contract §4** an
**unscoped** shared annotation view is **PROHIBITED** — it would paint a Spark-target
restart (production, us-east-2) onto Kafka charts (staging) and vice versa,
manufacturing false correlations. So Tab-3's annotation layer is derived **inline**
from `v_kc_env_events`:

- **CH upgrade** — `clickhouse_version` changed vs the previous pinned run.
- **Server restart** — `ch_uptime` dropped vs the previous pinned run (contract §2.1:
  a drop ⇒ the service restarted between runs).

Every emitted row carries the **contract §4 scope tuple**
`(connector, target_service, environment_class)` as columns. `connector` =
`'kafka-connect'`; `environment_class` = `runtime['environment_class']`;
`target_service` = `runtime['target_service']` **if present**, else a stable
`environment_class/target_region` composite (e.g. `staging/us-east-2`) — because the
runtime map does **not** yet carry a dedicated `target_service` key (the mandatory
identity keys today are `target_region` + `environment_class`, §1.1). The composite
uniquely names this benchmark's target among the connectors' targets; when a
dedicated key is added upstream the coalesce picks it up with no edit.

**Consumers MUST filter the scope tuple** (contract §4): the annotation overlay on
`kc_t3_pinned_drain_trend_annotated` filters
`connector='kafka-connect' AND environment_class='staging' AND target_service=<this
target>` so a Spark-target event is never rendered here.

> **Future work:** a **shared cross-connector annotation view** that UNIONs both
> connectors' *already-scoped* events is explicitly deferred. When it lands,
> `v_kc_env_events` becomes its kafka-connect branch unchanged (it is already scoped).

## Layout grid (Tab 3 — 12-wide, `tab3_charts.json`)

```
Row 1 (2×6):
  kc_t3_pinned_drain_trend_annotated <- v_kc_metric_trends (drain_rows_per_sec, pinned)
                                        + annotation overlay from v_kc_env_events (scoped)
  kc_t3_ch_version_timeline          <- v_kc_metric_trends (clickhouse_version over time)
Row 2 (2×6):
  kc_t3_throughput_vs_uptime_scatter <- v_kc_runs (drain_rows_per_sec vs ch_uptime, pinned)
  kc_t3_pre_run_state_trends         <- v_kc_metric_trends (pre_run_rss + pre_run_active_parts)
Row 3 (2×6):
  kc_t3_cov_noise_gauge_per_tier     <- v_kc_runs (CoV of rate per tier, pinned)
  kc_t3_parse_watch_tier0            <- v_kc_metric_trends (ch_insert_cpu_share_tier0 + 50% line)
```

6 charts (the plan's Tab-3 count exactly).

### Tier-0 parse-watch threshold (decision-9 revisit)

`kc_t3_parse_watch_tier0` carries a **threshold line at 50%** (#35 constraint 8 — the
decision-9 revisit level). `ch_insert_cpu_share_tier0` is the share of Tier-0
wall-clock the Null target spends in server-side insert CPU; observed values are
**1.6–2.75%**, leaving huge headroom. The **50% line is the documented revisit
trigger**: if the line trends toward and crosses it, the Null target is materially
parse-bound and decision 9 (whether Tier 0 is a fair connector ceiling on the 3 vCPU
box) MUST be revisited. The metric exists only from **pair 4 (2026-07-12)** onward
(sighted-gate — constraint 5), so the line **starts 2026-07-12**.

### CoV noise-gauge invariant

`kc_t3_cov_noise_gauge_per_tier` enforces the instrument-sanity rule: **Tier 0 CoV
must be ≪ Tier 1 CoV**. Tier 0 (Null target) removes server/merge variance, so if
Tier 0 is as noisy as Tier 1 the *instrument itself* is unstable and no band can be
trusted. The gauge shows `n_runs` so its **provisional at n=3** status (constraint 9)
is visible at a glance.

## Acceptance checklist (Tab 3 — Stage B)

Prep (done, this package):

- [x] `v_kc_env_events.sql` created; CREATEs + SELECTs against clickhouse-local, 0
      rows + no error when <2 pinned runs (`verify_tab23.sh`).
- [x] `verify_tab23.sh` green: env-events emits a `ch_version_change` row when
      synthetic pinned versions differ (25.1→25.2) and a `server_restart` when
      `ch_uptime` drops, each with the §4 scope tuple
      (`kafka-connect|staging/us-east-2|staging`); the first pinned run (no
      predecessor) emits nothing; **no head-derived rows** (pinned scoping);
      fixture/failed/non-kafka excluded.
- [x] `tab3_charts.json` parses; all 6 charts carry a `description` (#42 standard),
      including the 50% parse-watch threshold and the CoV invariant.

Stage B (gated on the coordinator's GO):

- [ ] Create `v_kc_env_events` on `DWH` (`dc93cd97`); reuse `v_kc_metric_trends`
      (created in the Tab-2 step) + `v_kc_runs`; no DB connection.
- [ ] Add Tab 3 as a new `TAB` node on 432 (existing tabs untouched); pin
      `arm='pinned'` via a Tab-3-scoped native filter.
- [ ] Build the 6 charts (bug #1 / #2 avoidance), each with its `description`; wire the
      annotation overlay filtered to the §4 scope tuple.
- [ ] Verify trends render with the 3 clean pairs (pinned arm); the parse-watch shows
      the 50% threshold line and the ~2% observed series (starting 2026-07-12); the
      CoV gauge shows `n_runs` and reads as provisional.
- [ ] Confirm the annotation overlay is **scope-filtered** (no Spark-target events on
      Kafka charts); document the shared-annotation-view future work.
- [ ] Existing tabs and dashboard 424 (Spark) untouched; screenshot evidence.

---

# Tab 5 — CROSS-CONNECTOR (issue #36)

Additive extension with the Tab 5 "CROSS-CONNECTOR" build (dashboard **432**, prod
DWH Superset). Tab 5 is the **payoff tab**: Spark vs Kafka Connect on the *shared*
perf schema — same metric definitions, same integrity rules, same server-cost
accounting — the argument for having kept one spec, dataset, and schema across both
benchmarks (plan §8). Strictly **additive** — no existing chart, dataset, or the
earlier tabs are modified.

Tab 5 is **descriptive, not a gate**: it carries NO verdict/band/alert column. Each
connector's own Tab 1 (`v_kc_pair_ratios` here; the Spark dashboard's `v_pair_ratios`
on 424) remains the single source of verdict + alert truth, so this tab cannot drift
from the alerting logic. Tab 5 lives **only** on the Kafka dashboard (contract §6);
the Spark dashboard does not duplicate it.

## Two structural caveats (both surfaced permanently on the tab)

**1. Same spec, NOT same instance (environment_class differs).** The connectors run
against dedicated targets: **Spark = `production`**, **Kafka Connect = `staging`**
(contract §6, §1.4). Each connector's own H/P *ratio* gate is unaffected (both arms
share one target), but the **absolute** cross-connector numbers on this tab straddle
a production/staging boundary. Every chart surfaces `environment_class` beside
`clickhouse_version`; a permanent markdown **banner** (`kc_t5_banner_caveat`) states
the caveat. Banner text is in `tab5_charts.json` (the `markdown` field) — reproduced
in the acceptance checklist below.

**2. Matched-dataset only — HONESTLY EMPTY today (ACCEPTED).** A cross-connector
comparison is valid **only** on a **matched dataset**, where matched = equal
`(dataset, rows_expected)` (contract §5/§6, Amendment 2026-07-09d). `dataset` alone
cannot discriminate volume (`'hits'`@10M vs `'hits'`@100M both read `'hits'`), so the
comparison bucket is the pair `(dataset, rows_expected)`; a bucket is **matched iff it
holds ≥2 distinct connectors**. Today **Kafka runs `hits`@10,000,000 and Spark runs
`hits`@~99,997,497**, so **no bucket is matched** and the comparison charts +
efficiency table render **empty** — this is **correct and ACCEPTED** (structure first).
A Kafka **100M graduation** is a deliberate future instrument event; when it lands the
matched side populates **automatically** (the rule self-maintains, no dashboard edit —
`rows_expected` un-matches a volume change on its own). Meanwhile the empty-state note
+ the **UNMATCHED CONTEXT** table keep the tab honest and useful.

## Matched-join SQL shape (the mechanism)

`v_xconn` carries every in-scope run's `dataset` + its `rows_expected` metric, keys a
comparison bucket, and counts distinct connectors per bucket with a window:

```sql
concat(dataset, '@', ifNull(toString(toUInt64(rows_expected)), 'unknown')) AS comp_bucket,
uniqExact(connector) OVER (PARTITION BY dataset, rows_expected)             AS bucket_connectors,
(isNotNull(rows_expected)
 AND uniqExact(connector) OVER (PARTITION BY dataset, rows_expected) >= 2)   AS matched_dataset
```

Rows missing `rows_expected` are `matched_dataset = 0` (safe-default UNMATCHED, §6).
The comparison charts + `v_xconn_efficiency` default-filter `matched_dataset = 1`; the
context table pins `matched_dataset = 0`. A mismatched row **never** reaches a shared
comparison series (§6) — it appears only in the clearly-labelled context table.

## Grounded connector scope (discovered, not guessed)

- **Spark = connector VALUE `'spark'`** — grounded in the deployed Spark dashboard SQL
  (`spark-clickhouse-connector/benchmarks/dashboard/v2/v_runs_enriched.sql:293`
  `WHERE r.connector = 'spark'`; write-side default
  `benchmarks/scripts/insert_run_record.py:70` and schema `DEFAULT 'spark'` in
  `benchmarks/sql/perf/02_create_runs.sql:19`). NOT `'clickhouse-spark'` (that string
  is only a Grafana title / superset tag).
- **Kafka = connector VALUE `'kafka-connect'`** (this repo's own scope value).
- Fixtures excluded for **both spellings**: Kafka `'__verdict_fixture__'` and Spark
  `'verdict_fixture'` (grounded: spark `v_verdict_fixture_check.sql:60`).
- **§2.2 aliasing** (view-only, never a stored rename): Spark `throughput_rows_per_sec`
  + Kafka `drain_rows_per_sec` (and the Tier-0 `null_*` analogues) fold to one
  `rows_per_sec` series. Server-cost headline is the PINNED
  `ch_insert_cpu_seconds_per_Mrows` (NOT the plan sketch's `server_cpu_per_Mrows`).

## Deployed flagged predicate (deployment deviation — REPLICATED)

`v_xconn` tests the validity flag as `runtime['flagged'] IN ('1','true')` to match the
deployed Tab-1/Tab-2 datasets (the harness has emitted both spellings). `flagged` is
carried as a **presentation** column (an integrity mark): a flagged run is
**shown-and-marked**, not dropped — Tab 5 is descriptive history, verdict/exclusion
truth stays on each connector's Tab 1. Failed-outcome runs ARE excluded by value.

## Datasets (Tab 5 — additive; `DWH` connection `dc93cd97`, no DB connection created)

| Dataset | Grain / purpose |
|---|---|
| `v_xconn` | tall per-run cross-connector view (per run × canonical metric): `value`, `metric_name`, `connector`/`connector_label`, `arm`/`tier`, `dataset`, `rows_expected`, `comp_bucket`, `bucket_connectors`, `matched_dataset`, `environment_class`, `clickhouse_version`, `target_region`, `compute_region`, `flagged`, `flag_reason`, `pair_ts`, `pair_seq`. BOTH connectors, arm=head, tier=1; §2.2 headlines aliased to `rows_per_sec`. This is the ONLY connector-unscoped view in the package. |
| `v_xconn_efficiency` | latest-30d MEDIAN per `(metric, connector)` + ratio, built ON `v_xconn`, matched-only: `metric_name`, `unit`, `spark_median`, `kafka_median`, `ratio_spark_over_kafka`, `spark`/`kafka_environment_class`, `spark`/`kafka_clickhouse_version`, `spark_n`, `kafka_n`. Feeds the efficiency table; EMPTY today (no matched bucket). |

## Layout grid (Tab 5 — 12-wide, `tab5_charts.json`)

```
Row 0 (width 12):
  kc_t5_banner_caveat            <- markdown (PERMANENT caveat: env_class + matched-only/empty)
Row 1 (2 x width 6):
  kc_t5_rows_per_sec_trend       <- v_xconn (rows_per_sec, 2 series by connector, matched)
  kc_t5_server_cpu_trend         <- v_xconn (ch_insert_cpu_seconds_per_Mrows, 2 series, matched)
Row 2 (2 x width 6):
  kc_t5_parts_per_insert_trend   <- v_xconn (parts_per_insert, 2 series, matched)
  kc_t5_merge_amplification_trend<- v_xconn (merge_amplification watch-only, 2 series, matched)
Row 3 (width 12):
  kc_t5_efficiency_table         <- v_xconn_efficiency (metric|Spark|Kafka|ratio, 30d medians)
Row 4 (width 12):
  kc_t5_unmatched_context        <- v_xconn (matched=0: each connector's numbers, NON-COMPARABLE)
```

7 nodes (1 markdown banner + 4 two-series trends + efficiency table + unmatched
context table). The 4 trend charts key their two series by `connector_label` ('Spark',
'Kafka Connect'). All comparison surfaces default-filter `matched_dataset = 1` and are
honestly empty today; the banner + context table document why.

## Tabs conversion (dashboard 432 — the coordinator executes; ADDITIVE-ONLY)

Assumes 432 is already TABS after Tabs 4/2/3. To add Tab 5:

1. **Create the 2 NEW datasets first** (`v_xconn`, then `v_xconn_efficiency` — the
   efficiency view reads `v_xconn`, so order matters) on `DWH` (`dc93cd97`). Do NOT
   create a DB connection. No existing dataset is reused or edited.
2. **Add a `TAB` node** titled "Tab 5 — CROSS-CONNECTOR" and lay out the 7 nodes per
   the grid. De-dupe chart ids before appending (build_superset.py bug #2 — a chart id
   must appear once in `position_json`).
3. **Set the comparison charts' default** to `matched_dataset = 1` (in each chart's
   `filters`); the context table pins `matched_dataset = 0`.

**Additive-only invariants (MUST hold):** no existing dataset SQL is edited; no
existing chart's `slice_name`/query/position changes; only NEW nodes are appended.
Avoid build_superset.py bug #1 (never bind a metric alias equal to its column) in the
new chart params.

## Acceptance checklist (Tab 5 — Stage B)

Prep (done, this package):

- [x] `v_xconn.sql` + `v_xconn_efficiency.sql` created; each CREATEs + SELECTs against
      clickhouse-local (perf.* swap) with 0 rows and no error when empty
      (`verify_tab5.sh`). Connector scope grounded (`'spark'` / `'kafka-connect'`).
- [x] `verify_tab5.sh` render-shape + rule acceptance green (all assertions): §2.2
      aliasing folds the four headline spellings to one `rows_per_sec` series (no
      stored name leaks); §7 `ch_parts_per_insert`→`parts_per_insert` fold; the pinned
      `ch_insert_cpu_seconds_per_Mrows` present and `server_cpu_per_Mrows` never
      appears; **matched_dataset=1 only when two connectors share equal
      `(dataset, rows_expected)`**; a genuinely matched bucket DOES produce a
      comparable ratio (proves the mechanism, not just the empty state); mismatched
      volumes (10M vs 100M) excluded from the efficiency medians by default;
      `environment_class` + `clickhouse_version` present (both-sided on the efficiency
      table); fixtures (both spellings)/failed/off-scope excluded, flagged carried +
      marked; and the **honest-empty** reproduction (10M vs 100M ⇒ zero matched rows,
      empty efficiency table, but the unmatched context still shows both connectors).
- [x] `tab5_charts.json` parses; all charts carry a `description` (#42 standard) with
      direction-of-goodness; the permanent caveat banner is present.
- [x] Existing `verify_tab23.sh` (17/17), `verify_tab4.sh` (17/17), and
      `verify_verdict_dwh.sh` (31/31) still green (contract-sync untouched).

Stage B (gated on the coordinator's GO):

- [ ] Create the 2 new datasets on `DWH` (`dc93cd97`) in order (`v_xconn` then
      `v_xconn_efficiency`); do NOT create a DB connection.
- [ ] Add the Tab 5 tab; confirm every existing tab still renders unchanged.
- [ ] Build the 7 nodes (de-dupe chart ids — bug #2), each with its `description`.
- [ ] The **caveat banner** renders as markdown with BOTH caveats (env_class prod/
      staging + matched-only/empty-today).
- [ ] **ACCEPTED HONESTLY EMPTY:** the 4 comparison trends + the efficiency table
      render **empty** today (no matched `(dataset, rows_expected)` bucket) WITHOUT
      error — a graceful empty state, not a broken chart; the empty-state note is
      visible.
- [ ] The **UNMATCHED CONTEXT** table renders NON-EMPTY (each connector's headline
      numbers on its own volume), clearly labelled non-comparable, carrying
      `rows_expected` + `environment_class` + `clickhouse_version`.
- [ ] Existing tabs and dashboard 424 (Spark) untouched; screenshot evidence.

> **Coordinator note (concurrence-sensitive):** the matched-dataset mechanism is the
> **kafka-manager recommendation** and is **pending spark-manager concurrence**. It
> adds no new keys and mutates no rows (it joins the existing `dataset` runtime key
> with the existing `rows_expected` metric), so it is safe to ship the structure now;
> if spark-manager prefers a different matched-key definition, only `v_xconn`'s
> `matched_dataset` expression changes (the charts/efficiency view are unaffected).

---

## Files

| File | Purpose |
|---|---|
| `v_kc_runs.sql` | base-fact virtual dataset (shared: Tab 1 + Tab 4 tiles/config) |
| `v_kc_pair_ratios.sql` | DWH-adapted verdict view (verbatim verdict map + presentation cols) |
| `v_kc_flagged_log.sql` | flagged/failed run log |
| `v_kc_run_drill.sql` | **Tab 4** arm-comparison long view (all metrics H/P/delta, no verdict) |
| `v_kc_drain_curve.sql` | **Tab 4/2** per-minute drain shape + remaining lag from ch_inserts |
| `v_kc_inserts_drill.sql` | **Tab 4/2** raw per-insert grain (latency sequence + batch-size dist) |
| `v_kc_metric_trends.sql` | **Tab 2/3** tall per-run metric series (run × metric, arm/tier scoped) |
| `v_kc_env_events.sql` | **Tab 3** kafka-local environment annotations (§4-scoped, derived inline) |
| `v_xconn.sql` | **Tab 5** cross-connector tall view (BOTH connectors, arm=head/tier=1, §2.2 aliased, matched-dataset rule) |
| `v_xconn_efficiency.sql` | **Tab 5** 30d-median ratio table built ON `v_xconn` (matched-only; metric\|Spark\|Kafka\|ratio) |
| `tab1_charts.json` | 8 Tab-1 chart specs, each with its description |
| `tab4_charts.json` | **12 Tab-4** chart specs, each with its description |
| `tab2_charts.json` | **16 Tab-2** charts + 2 markdown placeholders, each with its description |
| `tab3_charts.json` | **6 Tab-3** chart specs, each with its description |
| `tab5_charts.json` | **7 Tab-5** nodes (caveat banner + 4 two-series trends + efficiency table + unmatched context), each with its description |
| `visibility_precheck.sql` | Stage-B pre-flight queries (run first; abort on failure) |
| `verify_verdict_dwh.sh` | fixture acceptance for the adapted verdict view (31/31) |
| `verify_tab4.sh` | **Tab 4** render-shape acceptance (datasets parse + bucketing math) |
| `verify_tab23.sh` | **Tab 2/3** render-shape acceptance (trends + env-events shape) |
| `verify_tab5.sh` | **Tab 5** cross-connector render-shape + matched-rule acceptance (aliasing/fold/matched/empty-state) |
| `DASHBOARD.md` | this file |
