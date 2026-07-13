# Chaos / "monkey" exactly-once test — design

**Task 43, Kafka Benchmark v2.** Docs-first: this document is the reviewable
artifact that gates the harness implementation (Task follow-on). No harness code
lands in this task.

**Audience**: the harness author, the manager, and the principal reviewer.
Everything here is grounded in the infrastructure handbook
(`benchmarks/e2e/ECOSYSTEM.md`), the plan/contract, and the sink source read
during design (paths cited inline).

---

## 1. Goal & non-goals

**Goal.** Prove the connector delivers **zero loss and zero duplication** into
ClickHouse when faults are injected mid-drain, across **both delivery modes**:

- **at-least-once + block dedup** (`exactlyOnce="false"`) — the benchmark's
  production mode; five pairs of zero-duplicate evidence stand behind it.
- **exactly-once + KeeperMap state** (`exactlyOnce="true"`) — the guarantee the
  connector advertises; state store lives ON the target ClickHouse.

The oracle is the **integrity check** already used by the benchmark
(`capture/check_integrity.py` + SQL 20), applied after the connector has fully
recovered and drained to lag 0.

**Non-goals.**

- **This is not a performance benchmark.** No throughput bands, no ratios, no
  cost/throughput headline numbers, no verdict-map application. Recovery timing
  is captured as **diagnostic** signal only (§6), never gated.
- Chaos runs **must not pollute benchmark trends.** They carry
  `connector='kafka-connect-chaos'` (every dashboard dataset filters by
  `connector`, per ECOSYSTEM §8), a distinct topic/table/group (§4), and by
  default are **not exported** to the DWH at all. If ever exported for
  archival, the distinct `connector` value keeps them out of every pair trend.
- Not a proof of ClickHouse Cloud's own durability — we test the **connector's**
  behaviour when its dependencies misbehave.

---

## 2. How the sink actually behaves under a crash (source-grounded)

This section is the technical spine; the fault matrix (§3) and oracle (§5) hang
off it. All line references are to the checked-out `benchmark-v2` tree.

### 2.1 The two delivery modes select different state providers

`ProxySinkTask` (`src/main/java/com/clickhouse/kafka/connect/sink/ProxySinkTask.java:45-49`):

```
if (clickHouseSinkConfig.isExactlyOnce())
    this.stateProvider = new KeeperStateProvider(clickHouseSinkConfig);
else
    this.stateProvider = new InMemoryState();
```

- **`exactlyOnce=false`** → `InMemoryState`. No persisted offset ranges; on a
  crash the state is lost and Connect re-delivers from the last **committed
  Kafka offset**. Correctness rests entirely on ClickHouse **block
  deduplication** keyed by the connector's deduplication token (§2.3).
- **`exactlyOnce=true`** → `KeeperStateProvider`. Per topic-partition offset
  ranges + a BEFORE/AFTER state marker are persisted in a **KeeperMap** table
  ON the target CH (§2.4). Recovery reads that state and reconciles the
  re-delivered batch against it (§2.2). Block dedup is **also** active as a
  second line of defence.

### 2.2 The crash windows — the offset state machine

`Processing.doLogic()`
(`src/main/java/com/clickhouse/kafka/connect/sink/processing/Processing.java:131-266`)
runs the same three-step sequence for every batch:

```
setStateRecord(BEFORE_PROCESSING, [minOffset, maxOffset])   // window A opens
doInsert(records)                                            // window B: the insert itself
setStateRecord(AFTER_PROCESSING, [minOffset, maxOffset])    // window A closes
```

(Processing.java:169-171 is the canonical NONE-state path; the same
before/insert/after triple recurs for every overlap branch — ZERO reset 182-184,
SAME/NEW 188-189, OVER_LAPPING 202-215, and the AFTER_PROCESSING re-entry cases
234-250.)

The **three crash windows the harness must target** are exactly the gaps in
that sequence:

| # | Crash window | On recovery, stored state is… | Reconciliation branch |
|---|---|---|---|
| **W1** | after `BEFORE_PROCESSING` set, **before** insert starts | `BEFORE_PROCESSING` for [min,max]; **no rows in CH yet** | Re-delivered batch hits `case BEFORE_PROCESSING`; the branch taken depends on the re-polled boundaries (see below — `OVER_LAPPING` is the common shape). Rows land once. |
| **W2** | **mid-insert** (insert in flight when the pod/network dies) | `BEFORE_PROCESSING` for [min,max]; CH may hold a **partial or whole** copy of the block | `BEFORE_PROCESSING` branch re-inserts; **block dedup** (§2.3) absorbs whatever already landed *when the re-inserted chunk reproduces the stored boundaries*. This is the window that most needs the dedup token. |
| **W3** | after insert succeeds, **before** `AFTER_PROCESSING` set | `BEFORE_PROCESSING` for [min,max]; CH holds the **full** block | Re-insert: `SAME` boundaries → block dedup drops the whole block (0 net rows); `OVER_LAPPING` → left chunk re-inserted under the stored boundaries (dedup-absorbed), right chunk inserted fresh. State advances to `AFTER_PROCESSING`. |

**Which `BEFORE_PROCESSING` branch fires on recovery — `OVER_LAPPING` is the
common one, not `SAME`.** `RangeContainer.getOverLappingState`
(RangeContainer.java:57-68) returns `SAME` only when the re-polled batch's max
offset **exactly equals** the stored max (`actualMax == storedMax && actualMin
<= storedMin`). A post-crash re-poll starts from the last committed offset with
whatever `max.poll.records` yields, so its max typically **exceeds** the stored
max → `OVER_LAPPING` (RangeContainer.java:67): the batch is split at the stored
max (Processing.java:195-216); the left chunk is re-inserted only if its
boundaries reproduce the stored range exactly (then dedup-absorbed), **otherwise
it goes to the DLQ** (Processing.java:205-210 — a path the harness must watch:
DLQ'd records land nowhere, and the oracle sees that as loss); the right chunk
is inserted fresh under a new state record. Two further recovery shapes are
**transient, self-healing task errors** — not failures:

- **`CONTAINS` throws** (Processing.java:191-194): a re-polled batch **smaller**
  than the stored range raises a RuntimeException → task error → the framework
  retries → a later, larger poll exits the CONTAINS shape and the pipeline
  self-heals.
- The `ERROR` overlap default (Processing.java:217-219) likewise throws and
  retries.

The **recovery watcher** (§8.3) must therefore tolerate transient task
FAILED→RUNNING cycles during legitimate W1–W3 recovery (recorded in
`task_restart_count`, §6, not treated as cell failure), while still alerting on
the OVER_LAPPING left-boundary-mismatch DLQ path.

A fourth, coarser window — crash **after** `AFTER_PROCESSING` but before the
Kafka offset commit — lands the re-delivery in `case AFTER_PROCESSING`
(Processing.java:222-264): `SAME`/`CONTAINS` → `onStateUpdate` (no insert);
`PREVIOUS` → skip (or throw unless `tolerateStateMismatch=true`). This is the
"clean" window and is the common case under a Connect **rebalance**.

**Design consequence.** In exactly-once mode, W1–W3 all converge on the
`BEFORE_PROCESSING` branch, which re-inserts and leans on block dedup. So the
KeeperMap state does **not** replace block dedup — it *bounds re-delivery to the
recorded range and prevents double-advance*, while **block dedup still does the
actual duplicate suppression** at the row/block level. The matrix must therefore
prove **both** mechanisms, and prove exactly-once does not regress vs
at-least-once.

### 2.3 The deduplication token (both modes)

`QueryIdentifier.getDeduplicationToken()`
(`src/main/java/com/clickhouse/kafka/connect/util/QueryIdentifier.java:56-61`):

```
return String.format("%s-%s-%s-%s", topic, partition, minOffset, maxOffset);
```

It is applied per insert as `insert_deduplication_token`
(`ClickHouseWriter.java:1003-1005, 1204-1206, 1363-1365, 1473-1475`). Because
the token is a **deterministic function of (topic, partition, offset range)**, a
re-delivered batch with the **same boundaries** reproduces the same token, and
ClickHouse drops the duplicate block.

**Scope of that protection — boundary drift.** Token determinism only covers
re-deliveries that **reproduce the original batch boundaries** (framework
retries of the same put, and exactly-once recovery re-inserts bounded by the
stored range). In plain at-least-once, a **post-restart re-delivery is not
guaranteed to reproduce boundaries**: the consumer re-polls from the last
committed offset and poll composition can drift, so a batch whose insert
*completed* but whose offsets were never committed can be re-delivered under
**different boundaries → a new token → the rows double-land**. This is inherent
to at-least-once (there is no state store to trim against). The at-least-once
crash cells (F1/F8, §3.4) therefore carry an explicit
**boundary-drift caveat**: they are *expected*-PASS, and a duplicate observed
there is **signal about the delivery mode, not a harness error** — recorded as
a MISMATCH per the oracle, with `fault_window`/`redelivered_offsets` attached
for diagnosis.

**Critical edge the matrix must cover.** When `ignorePartitionsWhenBatching=true`
(at-least-once only), records are batched across partitions and the
`QueryIdentifier` is constructed **without** partition/offsets → `partition==-1`
→ `getDeduplicationToken()` returns **`null`** (QueryIdentifier.java:57-58), so
**no dedup token is set**. In that config the only duplicate defence is offset
commit timing. Under `exactlyOnce=true` this flag is **silently ignored, not
rejected**: every guard reads `!isExactlyOnce() && isIgnorePartitionsWhenBatching()`
(ClickHouseSinkTask.java:150, Processing.java:143, ProxySinkTask.java:87) and no
config validation flags the combination — unlike `bufferCount > 0` +
`exactlyOnce`, which **throws** a ConnectException at task start
(ClickHouseSinkTask.java:54-62). The harness must therefore **assert the config
combination itself** (fail fast if a cell config pairs
`ignorePartitionsWhenBatching=true` with `exactlyOnce=true` — the run would
silently test something other than what its runtime keys claim). The matrix pins
`ignorePartitionsWhenBatching=false` for the primary cells and adds one
**watch cell** with it true (at-least-once only) to characterise the
weaker-guarantee path — flagged as expected-may-duplicate, not a failure.

### 2.4 KeeperMap state table lifecycle

`KeeperStateProvider.init()`
(`src/main/java/com/clickhouse/kafka/connect/sink/state/provider/KeeperStateProvider.java:74-92`):

```
CREATE TABLE IF NOT EXISTS `<zkDatabase>`[ ON CLUSTER <keeperOnCluster>]
  (`key` String, minOffset BIGINT, maxOffset BIGINT, state String)
  ENGINE=KeeperMap('<zkPath>') PRIMARY KEY `key`;
```

- `zkDatabase` (config key `zkDatabase`, default `connect_state`) is the **state
  table name**; `zkPath` (`zkPath`, default `/kafka-connect`, must begin `/` —
  `ClickHouseSinkConfig.java:152-160`) is the KeeperMap root path.
- `key` = `"<topic>-<partition>"`; `setStateRecord` writes with
  `SETTINGS wait_for_async_insert=1` (KeeperStateProvider.java:135-157).
- `keeperOnCluster` (config `keeperOnCluster`, default `""`) is **only for
  self-hosted clusters**; on a single Cloud service it stays empty (help text at
  `ClickHouseSinkConfig.java:608`, and the integration test passes the cluster
  name only in cluster mode — `ExactlyOnceTest.java:169-186`).

**Stale-state trap (must be reset between runs).** Because `zkPath` is a fixed
KeeperMap root, offset ranges written by a previous chaos run **persist across
runs**. If run *N+1* replays offsets from 0 against state left by run *N*, the
overlap logic will mis-classify (`CONTAINS`/`PREVIOUS` throws, or worse silently
skips). The reset procedure is spelled out in §3.3.

### 2.5 Precedent already in the repo: Cloud-service restart chaos

`ExactlyOnceTest.checkSpottyNetwork*`
(`src/integrationTest/java/com/clickhouse/kafka/connect/sink/ExactlyOnceTest.java:128-235`)
already exercises exactly-once against a **Cloud** service by calling
`clickhouseCloudAPI.restartService()` mid-run (`:221`) then restarting the
connector, and asserting `count == expected` and `duplicates == 0`. The Cloud
API helper (`ClickHouseCloudAPI` — `stopInstance`/`startInstance`/
`restartService`) is a **ready-made CH-side fault primitive on a Cloud service**.

**Caveat the harness must inherit, not copy.** That test's oracle uses a
duplicates count derived on the target alone (`databaseCounts[2]`) — the
`count()`-vs-`uniqExact()` family that the corrected FORMULA LAW **bans** for
datasets that may contain duplicate source ids (§5). Our harness reuses the
`restartService()` *primitive* but replaces its oracle with the corrected
target-vs-source formula.

---

## 3. Fault matrix

### 3.1 Dimensions

- **Rows** = fault primitive (§3.2).
- **Columns** = delivery mode: `exactly_once='0'` (at-least-once + dedup) /
  `exactly_once='1'` (exactly-once + KeeperMap). Encoded **as string** `'0'`/`'1'`
  per the Map-value convention (contract §1, all runtime Map values are strings).
- **Kill timing** = `fault_at_pct ∈ {10, 50, 90}` (fraction of `rows_expected`
  drained when the fault fires). For the crash-class primitives the
  **KeeperMap crash windows W1/W2/W3** (§2.2) are a finer *sub-selection within
  a pct point, not a multiplier on run count*: at the chosen pct the harness
  races the fault against the connector's state-transition log markers to land
  inside a specific window (best-effort; the window actually landed is
  recorded in `fault_window`, not assumed).
- **Cell** = expected semantics + what the oracle asserts (§3.4).

### 3.2 Fault primitives

| # | `fault_type` | Primitive | Source of truth |
|---|---|---|---|
| F1 | `connect_pod_kill` | `kubectl delete pod bench-connect-connect-0` mid-drain; Strimzi recreates it, tasks rebalance | ECOSYSTEM §9 kill table row 1 |
| F2 | `task_restart` | Connect REST `POST /connectors/bench-clickhouse-sink/tasks/<n>/restart` via the poller host | ECOSYSTEM §9 row 2 |
| F3 | `broker_pod_kill` | `kubectl delete pod bench-combined-0` (RF=1: topic briefly unavailable; PVC persists, clients reconnect) | ECOSYSTEM §9 row 3 |
| F4 | `netpol_connect_ch` | **default-deny egress** NetworkPolicy on the Connect pod with explicit **broker + DNS carve-outs** (allow `bench-kafka-bootstrap` + kube-dns; everything else — including the CH endpoint — denied) for `recovery_seconds` T, then removed. Denying the Cloud endpoint by `ipBlock` is deliberately avoided: the Cloud LB IPs are dynamic and egress leaves via public subnets, so an ipBlock deny is fragile. **Enforcement prerequisite below.** | ECOSYSTEM §9 row 4 (corrected) |
| F5 | `netpol_connect_broker` | NetworkPolicy on the Connect pod denying egress to the broker (allow DNS + CH; deny `bench-kafka-bootstrap`) for T. **Enforcement prerequisite below.** | ECOSYSTEM §9 row 4 (corrected) |
| F6 | `ch_user_revoke` | `ALTER USER`/revoke or drop grants on the CH target mid-run, restored after T | ECOSYSTEM §9 CH option (a) |
| F7 | `ch_stop_merges` | `SYSTEM STOP MERGES` (+ optional quota) to induce back-pressure, `START MERGES` after T | ECOSYSTEM §9 option (b) |
| F8 | `ch_cloud_restart` | `ClickHouseCloudAPI.restartService()` on the staging service (§2.5) — the strongest CH-side primitive available on Cloud without self-hosting | ExactlyOnceTest.java:221 |
| F9 | `ch_node_kill` *(self-hosted extension)* | Delete a pod of an **in-cluster** CH StatefulSet on `bench-ng`; connector points at it via `TARGET_CH_HOST`. **`environment_class='self_hosted'`** per contract §1.1 (Amendment 2026-07-07, scoping 2026-07-07b). | ECOSYSTEM §9 option (d) |

Cloud-managed CH nodes **cannot** be killed directly (ECOSYSTEM §9); F8 (whole-
service restart) and F9 (self-hosted node kill) are the two ways to get
CH-side hard-failure semantics. F9 is the only way to get true single-node loss
while other nodes serve.

**⚠ F4/F5 enforcement prerequisite (BLOCKING for those cells).** The
`kafka-bench` cluster is provisioned with **vanilla EKS + VPC CNI**
(`infra/cluster.yaml` — its only addon is `aws-ebs-csi-driver`; no
`vpc-cni` `configurationValues` with `enableNetworkPolicy: "true"`, no network
policy node agent, no Calico/Cilium). On such a cluster a NetworkPolicy object
is **accepted by the API server and silently unenforced**: the F4/F5 cell would
drain completely unfaulted and record a **false-green PASS**. Before F4/F5 can
run, the cluster needs a NetworkPolicy enforcer — preferred: the managed
`vpc-cni` addon with `configurationValues: '{"enableNetworkPolicy": "true"}'`
(which deploys the AWS network policy node agent); alternative: Cilium. This is
an **infra change to `cluster.yaml`/provisioning and requires approval** —
raised as open question §11.7. Until it lands, F4/F5 cells are **not runnable**
and must be excluded from any sweep (never recorded as PASS). The
fault-took-effect rule (§5) is the systemic backstop: even with an enforcer
installed, an F4/F5 cell that shows **no observed fault effect** inside the
injection window is `integrity_unverified`, never PASS.

### 3.3 Deliberate dup-bearing dataset (REQUIRED matrix extension)

To prove **the oracle itself** — that it does not false-positive on legitimate
source duplicates and does not false-negative on real duplication — the matrix
includes a source variant with **known duplicate WatchIDs**:

- `SOURCE_ROWS_EXPECTED` = N (offsets-derived, counts duplicates),
  `SOURCE_UNIQUE_EXPECTED` = U where **U < N** (staged constant, computed once
  off the dup-bearing parquet prefix, recorded in the runtime map).
- Reuse the producer's staged-prefix mechanism (ECOSYSTEM §5/§9: any subset
  works, `rows_expected` is offsets-derived) with a prefix whose files contain
  duplicated ids. A small staged prefix (e.g. `hits-dup-2m/`) under
  `s3://shimons/clickbench-kafka-bench/` keeps cost low.
- A **fault-free** run on this dataset must PASS (`rows_delivered==N`,
  `duplicate_rows==0`, `unique_delivered==U`). A crash run on it must still PASS.
  A negative-control run (deliberately double-delivered without dedup) must
  produce `duplicate_rows>0` and FAIL — this is the fixture that proves the
  oracle can fail (§10, fail-then-pass discipline).

### 3.4 Cells — expected semantics per (primitive × mode)

Notation: **PASS** = oracle asserts zero loss + zero dup and expects it;
**PASS(dedup-absorbed)** = duplicates are produced on the wire but ClickHouse
drops them, net zero; **PASS(drift-caveat)** = expected-PASS **with the
boundary-drift caveat** (§2.3): a post-restart at-least-once re-delivery under
drifted batch boundaries can double-land a completed-but-uncommitted insert
under a new token — a duplicate in these cells is delivery-mode signal, not a
harness error (still recorded as MISMATCH per the oracle); **WATCH** =
characterisation cell, may show residual duplicates, flagged not failed. Every
cell additionally carries the **fault-took-effect assertion** (§5).

| Primitive | at-least-once (`'0'`) | exactly-once (`'1'`) |
|---|---|---|
| F1 connect_pod_kill | PASS(drift-caveat): rebalance re-delivers from last commit; token drops dup blocks when boundaries reproduce | PASS: KeeperMap `BEFORE_PROCESSING` bounds replay to recorded range; block dedup backs it |
| F2 task_restart | PASS(dedup-absorbed): framework retry of the same put reproduces boundaries | PASS |
| F3 broker_pod_kill | PASS: no data acked-then-lost (RF=1 PVC persists); on reconnect, re-consume + dedup | PASS: same, state reconciles |
| F4 netpol_connect_ch (default-deny egress + carve-outs; prerequisite §3.2) | PASS(dedup-absorbed): inserts fail during partition → framework retries same batch → same token | PASS: retries reuse token; W2 is the interesting window |
| F5 netpol_connect_broker (prerequisite §3.2) | PASS: consumer stalls, resumes; no insert attempted during partition | PASS |
| F6 ch_user_revoke | PASS-after-restore: inserts error while revoked (run marked `outcome='failed'` if ingest never completes); on restore, retries + dedup → integrity holds. Tests the CHECK_ERROR path (§5). | PASS-after-restore |
| F7 ch_stop_merges | PASS: back-pressure only; correctness unaffected (merges resume) | PASS |
| F8 ch_cloud_restart | PASS(drift-caveat): mirrors `checkSpottyNetwork`, but the restart also bounces the connector's view — post-restart re-polls may drift boundaries | PASS: the canonical exactly-once + Cloud-restart cell |
| F9 ch_node_kill (self-hosted) | PASS(dedup-absorbed) | PASS |
| dup-dataset (§3.3), any fault | PASS: `rows_delivered==N`, `unique==U`, `dup_rows==0` | PASS |
| `ignorePartitionsWhenBatching=true` watch (§2.3) | **WATCH**: token null → no block dedup; duplicates possible under W2/W3, flagged | N/A (flag unavailable under exactlyOnce) |

### 3.5 Run count

Full matrix arithmetic: 9 primitives × 2 modes × 3 `fault_at_pct` = 54 cells;
F6/F7 are timing-invariant and need only 1 pct point each (2 primitives × 2
modes × 2 dropped pct points = **−8**); plus the dup-dataset extension (F1/F8 ×
2 modes = **+4**), the 1 WATCH cell (**+1**) and 1 negative control (**+1**):
54 − 8 + 4 + 1 + 1 = **52 chaos runs** for the full sweep (the W1/W2/W3 axis
selects within a pct point and does not multiply this count, §3.1; F4/F5 cells
— 12 of the 52 — are gated on the NetworkPolicy enforcement prerequisite,
§3.2). The smoke subset (§7) is 4 runs.

---

## 4. Isolation ground rules (ECOSYSTEM §9, verbatim intent)

1. **Never run chaos concurrently with a benchmark pair** (shared broker/target).
   The harness must refuse to start if a pair run is active.
2. **Distinct everything**: topic `hits-chaos`, distinct consumer groups,
   distinct target table `clickbench.hits_chaos`, distinct `connector` value
   `kafka-connect-chaos`, distinct KeeperMap `zkPath`/`zkDatabase` for the
   exactly-once cells (e.g. `zkPath=/kafka-connect-chaos`,
   `zkDatabase=connect_state_chaos`) so chaos state never touches any real
   benchmark state.
3. **Digest-pin the images** under test; record them in the runtime map.
4. **Scale down when done**; verify both nodegroups report 0.
5. **Fail loud and classify honestly**: infra error ≠ assertion failure — the
   CHECK_ERROR vs MISMATCH lesson (§5).
6. **Expect Cloud idling** (~15 s wake) and occasional stalls; chaos assertions
   retry/timeout accordingly, and that flakiness is itself chaos signal.

**Run identity / runtime keys** (all Map values are **strings**, contract §1):

| Key | Values | Notes |
|---|---|---|
| `connector` | `'kafka-connect-chaos'` | (top-level `perf.runs` column, not the Map) keeps chaos out of trends |
| `chaos_id` | `<date>-<seq>` | groups all cells of one sweep |
| `fault_type` | F1…F9 token from §3.2 | |
| `fault_at_pct` | `'10'` \| `'50'` \| `'90'` | string |
| `fault_window` | `'W1'`\|`'W2'`\|`'W3'`\|`'post_after'`\|`'na'` | the crash window actually landed |
| `recovery_seconds` | int-as-string | T for timed faults; measured for pod kills (§6) |
| `exactly_once` | `'0'` \| `'1'` | delivery mode |
| `delivery_mode` | `'at_least_once'` \| `'exactly_once'` | human-readable mirror |
| `environment_class` | `'staging'` (Cloud target) \| `'self_hosted'` (F9) | contract §1.1 |
| `ignore_partitions_when_batching` | `'0'` \| `'1'` | pinned `'0'` except the WATCH cell |

---

## 5. Oracle

The oracle is the benchmark's integrity check, applied post-recovery-drain. It
**must** implement the corrected FORMULA LAW (ECOSYSTEM §8, contract §3, SQL 20
header):

```
rows_delivered   = count()          FROM clickbench.hits_chaos
unique_delivered = uniqExact(WatchID)
rows_expected    = Σ (end − beginning) over partitions   (broker offsets)

duplicate_rows   = rows_delivered − rows_expected          # target-vs-SOURCE
                                                           # MUST be 0
loss             = rows_expected   − rows_delivered        # MUST be 0
uniqueness       = unique_delivered == SOURCE_UNIQUE_EXPECTED  # source constant
```

**BANNED**: `duplicate_rows = count() − uniqExact()` on the target. That formula
false-positives on any dataset with legitimate duplicate source ids — precisely
the dup-bearing dataset §3.3 introduces. The in-repo `checkSpottyNetwork` oracle
(§2.5) uses the banned family; the harness must **not** copy it.

**Per-cell assertions** (§3.4 cells map to these):

- **Loss** = `rows_expected > rows_delivered` ⇒ integrity **MISMATCH** ⇒ run
  **FAILS** (contract §3.1). Any cell showing loss is a real bug.
- **Duplication** = `duplicate_rows > 0` ⇒ MISMATCH ⇒ FAILS. Expected 0 in every
  cell except the WATCH cell (§3.4), where it is **flagged**, not failed.
- **Uniqueness** = `unique_delivered != SOURCE_UNIQUE_EXPECTED` ⇒ MISMATCH.
  Asserted against the **source** constant (U for the dup dataset, N for the
  clean dataset), never against the target's own count.
- **Fault-took-effect (GENERAL RULE, every timed/injected fault cell).** A
  chaos PASS is only meaningful if the fault demonstrably fired. **Every cell
  must additionally assert an observed fault effect inside the injection
  window** — at least one of: insert errors (`perf.ch_inserts` exception codes
  / `insert_errors_during_fault > 0`), a consumer-lag stall or spike in the
  poller samples, task FAILED/restart transitions, or (pod-kill class) the pod
  actually recreated. A cell whose integrity numbers are clean but which shows
  **no observed fault effect** is **invalid — classified
  `integrity_unverified` (fault-not-observed), NEVER PASS**. This is the
  systemic defence against silently-unenforced fault primitives (the F4/F5
  NetworkPolicy trap, §3.2) and against injectors that raced past the drain.

**CHECK_ERROR vs MISMATCH discipline** (the load-bearing distinction). A chaos
test deliberately induces the exact failure classes that once mislabeled a
perfect run (CH read timeout during the check). Therefore:

- An **infra exception** during the integrity read (connection hang, read
  timeout, Cloud wake stall, transient auth error during an F6 revoke window)
  ⇒ **retry with backoff**; if still failing ⇒ classify
  **CHECK_ERROR → `integrity_unverified`** (contract §1.3), **never** a MISMATCH
  and **never** a false FAIL verdict.
- Only a **successfully computed** count/uniqExact that violates the assertions
  above is a MISMATCH.
- A run whose ingest step never completed (e.g. F6 grant never restored in time)
  is `outcome='failed'` and its integrity is `integrity_unverified` by
  definition (contract §1.3) — captured and marked, not a false pass.

This maps onto the checker hardening already in progress (ECOSYSTEM §8: "an
infra exception during the check must classify as
CHECK_ERROR/`integrity_unverified`, never as MISMATCH"). The harness task should
depend on / land that hardening in `check_integrity.py`.

---

## 6. Recovery metrics (diagnostic, never gated)

Emitted as `perf.metrics` rows on the chaos run, for characterisation only. They
**do not** feed bands, ratios, or verdicts (§1 non-goals).

| Metric | Meaning | Source |
|---|---|---|
| `recovery_seconds` | wall time from fault injection to lag returning to 0 | poller lag samples |
| `redelivered_offsets` | Σ offsets re-consumed after the fault (re-delivery volume) | consumer group offset deltas / Connect JMX |
| `ch_dedup_dropped_blocks` | blocks ClickHouse dropped via `insert_deduplication_token` | `system.part_log` / `system.query_log` dedup counters on the target |
| `task_restart_count` | tasks that transitioned FAILED→RUNNING or restarted | Connect REST status polls |
| `insert_errors_during_fault` | insert exceptions in the fault window | `perf.ch_inserts` exception codes (existing capture) |

`ch_dedup_dropped_blocks > 0` in a PASS(dedup-absorbed) cell is the positive
evidence that dedup did the work; `== 0` in an exactly-once W3 cell says state
short-circuited the re-insert before it reached CH.

---

## 7. Cost & cadence

- **Node shape per cell**: identical to a benchmark run — 4×`m6i.large`
  (`bench-ng`) + 1×`m6i.xlarge` (`connect-ng`), ~$0.58/hr compute fully scaled
  (ECOSYSTEM §2). F9 adds a small in-cluster CH StatefulSet on `bench-ng`
  (fits within the 4 nodes; may need a 5th at ~$0.096/hr for headroom).
- **Est. cost/cell**: a chaos cell is shorter than a full pair (single arm,
  smaller dataset acceptable). At ~10–15 min/cell incl. scale + preload +
  recovery + settle ≈ **$0.10–0.20/cell**. Scale up once, run several cells
  back-to-back before scaling down, to amortize the ~6–7 min preload and node
  spin-up (the dataset is re-loaded per cell only if it differs).
- **Suggested initial subset (smoke, 4 runs)** before the full matrix:
  `{F1 connect_pod_kill, F8 ch_cloud_restart} × {at-least-once, exactly-once}`
  at `fault_at_pct='50'` on the clean 10M dataset. This proves both state
  providers, both a Connect-side and a CH-side fault, and the whole
  orchestration/oracle path end to end, at ~$1 total. Add the dup-dataset
  fault-free PASS + negative control (2 runs) to validate the oracle before any
  broader sweep.
- **Cadence**: on-demand / pre-release, **not** nightly. Never concurrent with
  the nightly pair.

---

## 8. Orchestration & shared-lib factoring

### 8.1 What transfers unchanged (reuse as-is)

Cluster + scale scripts (`infra/scale-up.sh`, `scale-down.sh`), broker +
registry, the producer (any row count via a staged prefix), Connect/connector
templates, the poller, `perf.*` capture, and the integrity check as oracle
(ECOSYSTEM §9 "what transfers unchanged").

### 8.2 `orchestration/lib_bench.sh` — factor these OUT of `run_pair.sh`

`run_pair.sh` (1632 lines) already isolates its phases as functions; the harness
task extracts the run-mode-agnostic ones into a shared `lib_bench.sh` sourced by
**both** `run_pair.sh` and the new `chaos_run.sh`. Factor out (function names as
they exist today in `run_pair.sh`):

| Function(s) | Line(s) today | Role |
|---|---|---|
| `log` / `warn` / `die` | 126-128 | logging |
| `_ref_is_digest`, `resolve_tag_to_digest`, `validate_image_ref`, `_ecr_newer_push_exists`, `check_image_provenance` | 176-337 | digest-pin enforcement |
| `phase_scale_up` | 551 | scale nodegroups + preflight + Kafka Ready |
| `phase_preload` + `broker_topic_row_count` + `producer_rows_sent_sum` | 556, 688, 735 | topic + sharded producer Job; `rows_expected` from offsets |
| `phase_poller_host` / `teardown_poller_pod` | 767, 799 | in-cluster poller pod |
| `apply_secret_and_metrics`, `connect_pod`, `deploy_connect`, `delete_connect`, `deploy_connector`, `delete_connector`, `wait_tasks_running` | 804-984 | Connect + connector deploy/teardown |
| `run_poller_sample` | 986 | lag/JMX/CPU sampling |
| `finalize_and_insert_metrics`, `capture_and_record`, `append_flag`, `ingest_fail_reason`, `ingest_failed` | 1035-1331 | client/server metric capture + `perf.*` insert (+ integrity call) |
| `phase_teardown_topic`, `phase_scale_down`, `cleanup_trap` | 1454-1499 | teardown + always-cleanup trap |

`build_runtime_json` (466) is factored but **parameterized**: the chaos harness
passes chaos runtime keys (§4) instead of arm/tier/pair keys.

### 8.3 `chaos_run.sh` — chaos-only additions (new)

A `run_pair.sh` sibling in `orchestration/`, sourcing `lib_bench.sh`:

- **Fault injectors** — one function per F1…F9 (§3.2): `fault_connect_pod_kill`,
  `fault_task_restart`, `fault_broker_pod_kill`, `fault_netpol_connect_ch`,
  `fault_netpol_connect_broker`, `fault_ch_user_revoke`, `fault_ch_stop_merges`,
  `fault_ch_cloud_restart`, `fault_ch_node_kill`. Each takes `recovery_seconds`
  and cleans up its own fault (removes the NetworkPolicy, restores the grant,
  `START MERGES`, etc.) in a nested trap so a mid-fault failure never leaves the
  cluster wedged. The netpol injectors **preflight the enforcement
  prerequisite** (§3.2: verify the network-policy node agent / enforcer is
  running) and refuse to run the cell otherwise — never rely on the API
  accepting the policy object.
- **Recovery watcher** — polls consumer lag + Connect task status until lag=0
  and 3/3 RUNNING, records `recovery_seconds` and the §6 diagnostics. Tolerates
  transient task FAILED→RUNNING cycles during legitimate W1–W3 recovery (the
  CONTAINS/ERROR retry shapes, §2.2) without failing the cell; it also collects
  the **fault-took-effect evidence** (§5) for the oracle.
- **Window racer** — for the crash-class faults, watches the connector log for
  the state-transition markers to fire the kill inside W1/W2/W3; records
  `fault_window` actually landed. **Log-level reality**: the per-batch
  insert-start marker on the partitioned path is **DEBUG**
  (`doInsert` — Processing.java:67), so the racer either (a) runs the Connect
  worker with DEBUG for `com.clickhouse.kafka.connect.sink.processing`, or
  (b) races on `KeeperStateProvider.setStateRecord`'s **INFO**
  `"Write state record: …"` lines (KeeperStateProvider.java:141) — the
  BEFORE/AFTER state writes bracket the insert, so two consecutive
  state-write lines delimit W1/W2/W3 without a log-level change (exactly-once
  cells only; at-least-once cells have no state writes and fall back to (a) or
  pct-only timing).
- **KeeperMap reset** (exactly-once cells) — before each exactly-once cell,
  `DROP TABLE IF EXISTS <zkDatabase>` on the target (drops the KeeperMap state)
  and verify the KeeperMap path is clear, per §2.4 stale-state trap. Recreated
  lazily by `KeeperStateProvider.init()` on connector start.
- **Per-cell loop** — iterate the matrix (§3), scale up once, preload once per
  dataset, run each cell (deploy → drain to `fault_at_pct` → inject → recover →
  drain to 0 → settle → integrity → record with `connector='kafka-connect-chaos'`),
  scale down at the end.
- **Chaos config template** — a `kafkaconnector.json.tmpl` variant (or
  parameters) setting `exactlyOnce`, `zkPath`/`zkDatabase`/`keeperOnCluster`,
  and pinning `bufferCount=0`/`ignorePartitionsWhenBatching=false` per §2.3.
  The harness **validates the combination itself** before deploy:
  `bufferCount>0`+`exactlyOnce` would throw at task start
  (ClickHouseSinkTask.java:54-62), but
  `ignorePartitionsWhenBatching=true`+`exactlyOnce` is **silently ignored** by
  the connector (§2.3) — the harness must reject it so a cell never silently
  tests a different config than its runtime keys record.

---

## 9. KeeperMap-on-staging feasibility (from source/docs, no live calls)

**Question**: can the exactly-once cells run against the ClickHouse Cloud
**staging** target (`gams6lhck3.us-east-2`) as-is?

**Answer (design-time, needs one live confirmation before the harness runs)**:
**Very likely yes, with an empty `keeperOnCluster` and a distinct `zkPath`** — but
the server-side `keeper_map_path_prefix` must be present on the staging service,
and that is the one item to verify live.

Evidence:

1. **The connector creates the KeeperMap table itself** with
   `ENGINE=KeeperMap('<zkPath>')` and no `ON CLUSTER` when `keeperOnCluster` is
   empty (KeeperStateProvider.java:74-92). On a single Cloud service
   `keeperOnCluster` stays empty — the integration test only passes a cluster
   name in cluster mode (ExactlyOnceTest.java:169-186).
2. **Cloud already runs exactly-once against a Cloud service** in the repo's own
   suite: `checkSpottyNetwork*` runs the exactly-once connector against a Cloud
   service and restarts it (ExactlyOnceTest.java:128-235), i.e. the KeeperMap
   engine works on ClickHouse Cloud in the exact configuration we need.
3. **The KeeperMap engine requires the server setting
   `<keeper_map_path_prefix>`.** The repo's own test cluster sets it
   (`src/testFixtures/docker/clickhouse/cluster/config.xml:53`:
   `<keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>`).
   ClickHouse Cloud services generally have this enabled by default, but it is a
   **server-level** setting we do not control from the connector.

**The one live check to run before the exactly-once cells** (documented so the
harness author does it, not skipped):

```sql
-- On the staging target, confirm KeeperMap is usable:
SELECT value FROM system.server_settings WHERE name = 'keeper_map_path_prefix';
-- non-empty ⇒ KeeperMap allowed. Then a smoke create/drop:
CREATE TABLE IF NOT EXISTS connect_state_chaos_probe
  (`key` String, minOffset BIGINT, maxOffset BIGINT, state String)
  ENGINE=KeeperMap('/kafka-connect-chaos-probe') PRIMARY KEY `key`;
DROP TABLE connect_state_chaos_probe;
```

If `keeper_map_path_prefix` is empty on staging, the exactly-once cells fall back
to **F9's in-cluster self-hosted CH** (which we configure with the prefix, as the
test cluster does) with `environment_class='self_hosted'` — the at-least-once
cells are unaffected either way.

---

## 10. Acceptance criteria for the harness task (follow-on)

The harness task is accepted when:

1. `orchestration/lib_bench.sh` exists, `run_pair.sh` sources it and still passes
   its existing tests (no behavioural change to the pair path — a pure
   refactor, proven by the current `orchestration/tests`).
2. `orchestration/chaos_run.sh` runs the **smoke subset** (§7, 4 runs) green:
   both modes × {F1, F8} at 50%, each landing `integrity_ok`, `duplicate_rows=0`,
   `rows_delivered==rows_expected`, `unique_delivered==SOURCE_UNIQUE_EXPECTED`.
3. The dup-dataset fault-free PASS and the **negative control** (deliberate
   double-delivery → `duplicate_rows>0` → FAIL) both behave as designed —
   proving the oracle can both pass and fail.
4. The integrity checker's **CHECK_ERROR vs MISMATCH** hardening is in place: an
   injected infra exception during the check yields `integrity_unverified`, not
   a false MISMATCH/FAIL (fixture-tested). The **fault-took-effect rule** (§5)
   is likewise fixture-tested fail-then-pass: a cell with clean integrity but no
   observed fault effect classifies `integrity_unverified` (fault-not-observed),
   never PASS.
5. KeeperMap reset between exactly-once cells is verified (no stale-state
   carryover; a second cell on the same `zkPath` after a reset starts from
   NONE state).
6. Chaos runs carry `connector='kafka-connect-chaos'` and are absent from every
   benchmark trend/band/ratio.

### Review / fixture standards that apply

- **Fail-then-pass for any oracle logic** (contract + repo convention): every
  assertion path in the oracle and every verdict branch must be demonstrated
  failing on a crafted input before it is trusted passing. The negative control
  (§3.3, criterion 3) is the headline instance; each new `flag_reason`/verdict
  branch needs its own fail-then-pass fixture.
- Digest-pinning is law (ECOSYSTEM §4) — `chaos_run.sh` rejects bare tags like
  `run_pair.sh` does.
- Fail loud, classify honestly, always tear down (§4).

---

## 11. Open questions for the manager / principal

1. **KeeperMap on staging** (§9): who runs the one live
   `system.server_settings` check, and is the fallback to self-hosted F9 for
   exactly-once acceptable if staging lacks the prefix? (Blocks only the
   exactly-once cells, not the whole task.)
2. **Dup-bearing dataset staging** (§3.3): approve staging a `hits-dup-*` prefix
   under `s3://shimons/clickbench-kafka-bench/`, and who computes/records the
   `SOURCE_UNIQUE_EXPECTED=U` constant? Full ClickBench `hits` already contains
   duplicate `WatchID`s (per ECOSYSTEM §8) — we can carve a dup-bearing prefix
   straight from it rather than synthesize one.
3. **`tolerateStateMismatch`** (Processing.java:252-259): should exactly-once
   chaos cells run with it `true` (survive `PREVIOUS`/re-processed batches
   quietly) or `false` (throw, surfacing the mismatch as a hard signal)? Default
   is `false`; a crash test arguably wants `false` to catch real state bugs, but
   some benign re-delivery windows legitimately hit `PREVIOUS`. Recommend
   `false` + treat the throw as a captured `outcome='failed'`, but flag for a
   decision.
4. **Cloud-restart blast radius** (F8, §2.5): `restartService()` restarts the
   whole staging service. Confirm no other workload shares that staging service
   during a chaos window (isolation rule 1 covers pairs; does anything else use
   `gams6lhck3`?).
5. **Export policy**: keep chaos runs entirely out of the DWH (recommended), or
   export them under `connector='kafka-connect-chaos'` for an archival chaos
   dashboard? Affects whether `chaos_run.sh` wires the export bridge at all.
6. **Checker hardening ownership**: is the CHECK_ERROR/`integrity_unverified`
   hardening (ECOSYSTEM §8, "in progress") landing under this task, or is the
   harness task allowed to depend on it landing separately first?
7. **NetworkPolicy enforcement — infra change approval** (§3.2 prerequisite,
   BLOCKING for F4/F5 only): the cluster's vanilla VPC CNI does **not** enforce
   NetworkPolicy; F4/F5 need the managed `vpc-cni` addon with
   `configurationValues: '{"enableNetworkPolicy": "true"}'` (+ its node agent)
   — or Cilium — added to `infra/cluster.yaml`/provisioning. This is a
   substrate change to the shared benchmark cluster (agent runs on the same
   nodes as the nightly pair): approve the change, or defer F4/F5 and run the
   other 40 cells first? Either way F4/F5 must not run (and must never record
   PASS) until an enforcer is verified present.
