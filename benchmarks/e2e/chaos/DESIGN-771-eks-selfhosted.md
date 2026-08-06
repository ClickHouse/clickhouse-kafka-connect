# Chaos / "monkey" exactly-once test — design (EKS self-hosted target)

**Issue [#771](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/771).**
This is an **alternative design** to `chaos/DESIGN.md` (task #43). It was produced
by re-reading #771 against the live infrastructure and the sink source, and it
deliberately diverges from `DESIGN.md` on three axes (§0). It reuses `DESIGN.md`'s
genuinely-good parts verbatim: the corrected **formula law**, the **crash-window**
analysis (W1/W2/W3), and the **fault-took-effect** rule.

**Audience**: the harness author, the manager, the principal reviewer.
Everything is grounded in `benchmarks/e2e/ECOSYSTEM.md`, the plan/contract, and
the checked-out `benchmark-v2` sink source (paths cited inline).

**Docs-first.** This document gates the harness implementation (a follow-on
task). No harness code lands under this task.

---

## 0. What this design changes vs `chaos/DESIGN.md`, and why

| Axis | `DESIGN.md` (task #43) | **This design (#771)** | Why |
|---|---|---|---|
| **CH target** | ClickHouse **Cloud staging** primary; self-hosted in-cluster CH is an *F9 fallback* for one row | **Self-hosted CH cluster on EKS is the *only* target** | #771 says "local Docker cluster, **not Cloud APIs**". The operator's ruling: an in-cluster CH cluster on EKS **is** the equivalent of the issue's docker cluster. We control it, kill its nodes directly (no Cloud API), and enable `keeper_map_path_prefix` ourselves. |
| **Test shape** | Deterministic **52-cell matrix** | **Randomized "monkey" loop primary** (seeded, reproducible) + a small deterministic **smoke gate** | #771: "a looping *monkey* test injecting faults at **random intervals across many rounds**". |
| **Scope** | Includes Cloud-only faults (F6 user-revoke, F8 Cloud-restart) + a KeeperMap-on-Cloud-staging feasibility question (§9/§11.1 of `DESIGN.md`) | **Drops** F8 (Cloud restart) and the KeeperMap-on-staging question entirely; keeps CH-side faults as **node/keeper kills** | Self-hosted target makes the strongest CH-side primitive (real node loss) directly available and removes the Cloud dependency and its open question. |

Everything else in `DESIGN.md` — the source-grounded crash analysis, the oracle
formula law, the isolation ground rules, the runtime-key vocabulary — is inherited.

---

## 1. Goal & non-goals

**Goal.** Prove the connector delivers **zero loss and zero duplication** into a
**self-hosted ClickHouse cluster running on EKS** when faults are injected
mid-drain — CH nodes killed (including the Keeper-hosting node), Connect worker
hard-killed mid-batch, broker killed — driven by a randomized **monkey loop**
across many rounds, across **both delivery modes**:

- **at-least-once + block dedup** (`exactlyOnce="false"`) — the benchmark's
  production mode.
- **exactly-once + KeeperMap state** (`exactlyOnce="true"`) — the guarantee the
  connector advertises; state store lives ON the self-hosted CH keeper quorum.

The oracle is the benchmark **integrity check** (`capture/check_integrity.py` +
SQL 20), applied after the connector has fully recovered and drained to lag 0.

**Non-goals.**

- **Not a performance benchmark.** No throughput bands, ratios, or verdict-map.
  Recovery timing is **diagnostic** only (§6), never gated.
- **Must not pollute benchmark trends.** `connector='kafka-connect-chaos'`,
  distinct topic/table/group (§4), not exported to the DWH by default.
- **Not a proof of ClickHouse's own durability.** We deploy CH with a topology
  (§2) chosen so that a single node loss is *survivable by design*; we then test
  the **connector's** behaviour across that survivable loss. A design that lost
  data on node kill regardless of the connector (e.g. unreplicated shards) would
  make the oracle meaningless — see §2.

---

## 2. The chaos target — self-hosted CH cluster on EKS

**This is the load-bearing new piece.** The connector doesn't care which CH it
points at (`TARGET_CH_HOST`, ECOSYSTEM §9 option (d)); we deploy CH **in-cluster**
so we can kill its pods.

**Topology (mirrors the docker `one_shard_three_replicas` fixture).**

- **3 CH pods**, `ReplicatedMergeTree`, **1 shard × 3 replicas** — data is
  replicated to all three, so **killing one node loses no committed data**. This
  is required for a zero-loss oracle to be meaningful: on the alternative
  `three_shards_one_replica_each` shape, killing a node permanently loses that
  shard's rows *by design*, and the oracle would false-fail on infrastructure,
  not on a connector bug.
- **Embedded `clickhouse-keeper` in each CH pod → a 3-node quorum.** This mirrors
  `src/testFixtures/docker/clickhouse/cluster/config.xml` (which runs
  `keeper_server` + `zookeeper` sections on each node and sets
  `<keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>`). A
  3-node quorum **survives one node loss**, so KeeperMap state (exactly-once)
  persists across a single `ch_node_kill`. Killing a **second** node
  concurrently would break quorum — the monkey loop caps concurrent CH kills at
  1 (§3.3) so quorum is never intentionally destroyed (a quorum-loss test is a
  separate, explicitly-flagged experiment, not the default zero-loss assertion).
- **`keeper_map_path_prefix` is set in the deployed config** — so exactly-once /
  KeeperMap works with an **empty `keeperOnCluster`** and a distinct `zkPath`
  (`KeeperStateProvider.init()` creates the table itself,
  `KeeperStateProvider.java:74-92`). No Cloud, no feasibility unknown.
- **Access**: a headless + a round-robin `Service` (nginx or CH native LB) in
  front of the 3 pods, exactly as the docker fixture uses nginx. Connector's
  `TARGET_CH_HOST` = that Service. There is **no metrics CH** (decision §10.2):
  the oracle reads the self-hosted target directly and results are written as CI
  artifacts (§11).

**Capacity.** 3 CH+keeper pods on `bench-ng` (m6i.large ×4). Each CH pod is
modest for chaos-scale data (§7 uses a small dataset). `DESIGN.md` §7 already
judged an in-cluster CH "fits within the 4 nodes; may need a 5th at ~$0.096/hr" —
this design needs 3 such pods, so budget a **5th `bench-ng` node** for headroom
(open item §11.1: confirm live before first run).

**New infra artifacts** (mirror the docker fixture, translated to k8s):

- `chaos/ch-cluster.yaml` — StatefulSet (3 replicas) + Service(s) + a ConfigMap
  carrying `config.xml`/`users.xml` **derived from the test fixture** so the two
  clusters stay in lockstep (same keeper config, same `keeper_map_path_prefix`,
  same `ReplicatedMergeTree` macros via `REPLICA_NUM`/`SHARD_NUM` env).
- Provision/teardown wired into `chaos_run.sh` (§8), not into the pair path.

---

## 3. Fault model

### 3.1 Fault primitives (all kubectl / Connect REST — **zero Cloud API**)

| # | `fault_type` | Primitive | Source of truth |
|---|---|---|---|
| C1 | `connect_pod_kill` | `kubectl delete pod bench-connect-connect-0` (`--grace-period=0 --force` for a *hard* kill) mid-drain; Strimzi recreates it, tasks rebalance | ECOSYSTEM §9 kill table row 1 |
| C2 | `task_restart` | Connect REST `POST /connectors/bench-clickhouse-sink/tasks/<n>/restart` via the poller host | ECOSYSTEM §9 row 2 |
| C3 | `broker_pod_kill` | `kubectl delete pod bench-combined-0` (RF=1: topic briefly unavailable; PVC persists) | ECOSYSTEM §9 row 3 |
| C4 | `ch_node_kill` | `kubectl delete pod ch-chaos-<random>` — random replica of the self-hosted cluster (§2). Replication + quorum ⇒ survivable. **`environment_class='self_hosted'`.** | ECOSYSTEM §9 option (d) |
| C5 | `ch_keeper_leader_kill` | Kill the CH pod **currently leading Keeper** (resolve via `SELECT * FROM system.zookeeper`/keeper 4lw `mntr`), i.e. the issue's "kill the Keeper-hosting node" precisely | #771 "including the Keeper-hosting node" |

**Deferred (not in the default sweep):**

- `netpol_*` (network partitions) — same **CNI-enforcement prerequisite** as
  `DESIGN.md` §3.2: the cluster's vanilla VPC CNI silently drops NetworkPolicy.
  Deferred behind an approved substrate change; the fault-took-effect rule (§5)
  is the backstop.
- `ch_cloud_restart` / `ch_user_revoke` — **dropped**: Cloud-only, out of scope
  for a self-hosted, no-Cloud-API design.

### 3.2 Delivery modes (columns)

- `exactly_once='0'` — at-least-once + `insert_deduplication_token`
  (`QueryIdentifier.getDeduplicationToken()`,
  `QueryIdentifier.java:56-61`; applied `ClickHouseWriter.java:1003-1005` et al.).
- `exactly_once='1'` — exactly-once + KeeperMap on the self-hosted keeper quorum.

Both modes pin `bufferCount=0` and `ignorePartitionsWhenBatching=false`. The
harness **validates the config combination** before deploy: `bufferCount>0`
+`exactlyOnce` throws at task start (`ClickHouseSinkTask.java:54-62`), but
`ignorePartitionsWhenBatching=true`+`exactlyOnce` is **silently ignored**
(`ClickHouseSinkTask.java:150`, `Processing.java:143`, `ProxySinkTask.java:87`) —
so a run could otherwise silently test a config its runtime keys don't reflect.
One optional at-least-once **WATCH** cell with `ignorePartitionsWhenBatching=true`
characterises the token-null path (`QueryIdentifier.java:57-58` → token `null` →
no block dedup); duplicates there are flagged, not failed.

### 3.3 The monkey loop (primary)

The headline test. One monkey run = one delivery mode, one seed, many rounds:

```
seed  := <explicit int>                      # logged; the whole run is replayable
rng   := Random(seed)

# --- RAMP: continuous production so there is always backlog to chew ---
preload a base into hits-chaos, start continuous bounded-rate producer
deploy Connect + connector (the drain begins)

# --- INJECTION: R rounds, each fault lands mid-processing (§3.6 gate) ---
for r in 1..R:                               # ends early on budget / T_recover
    wait rng.uniform(t_min, t_max)           # random interval
    f := rng.choice(enabled_faults)          # C1..C5 (respect concurrency caps)
    inject f (crash-window racing for crash-class faults, §3.4)
    record {round, fault_type, fault_window, inject_ts, evidence}
    RECOVERY GATE (§3.6): block until fully recovered for quiet-window Q,
        or end the run on T_recover timeout (classify: stuck vs infra stall)

# --- FENCE: freeze rows_expected ---
stop the producer
rows_expected := Σ (broker end − beginning) over partitions   # frozen here

# --- QUIESCENCE / settle (§3.6): lag=0 sustained W, tasks RUNNING,
#     replication queues empty, DLQ depth == 0 ---
await quiescence  (or T_settle timeout → integrity_unverified)

# --- ORACLE (§5) once ---
run integrity oracle; classify; record
write one result artifact per run + per-round records (§11); return them
```

**Concurrency caps (invariants the loop must enforce):**

- **At most one CH pod down at a time** (§2: preserve keeper quorum + replica
  availability). The loop refuses to schedule `ch_node_kill`/`ch_keeper_leader_kill`
  while a CH pod is still `NotReady`.
- Never kill CH + broker + Connect all at once (that's a total outage, not a
  connector test); cap total concurrent faults at 1 in the default profile. A
  `--aggressive` profile (concurrent Connect+CH kill, still ≤1 CH) is a
  flagged, opt-in experiment.

**Determinism.** A fixed seed reproduces the fault *sequence and timings* exactly;
what it cannot pin is the precise crash *window* landed (that depends on live
timing), which is why `fault_window` is **recorded, never assumed** (§3.4).

### 3.4 Crash-window targeting (inherited from `DESIGN.md` §2.2)

`Processing.doLogic()` (`Processing.java:131-266`) runs, per batch:
`setStateRecord(BEFORE_PROCESSING)` → `doInsert` → `setStateRecord(AFTER_PROCESSING)`.
The three crash windows are the gaps: **W1** (after BEFORE, before insert),
**W2** (mid-insert), **W3** (after insert, before AFTER). On recovery all three
converge on the `BEFORE_PROCESSING` branch, which re-inserts and leans on **block
dedup**; `OVER_LAPPING` (not `SAME`) is the common re-poll shape
(`RangeContainer.java:57-68`, split at `Processing.java:195-216`) and its
left-boundary-mismatch path can **DLQ** rows (`Processing.java:205-210`) — the
watcher must alert on that (looks like loss to the oracle).

For crash-class faults the **window racer** (§8) watches the connector log for
state-transition markers and fires the kill inside a target window; the window
**actually** landed is recorded in `fault_window`
(`'W1'|'W2'|'W3'|'post_after'|'na'`). Log-level reality (from `DESIGN.md` §8.3):
the exactly-once `KeeperStateProvider.setStateRecord` writes are **INFO**
("Write state record: …", `KeeperStateProvider.java:141`) and bracket the insert,
so two consecutive state-write lines delimit the windows without a log-level
change; at-least-once has no state writes, so it races on the DEBUG
`doInsert` marker (`Processing.java:67`) or falls back to interval-only timing.

### 3.5 Smoke gate (deterministic, CI)

Before any long monkey run, a fixed, tiny deterministic set gates correctness end
to end: **{C1 connect_pod_kill, C4 ch_node_kill} × {at-least-once, exactly-once}**,
each a single fault at 50 % drained on the clean dataset. 4 runs, ~$1. Proves
both state providers, a Connect-side and a CH-side fault, the self-hosted CH
deploy, and the whole oracle path. Plus the dup-dataset PASS + negative control
(§5) to prove the oracle can pass *and* fail.

### 3.6 Recovery gate & run conclusion (the load-bearing loop invariants)

Two questions a monkey loop must answer explicitly, or it silently conflates
faults and runs its oracle on an unsettled system.

**(a) Recovery between crashes — the recovery gate.** The default profile
**fully recovers between faults**. This is not gentleness: it is what makes each
fault land *mid-processing* (the issue's actual requirement — a fault fired
during a dead period tests nothing) and keeps a failure attributable to a *fault
sequence* (a seed) rather than to an unbounded, unattributable pile-up. After
each injection, the loop blocks until **all** of the following hold, sustained
for a quiet window **Q (~30 s)**:

- all N tasks report `RUNNING` (not `FAILED`/`UNASSIGNED`) — reuse
  `ConfluentPlatform.getFirstTaskFailureOpt` (returns the failing task's trace);
- the killed pod (if any) is back `Ready` (Strimzi recreated Connect; the CH
  StatefulSet re-scheduled the replica);
- consumer lag is **strictly decreasing again** — forward progress observed
  *after* the fault cleared, not merely a momentary dip — or lag is already 0;
- **no new `FAILED→` task transition during Q**;
- (`ch_node_kill`/`ch_keeper_leader_kill` only) the rejoined replica has caught
  up: `system.replicas` replication queue empty and/or `SYSTEM SYNC REPLICA`
  returns.

The gate is bounded by **`T_recover`**. If it does not clear in time the run
**ends and is classified** — a **stuck connector** (a task pinned `FAILED` with a
state-machine exception in its trace) is a real bug (`outcome='failed'`, oracle
still run and typically shows loss); an **infra stall** (CH slow to reschedule,
replica slow to sync, with no task FAILED) is `integrity_unverified`. A monkey
loop must never hang. Transient `FAILED→RUNNING` cycles *inside* Q from
legitimate W1–W3 recovery (the `CONTAINS`/`ERROR` retry shapes,
`Processing.java:191-219`) are tolerated and counted in `task_restart_count`,
not treated as gate failures. Overlapping/cascading faults (inject before full
recovery) are the opt-in **`--aggressive`** profile only, still capped at ≤1 CH
pod down (§3.3).

**(b) When the test concludes — the fence + quiescence.** A run is *not*
concluded when lag momentarily hits 0; it is concluded at **quiescence** after a
**production fence** freezes the count:

1. **Fence** — stop the continuous producer, then read broker end-offsets →
   `rows_expected` is **frozen** (nothing more will be produced, so the target is
   fixed). `rows_expected` is offsets-derived, never producer-side (ECOSYSTEM §5).
2. **Quiescence / settle** — wait until **all** hold, sustained for a window
   **W (~60 s)**: consumer lag = 0; all tasks `RUNNING`; CH replication queues
   empty across replicas; **and the DLQ topic depth = 0** (see below). Reuse the
   benchmark's existing `settle-*.status` mechanism. Bounded by **`T_settle`**;
   timeout ⇒ `integrity_unverified` (never a silent pass).
3. **Oracle** (§5) runs exactly once, on the settled system.

**Continuous production is required** (not a large one-shot preload) so that
`R` real *mid-processing* rounds can occur with full recovery between each —
a fixed preload drains to 0 long before `R` faults with the recovery gate in
place. The fence is what keeps the oracle exact despite continuous production.

**DLQ is part of the conclusion, not an afterthought.** The `OVER_LAPPING`
left-boundary-mismatch path routes records to the DLQ
(`Processing.java:205-210`); to a count-based oracle those rows are **silent
loss**. Therefore quiescence asserts **DLQ depth 0**, and a non-zero DLQ at
conclusion is a **MISMATCH** (records landed nowhere), captured with the
offending offsets — never an unnoticed pass. (This assumes `errors.tolerance`
routing; the chaos connector pins DLQ config explicitly so depth is meaningful.)

### 3.7 Quorum-loss experiment (opt-in, flagged — decision §10.8)

An explicit `--quorum-loss` mode, **separate from the default zero-loss sweep**
and flagged distinctly so it never contaminates the core PASS. It kills **2 of
the 3 keeper nodes** at once (deliberately breaking the 3-node quorum, overriding
the §3.3 ≤1-CH-kill cap), holds for T, then restores them. The assertion is
**not** "no stall" — it is: while quorum is lost the connector **stalls** (inserts
fail / lag flat — exactly-once cannot write KeeperMap state; observed as
`fault_observed`), and once quorum returns it **recovers with zero loss and zero
duplication** (the standard oracle at quiescence, §3.6b). Result artifacts carry
`chaos_mode='quorum_loss'` so this experiment is filterable and never averaged
into the default runs. Exactly-once only (at-least-once has no KeeperMap
dependency, so quorum loss degenerates to a plain CH availability blip already
covered by C4).

---

## 4. Isolation & run identity

Same ground rules as ECOSYSTEM §9 / `DESIGN.md` §4:

1. Never concurrent with a benchmark pair (refuse to start if a pair is active).
2. Distinct everything: topic `hits-chaos`, distinct consumer groups, target
   table `clickbench.hits_chaos` on the **self-hosted** cluster, distinct
   `connector='kafka-connect-chaos'`, distinct KeeperMap
   `zkPath=/kafka-connect-chaos` / `zkDatabase=connect_state_chaos`.
3. Digest-pin images; record in the runtime map.
4. Scale down when done; verify nodegroups at 0 **and** the CH StatefulSet torn
   down.
5. Fail loud, classify honestly (CHECK_ERROR ≠ MISMATCH, §5).

**Runtime keys** (all Map values strings, contract §1):

| Key | Values |
|---|---|
| `connector` | `'kafka-connect-chaos'` (top-level column) |
| `chaos_id` | `<date>-<seq>` (groups a monkey run) |
| `chaos_mode` | `'monkey'` \| `'smoke'` |
| `chaos_seed` | int-as-string (monkey runs; the replay key) |
| `chaos_rounds` | int-as-string |
| `fault_type` | `C1..C5` token |
| `fault_window` | `'W1'|'W2'|'W3'|'post_after'|'na'` (per round) |
| `exactly_once` | `'0'` \| `'1'` |
| `delivery_mode` | `'at_least_once'` \| `'exactly_once'` |
| `environment_class` | `'self_hosted'` (always, this design) |
| `ignore_partitions_when_batching` | `'0'` (pinned; `'1'` only for the WATCH cell) |
| `rounds_completed` | int-as-string (rounds actually injected before fence/timeout) |
| `run_conclusion` | `'quiesced'` \| `'t_recover_timeout'` \| `'t_settle_timeout'` (§3.6) |
| `dlq_depth` | int-as-string (0 on PASS, §3.6/§5) |

---

## 5. Oracle (corrected formula law — inherited verbatim)

Applied **at quiescence** (§3.6: after the fence freezes `rows_expected` and the
system has settled), once per run:

```
rows_delivered   = count()          FROM clickbench.hits_chaos   (SELECT ... SETTINGS select_sequential_consistency=1, on a replica)
unique_delivered = uniqExact(WatchID)
rows_expected    = Σ (end − beginning) over partitions           (broker offsets, FROZEN at the fence §3.6)
dlq_depth        = messages in the chaos DLQ topic                (must be 0)

duplicate_rows   = rows_delivered − rows_expected                # target-vs-SOURCE; MUST be 0
loss             = rows_expected   − rows_delivered              # MUST be 0
uniqueness       = unique_delivered == SOURCE_UNIQUE_EXPECTED    # source constant
dlq_loss         = dlq_depth > 0                                 # MUST be false (§3.6)
```

**BANNED**: `duplicate_rows = count() − uniqExact()` on the target — false-positives
on any dataset with legitimate duplicate source ids (the dup-bearing dataset
below). The in-repo `ExactlyOnceTest.getCounts` uses the banned family
(`ExactlyOnceTest.java:255`, `total - uniqueTotal`); the harness must **not**
copy it.

**Replica-read caveat (new, self-hosted-specific).** Reading `count()` right after
a node was killed and rejoined must use **sequential consistency** and target a
replica known to be caught up (`select_sequential_consistency=1`, and/or await
replication via `system.replicas`/`SYSTEM SYNC REPLICA`), else a lagging replica
reports a transient undercount that looks like loss. A transient undercount that
resolves on re-read is a **CHECK_ERROR**, never a MISMATCH (below).

**Fault-took-effect (GENERAL RULE).** Every round's fault must show an observed
effect (insert errors from `system.query_log` on the target, a lag stall/spike in poller samples,
task FAILED→RUNNING, or the pod actually recreated). A run whose integrity is
clean but which shows **no** observed fault effect is `integrity_unverified`
(fault-not-observed), **never PASS**.

**CHECK_ERROR vs MISMATCH.** Infra exception during the read (connection hang,
read timeout, replica-not-caught-up transient) ⇒ retry with backoff ⇒ if still
failing, `integrity_unverified`, never a false FAIL. Only a *successfully
computed* count that violates the assertions is a MISMATCH. Depends on / lands
the `check_integrity.py` hardening (ECOSYSTEM §8, "in progress").

**Dup-bearing dataset + negative control** (inherited from `DESIGN.md` §3.3): a
staged prefix with known duplicate `WatchID`s (`SOURCE_UNIQUE_EXPECTED=U < N`)
must PASS fault-free and under chaos; a deliberately double-delivered
no-dedup negative control must FAIL (`duplicate_rows>0`). This is the
fail-then-pass fixture proving the oracle can fail.

---

## 6. Recovery metrics (diagnostic, never gated)

Per round, written as **fields in the per-round `rounds[]` array of the result
artifact** (§11), characterisation only — never gated: `recovery_seconds`,
`redelivered_offsets`, `ch_dedup_dropped_blocks` (`system.part_log`/`query_log`
dedup counters on the self-hosted target — fully queryable since we own the
server), `task_restart_count`, `insert_errors_during_fault`, `fault_observed`.

---

## 7. Cost & cadence

- Node shape: 4–5×`m6i.large` (`bench-ng`, +1 for the CH StatefulSet headroom) +
  1×`m6i.xlarge` (`connect-ng`) ≈ $0.58–0.68/hr fully scaled.
- Dataset: small (e.g. 2M rows) is sufficient for chaos; keeps preload + drain
  short. `rows_expected` is offsets-derived so any subset works (ECOSYSTEM §5).
- A monkey run (20 rounds, small dataset) ≈ 20–40 min ⇒ ~$0.30–0.50. Scale up
  once, run smoke + both-mode monkey runs back-to-back, scale down.
- **Cadence**: on-demand / pre-release, never nightly, never concurrent with a
  pair. The smoke gate is cheap enough to run in a chaos-labelled CI job.

---

## 8. Orchestration & shared-lib factoring (inherited from `DESIGN.md` §8, adapted)

**Reuse as-is**: scale scripts, broker + registry, producer (staged prefix),
Connect/connector templates, poller sampling, the integrity oracle. (The
`perf.*` capture/export is pair-only — not reused, decision §10.2.)

**`orchestration/lib_bench.sh`** — factor the run-mode-agnostic phases out of
`run_pair.sh` (1632 lines; phases already isolated as functions), sourced by
**both** `run_pair.sh` and the new `chaos_run.sh`: `log/warn/die`, the
digest-pin enforcement family, `phase_scale_up`, `phase_preload` (+ offset
counters), `phase_poller_host`, the Connect deploy/teardown family,
`run_poller_sample`, teardown + cleanup trap. **The `perf.*`-insert / export
family is NOT reused** (decision §10.2 — no metrics CH); the pair path keeps it,
the chaos path writes the artifact of §11 instead. `build_runtime_json` is
parameterized to accept chaos keys (§4) and emit them as artifact JSON fields.
Acceptance: `run_pair.sh` still passes its existing `orchestration/tests` after
the extraction (pure refactor).

**`orchestration/chaos_run.sh`** (new) — sources `lib_bench.sh`, adds:

- **CH-cluster lifecycle** — `deploy_ch_cluster` / `teardown_ch_cluster` applying
  `chaos/ch-cluster.yaml` (§2); waits 3/3 CH pods Ready + keeper quorum formed;
  sets `TARGET_CH_HOST` to the in-cluster Service.
- **Fault injectors** — one per C1..C5 (§3.1), each self-cleaning in a nested
  trap. `ch_keeper_leader_kill` resolves the current leader first. Each injector
  emits its fault-took-effect evidence.
- **Continuous producer** — the pair's producer is a one-shot preload Job; the
  chaos harness needs a **bounded-rate, long-lived** producer so there is always
  backlog to chew for `R` mid-processing rounds (§3.6b). Either a rate-limited
  streaming mode added to `producer.py` (preferred — one codebase) or a
  looped-replay wrapper; `enable.idempotence=true` and the offsets-derived count
  contract (ECOSYSTEM §5) are preserved. The **fence** stops it before freezing
  `rows_expected`.
- **Monkey loop** (§3.3) — seeded RNG, round scheduler, concurrency-cap
  enforcement, per-round recording, and the phase sequence
  ramp→injection→fence→quiescence→oracle (§3.6b).
- **Recovery gate + watcher** (§3.6a) — the multi-signal gate (tasks RUNNING,
  pod Ready, lag strictly decreasing again, no new FAILED during Q, replica
  caught up), bounded by `T_recover`; tolerates transient FAILED→RUNNING cycles
  from legitimate W1–W3 recovery (the CONTAINS/ERROR retry shapes,
  `Processing.java:191-219`); collects fault-took-effect evidence and the
  failing task trace (`getFirstTaskFailureOpt`) for stuck-vs-stall classification.
- **Fence + quiescence watcher** (§3.6b) — stops the producer, freezes
  `rows_expected`, then awaits lag=0 sustained W + tasks RUNNING + replication
  queues empty + **DLQ depth 0**, bounded by `T_settle`; reuses the existing
  `settle-*.status` mechanism.
- **Window racer** (§3.4) — state-marker log tail; records `fault_window`.
- **KeeperMap reset** (exactly-once) — `DROP TABLE IF EXISTS <zkDatabase>` on the
  self-hosted target before each exactly-once run; recreated lazily by
  `KeeperStateProvider.init()`. (Stale-state trap, `DESIGN.md` §2.4.)
- **Chaos connector template** — `exactlyOnce`, `zkPath`/`zkDatabase`/empty
  `keeperOnCluster`, pinned `bufferCount=0`/`ignorePartitionsWhenBatching=false`;
  the config-combination validation of §3.2.
- **Quorum-loss injector** (opt-in `--quorum-loss`, §3.7) — kills 2 keeper nodes,
  overriding the ≤1-CH-kill cap; asserts stall-then-recover; tags the artifact
  `chaos_mode='quorum_loss'`.
- **Result-artifact writer** (§11, decision §10.2) — writes the per-run JSON +
  per-round records + raw logs to an output dir and returns them. **No `perf.*`
  insert, no DWH export.**
- **GitHub Actions wiring** — a workflow that provisions/scales, runs the smoke
  gate (and optionally a monkey run), uploads the artifacts, and **fails the job**
  on `integrity_ok=false` or a `run_conclusion` timeout. `CHAOS_CH_VERSION`
  (default `latest`) selects the CH image (decision §10.3).

---

## 9. Acceptance criteria (harness follow-on task)

1. `orchestration/lib_bench.sh` exists; `run_pair.sh` sources it and still passes
   its existing tests (pure refactor).
2. `chaos/ch-cluster.yaml` deploys a 3-replica CH + 3-node keeper quorum on
   `bench-ng`; keeper quorum forms; `keeper_map_path_prefix` present; a
   smoke `CREATE/DROP` of a `KeeperMap` table succeeds.
3. `chaos_run.sh` runs the **smoke gate** (§3.5, 4 runs) green: both modes ×
   {C1, C4} at 50 %, each `integrity_ok`, `duplicate_rows=0`,
   `rows_delivered==rows_expected`, `unique_delivered==SOURCE_UNIQUE_EXPECTED`.
4. A **monkey run** (both modes, ≥1 seed, ≥10 rounds) completes with per-round
   fault records and a final PASS; a replay with the same seed reproduces the
   fault sequence/timings. The **recovery gate** (§3.6a) is exercised — a run
   whose gate never clears within `T_recover` concludes as
   `t_recover_timeout` (not a hang), distinguishing a stuck task from an infra
   stall; and the **fence + quiescence** (§3.6b) — including **DLQ depth = 0** —
   are asserted before the oracle runs.
5. Dup-dataset fault-free PASS **and** negative control (deliberate
   double-delivery → `duplicate_rows>0` → FAIL) both behave as designed.
6. CHECK_ERROR vs MISMATCH + fault-took-effect hardening in `check_integrity.py`
   is fixture-tested fail-then-pass (incl. the replica-not-caught-up transient →
   `integrity_unverified`, never MISMATCH).
7. Chaos runs carry `connector='kafka-connect-chaos'`, `environment_class='self_hosted'`,
   and are absent from every benchmark trend/band/ratio.
8. Scale-down verifies both nodegroups at 0 **and** the CH StatefulSet gone.

### Review / fixture standards
- **Fail-then-pass** for every oracle assertion and verdict branch.
- Digest-pinning is law; `chaos_run.sh` rejects bare tags like `run_pair.sh`.
- Fail loud, classify honestly, always tear down.

---

## 10. Resolved decisions (principal, 2026-07-16)

All eight questions were resolved with the principal before implementation. Each
decision and its structural consequence:

1. **`bench-ng` capacity** → **bump `bench-ng` max to 5.** The 3 CH+keeper pods
   get headroom alongside broker/registry/producer/poller. Cost bump ~$0.096/hr
   only while scaled. A read-only capacity check runs before the first deploy.
2. **Reporting** → **NO `perf.*` / DWH / Superset.** Metrics need not persist.
   `chaos_run.sh` writes results to **local JSON/artifact files and returns
   them**; the whole thing is ultimately orchestrated by **GitHub Actions**, where
   results are visible as CI artifacts. This removes the metrics-CH dependency,
   the export bridge, and the dashboard entirely (see §4/§6/§8, revised).
3. **CH version** → **`latest` by default, with a manual override** env
   (`CHAOS_CH_VERSION`, default `latest`); recorded in the result artifact.
4. **`tolerateStateMismatch`** → **`false`** for exactly-once chaos runs (throw on
   `PREVIOUS`, surface as `outcome='failed'` — a crash test wants real state bugs
   loud). `Processing.java:252-259`.
5. **Dup-bearing dataset** → **carve a small prefix from full ClickBench `hits`**
   (already contains duplicate `WatchID`s per ECOSYSTEM §8); compute
   `SOURCE_UNIQUE_EXPECTED=U` once and record it in the artifact. No synthesis.
6. **Checker hardening** → **lands under this task**, fixture-tested fail-then-pass
   (CHECK_ERROR-vs-MISMATCH + fault-took-effect + replica-consistency). Keeps the
   oracle self-contained and CI-runnable.
7. **NetworkPolicy faults (`netpol_*`)** → **deferred.** Ship C1–C5 now; leave
   netpol out until the CNI-enforcement change is separately approved.
8. **Quorum-loss** → **include as an opt-in, flagged experiment.** Default keeps
   the ≤1-CH-kill invariant; a `--quorum-loss` mode (§3.7) kills 2 keeper nodes,
   asserting the connector **stalls then recovers with no loss** once quorum
   returns. Flagged distinctly so it never contaminates the core zero-loss PASS.

## 11. Reporting model (revised per decision §10.2 — CI artifacts, no perf.*)

`chaos_run.sh` emits a single **result artifact per run** (JSON) plus per-round
records and the raw logs, written under an output dir and returned/uploaded:

```
chaos-result-<chaos_id>-<mode>-eo<0|1>.json   # one per monkey/smoke run
  { chaos_id, chaos_mode, chaos_seed, chaos_rounds, delivery_mode,
    environment_class:'self_hosted', ch_version,
    rows_expected, rows_delivered, unique_delivered, duplicate_rows, loss,
    dlq_depth, integrity_ok, outcome, run_conclusion,
    rounds:[ {round, fault_type, fault_window, inject_ts, recovery_seconds,
              task_restart_count, insert_errors_during_fault,
              ch_dedup_dropped_blocks, fault_observed:bool} ... ] }
```

The runtime-key vocabulary of §4 is preserved **as JSON fields**, not as a
`perf.runs` Map — every place the spec said "record a `perf.*` row / metric",
read "write the field into this artifact." GitHub Actions publishes the artifact
and fails the job on `integrity_ok=false` or any `run_conclusion` timeout.
```
