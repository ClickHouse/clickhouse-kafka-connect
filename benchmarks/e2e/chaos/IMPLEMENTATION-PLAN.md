# Chaos / monkey test (#771) — Phase-1 implementation plan

> **STATUS: PHASE 1 COMPLETE (2026-07-16).** All 14 tasks (T1–T14) landed green.
> T14 verification gate:
> - Combined suite (orchestration/tests, capture/tests, chaos/tests, producer)
>   under the producer venv: **513 passed, 2 skipped** — skips are the live
>   `kubectl --dry-run` (T2) and `actionlint` (T12), both correctly conditional
>   on the tool being present.
> - `bash -n` clean on all 5 shell scripts. `shellcheck`: `chaos_run.sh` +
>   `lib_faults.sh` clean; `lib_bench.sh` + `lib_ch_cluster.sh` carry only the
>   pre-existing-pattern SC2016 (the envsubst allowlist, matching the existing
>   repo convention — intentionally not disabled).
> - `chaos_run.sh --plan` for monkey/smoke/quorum-loss all exit 0;
>   `run_pair.sh --plan` byte-identical to the pre-T1 golden (pure refactor).
> - Diff audit clean: no assertion removed/weakened; no `perf.*` insert reachable
>   from `chaos_run.sh`; banned `count()−uniqExact()` absent from code (only in
>   docstrings that forbid it); no direct `aws`/`eksctl` in `chaos_run.sh`.
> - Interface contracts IC-1…IC-9 all honored as landed.
>
> The remaining work is the **live-EKS downstream phase** (bottom of this doc),
> gated on the principal's approval.

> **LIVE VALIDATION — L1/L2 findings (2026-07-19).** The offline gate was green;
> these are runtime-only defects the local tests structurally could not catch,
> which is why the live gate exists. Both FIXED, each with an added regression
> test; offline suites re-green.
> - **F-L1 (ch-cluster.yaml, StatefulSet)**: `podManagementPolicy` was
>   `OrderedReady`, which **deadlocks the embedded keeper quorum bootstrap** —
>   pod-0 can't reach Ready until a quorum forms, but OrderedReady won't create
>   the peer pods until pod-0 is Ready. Fixed to `Parallel` (headless Service
>   already sets `publishNotReadyAddresses`). Regression test:
>   `test_statefulset_pod_management_is_parallel`. L1 then passed live: 3/3
>   Ready, quorum formed, KeeperMap CREATE/DROP OK, ReplicatedMergeTree DDL
>   applied, teardown verified. **Root-cause note for IC-2**: any k8s
>   translation of a peer-quorum StatefulSet MUST use `Parallel` — the docker
>   fixture has no equivalent knob, so the lockstep test alone could not surface
>   this. IC-2's manifest contract now implies `Parallel` + not-ready-addresses.
> - **F-L2 (render_producer_job.py)**: the renderer overrode only
>   `PARQUET_SOURCE` + `SHARD_COUNT`, **not `TOPIC`** — so the chaos preload
>   wrote to job.yaml's hardcoded `hits` while the run created `hits-chaos`,
>   yielding `_UNKNOWN_TOPIC` and a failed preload. Fixed: added optional
>   `--topic` (omitted ⇒ leaves `hits`, so the pair path is unaffected);
>   `chaos_run.sh` now passes `--topic "${TOPIC}"`. Regression test:
>   `test_render_topic_override_and_default`. This tightens IC-3's identity
>   contract: **every chaos-distinct name (topic, group, table, connector) must
>   be threaded all the way into the rendered producer/connector artifacts** —
>   a name that lives only in `chaos_run.sh` env but not in a template override
>   is a silent cross-wire to the pair defaults.
>
> Two follow-ups from an L2 operational incident (a killed background run leaked
> nodes) are folded in as **T15 (`--reap`)** and **F-L2b (smoke preload
> timeout)** below; see the DOWNSTREAM note and the advice recorded there.

**Authority**: `chaos/DESIGN-771-eks-selfhosted.md` (THE SPEC). All §-references
below are to that document unless prefixed `ECOSYSTEM`.

**Phase-1 boundary (hard)**: local code, manifests, and tests ONLY. Nothing in
any task below may contact AWS/EKS, ClickHouse Cloud, or spend money. Every
acceptance check runs on the operator's machine: `pytest`, `bash -n`,
`shellcheck`, `--plan` dry-runs, `kubectl --dry-run=client --validate=false` /
kubeconform, PATH-stubbed `kubectl`. The live EKS deploy (smoke gate + monkey
runs, spec §9 acceptance 2–5, 7–8 live halves) is a **downstream phase, gated
on the principal's approval** — it is deliberately NOT in this DAG.

**Method (every task)**: TDD — tests written first and observed RED, then the
implementation turns them GREEN. Every oracle assertion and verdict branch has
a fail-then-pass fixture pair. `shellcheck` on every new/modified `.sh`.
Follow existing repo patterns: pytest-driven bash tests
(`orchestration/tests/test_orchestration.py` style: `subprocess` + `source`,
source-text assertions, `--plan` goldens), python scripts mirroring
`capture/*.py` conventions (env contract in the docstring, stdout = machine
output, stderr = logging).

**Worker ground rules**:
- DRY; digest-pinning is law (see IC-8 for the one scoped CH-image exception).
- Existing tests may be modified ONLY under rule T1-R (below). Never weaken or
  delete an assertion.
- All 8 spec §10 decisions are final. Do not reopen them.

---

## Interface contracts (pinned here so tasks can build concurrently)

### IC-1 `orchestration/lib_bench.sh` — exported function list (spec §8)

Sourced by both `run_pair.sh` and `chaos_run.sh`. Bash, `set -uo pipefail`
compatible, **no top-level side effects beyond variable defaults** (sourcing
must never execute a phase). Functions moved verbatim from `run_pair.sh`
(behavior-identical; the log prefix becomes `${BENCH_LOG_PREFIX:-run_pair}`):

| Group | Functions |
|---|---|
| logging | `log`, `warn`, `die` |
| digest-pin family | `_ref_is_digest`, `resolve_tag_to_digest`, `validate_image_ref`, `_ecr_newer_push_exists`, `check_image_provenance` |
| preload | `phase_preload` (topic name/partitions via existing `TOPIC`/`EXPECTED_PARTITIONS` vars), `broker_topic_row_count`, `producer_rows_sent_sum` |
| poller host | `phase_poller_host`, `teardown_poller_pod`, `run_poller_sample` |
| Connect family | `apply_secret_and_metrics`, `connect_pod`, `deploy_connect`, `delete_connect`, `deploy_connector`, `delete_connector`, `wait_tasks_running` |
| scale/teardown | `phase_scale_up`, `phase_teardown_topic`, `phase_scale_down` |
| runtime echo | `build_runtime_json` (parameterized, see below) |
| failure hook | `fail_run` |

**NOT moved** (pair-only, decision §10.2): `finalize_and_insert_metrics`,
`capture_and_record`, `ingest_failed`, `ingest_fail_reason`,
`compute_cpu_gate_t0`, `phase_pair_cost`, `resolve_arm_order`, `print_plan`,
`cleanup_trap`, `main`, the `perf.*`/export path. Path constants
(`SCRIPT_DIR`, `NS`, `TOPIC`, `CONNECT_NAME`, `CONNECTOR_NAME`, `CONNECT_REST`,
`POLLER_POD`, `BROKER_POD`, `EXPECTED_*`, timeouts) move to lib_bench.sh as
overridable `${VAR:-default}` so chaos can retarget topic/group/table/connector
names without touching pair behavior.

`build_runtime_json` parameterization: new optional env
`RUNTIME_EXTRA_KEYS_JSON` — a flat JSON object of string keys merged into the
map **after** the mandatory-key check (chaos passes the §4 vocabulary:
`connector`, `chaos_id`, `chaos_mode`, `chaos_seed`, `fault_type`, …). Empty ⇒
byte-identical pair output. Collision with a built-in key ⇒ hard error.

**T1-R (test-modification rule)**: tests that assert on *source text* currently
read `run_pair.sh` alone. They may be updated in exactly one way: read the
**concatenation of `run_pair.sh` + `lib_bench.sh`** (one shared helper/constant
change). Assertion predicates themselves are untouched. Tests that `source`
run_pair.sh keep working automatically (run_pair.sh sources lib_bench.sh).

### IC-2 `chaos/ch-cluster.yaml` resource names

Namespace `kafka-bench`. StatefulSet `ch-chaos` (3 replicas, pods
`ch-chaos-{0,1,2}`, nodeSelector `role=bench`), headless Service
`ch-chaos-headless` (governing the STS; stable DNS
`ch-chaos-N.ch-chaos-headless.kafka-bench.svc`), client Service `ch-chaos`
(round-robin over ready pods, ports 8123/9000). ConfigMap `ch-chaos-config`
(`config.xml`, `users.xml`). Image placeholder `${CHAOS_CH_IMAGE}` (IC-8).
`TARGET_CH_HOST=ch-chaos.kafka-bench.svc`, `TARGET_CH_PORT=8123`, no TLS
(in-cluster). PVC template `data` (gp3-bench, small — chaos dataset is ~2M
rows). Keeper client port 9181, raft 9234 on every pod (embedded quorum,
mirrors the fixture).

### IC-3 `chaos_run.sh` CLI + exit codes

```
orchestration/chaos_run.sh
  --mode {smoke|monkey}            (required)
  --seed <int>                     (monkey; required for monkey, logged, replay key)
  --rounds <int>                   (monkey; default 20)
  --exactly-once {0|1}             (required; one run = one delivery mode)
  --t-min <s> --t-max <s>          (monkey inter-fault interval; default 30/180)
  --faults C1,C2,...               (default C1,C2,C3,C4,C5)
  --quorum-loss                    (opt-in §3.7; forces --exactly-once 1)
  --aggressive                     (opt-in §3.3 overlap profile)
  --watch-cell                     (opt-in §3.2 ignorePartitionsWhenBatching WATCH cell; at-least-once only)
  --out <dir>                      (artifact dir; default orchestration/artifacts/chaos)
  --plan | -n                      (print the phase plan, execute NOTHING)
  --allow-tag                      (local-hacking escape hatch, as run_pair.sh)
```
Env: `ARM_IMAGE` (connect image, digest), `PRODUCER_IMAGE` (digest),
`CHAOS_CH_VERSION` (default `latest`, IC-8), `PARQUET_SOURCE`,
`SOURCE_UNIQUE_EXPECTED` (smoke; monkey derives it, IC-7),
`T_RECOVER` (default 600), `T_SETTLE` (default 900), `Q_SECONDS` (default 30),
`W_SECONDS` (default 60), `STREAM_RATE` (rows/s, default 5000).

Identity (§4): topic `hits-chaos` (3 partitions), DLQ topic `hits-chaos-dlq`,
table `clickbench.hits_chaos`, consumer group `ch-sink-chaos-eo<0|1>`,
connector CR `chaos-clickhouse-sink`, KafkaConnect CR reuses `bench-connect`
(same worker template), `zkPath=/kafka-connect-chaos`,
`zkDatabase=connect_state_chaos`, `connector='kafka-connect-chaos'`,
`environment_class='self_hosted'`.

Exit codes (consumed by the workflow, T12): `0` PASS (integrity_ok, quiesced);
`1` FAIL (MISMATCH, or stuck-connector `t_recover_timeout`); `3` UNVERIFIED
(CHECK_ERROR / fault-not-observed / infra-stall / `t_settle_timeout`). The
artifact is written in **all** cases before exit.

### IC-4 per-round record + gate exit codes

Round records are JSONL lines appended to `<out>/rounds-<chaos_id>.jsonl`:
```json
{"round": 3, "fault_type": "C4", "fault_window": "W2", "inject_ts": "…Z",
 "recovery_seconds": 41.2, "task_restart_count": 1,
 "insert_errors_during_fault": 2, "ch_dedup_dropped_blocks": 1,
 "fault_observed": true, "evidence": {"kind": "pod_recreated", "detail": "…"}}
```
`chaos/gates.py` subcommands and exit codes:
`recovery` → `0` recovered, `21` t_recover_timeout **stuck** (a task pinned
FAILED; trace captured to stderr/JSON), `22` t_recover_timeout **infra stall**;
`quiescence` → `0` quiesced, `23` t_settle_timeout;
`drain-progress --target-pct 50` → `0` when reached (smoke-gate trigger).
All probes (Connect REST status, lag, pod readiness, replication queue, DLQ
depth) are injected as `--probe-*-cmd` argument strings so tests substitute
stubs; the decision logic is pure functions over probe samples.

### IC-5 `chaos/schedule.py` (seeded monkey schedule)

`python3 chaos/schedule.py --seed S --rounds R --t-min a --t-max b --faults C1,..`
→ stdout JSON `{"seed": S, "rounds": [{"round": 1, "wait_seconds": 47.3,
"fault_type": "C1", "target_window": "W2"}, …]}`. Deterministic: identical
input ⇒ byte-identical output (uses `random.Random(seed)` only). Runtime
concurrency caps (§3.3) are enforced by **delaying** a scheduled fault until
the cap clears — never re-rolling or skipping — so the seeded *sequence* is
preserved and only realized timings drift (recorded per round).

### IC-6 oracle: `check_integrity.py --direct` + `integrity_math.chaos_verdict`

`integrity_math.chaos_verdict(rows_delivered, rows_expected, unique_delivered,
unique_expected, dlq_depth, fault_observed) -> ChaosVerdict` — pure, no I/O.
Verdicts: `PASS` | `MISMATCH` (loss, duplicate_rows≠0, uniqueness violation, or
`dlq_depth>0` §3.6b) | `UNVERIFIED_FAULT_NOT_OBSERVED` (clean integrity but no
fault effect, §5). Formula law: `duplicate_rows = rows_delivered −
rows_expected`; `count()−uniqExact()` on the target is BANNED (asserted by a
test that greps the diff for the banned family).

`check_integrity.py --direct` (existing perf.metrics read-back mode untouched):
reads the target directly — `SELECT count(), uniqExact(WatchID) FROM
{db}.{table} SETTINGS select_sequential_consistency=1` — with the existing
retry/backoff envelope; a transient undercount that resolves on re-read is
CHECK_ERROR territory, never MISMATCH (replica-consistency rule §5). Inputs
via env: `TARGET_CH_*`, `CH_DATABASE`, `CH_TABLE`, `ROWS_EXPECTED`,
`SOURCE_UNIQUE_EXPECTED`, `DLQ_DEPTH`, `FAULT_OBSERVED` (`0|1`). Emits one JSON
object on stdout (`{rows_delivered, rows_expected, unique_delivered,
unique_expected, duplicate_rows, loss, dlq_depth, verdict, reason}`) consumed
by the artifact writer. Exit codes preserved: `0` PASS, `1` MISMATCH, `3`
CHECK_ERROR/unverified (reason distinguishes the classes).

### IC-7 producer streaming mode + uniqueness derivation

`producer.py --stream --rate-limit N` : long-lived, single-pass, bounded-rate
(token bucket) production of the staged prefix in the existing deterministic
file/row order. **Never wraps**: exhausting the dataset before the fence is a
loud producer-side error (the harness sizes the dataset ≥ rate × max run
duration). SIGTERM = the fence signal: stop reading, `flush()` all in-flight
(idempotence on, as today), then print the final summary JSON — now including
`"unique_sent"` (exact count of distinct WatchIDs acked). `rows_expected`
remains offsets-derived (ECOSYSTEM §5); `SOURCE_UNIQUE_EXPECTED` for a monkey
run := producer `unique_sent`, accepted ONLY after the existing
`rows_sent == Σ offsets` cross-check passes (idempotence makes the acked set
identical to the broker set); cross-check failure ⇒ run `UNVERIFIED`. One-shot
mode (no `--stream`) is byte-identical to today.

### IC-8 CH image rule (scoped exception to the digest law)

`CHAOS_CH_VERSION` (decision §10.3, default `latest`) renders
`CHAOS_CH_IMAGE=clickhouse/clickhouse-server:<version>` into ch-cluster.yaml.
The digest law binds the **SUT and harness images** (`ARM_IMAGE`,
`PRODUCER_IMAGE` — validated by `validate_image_ref` exactly as run_pair.sh);
the chaos *target* CH is version-selected by design. Post-deploy,
`chaos_run.sh` reads the **resolved digest** back from the running pods
(`.status.containerStatuses[0].imageID`) and records it in the artifact as
`ch_version` + `ch_image_digest` — deployed truth, not template intent.

### IC-9 result artifact (§11, verbatim)

`chaos-result-<chaos_id>-<mode>-eo<0|1>.json`:
```json
{ "chaos_id": "…", "chaos_mode": "monkey|smoke|quorum_loss", "chaos_seed": "…",
  "chaos_rounds": "…", "rounds_completed": "…",
  "delivery_mode": "at_least_once|exactly_once", "exactly_once": "0|1",
  "connector": "kafka-connect-chaos", "environment_class": "self_hosted",
  "ignore_partitions_when_batching": "0|1",
  "ch_version": "…", "ch_image_digest": "…",
  "arm_image": "…", "producer_image": "…",
  "rows_expected": 0, "rows_delivered": 0, "unique_delivered": 0,
  "unique_expected": 0, "duplicate_rows": 0, "loss": 0, "dlq_depth": 0,
  "integrity_ok": false, "verdict": "PASS|MISMATCH|…", "outcome": "…",
  "run_conclusion": "quiesced|t_recover_timeout|t_settle_timeout",
  "rounds": [ { "round": 1, "fault_type": "C1", "fault_window": "W1",
                "inject_ts": "…", "recovery_seconds": 0.0,
                "task_restart_count": 0, "insert_errors_during_fault": 0,
                "ch_dedup_dropped_blocks": 0, "fault_observed": true } ] }
```
All §4 runtime keys appear as fields (decision §10.2 — no `perf.*`, no DWH).
Writer hard-fails on any missing mandatory field.

---

## Task DAG

### T1 — `lib_bench.sh` extraction (pure refactor)

- **Files**: NEW `benchmarks/e2e/orchestration/lib_bench.sh`; MOD
  `benchmarks/e2e/orchestration/run_pair.sh`; MOD (rule T1-R only)
  `benchmarks/e2e/orchestration/tests/test_orchestration.py`; NEW
  `benchmarks/e2e/orchestration/tests/test_lib_bench.py`.
- **What**: implements spec §8 factoring. Move the IC-1 function set +
  overridable constants into `lib_bench.sh`; `run_pair.sh` sources it first
  thing and keeps everything pair-specific. `build_runtime_json` gains
  `RUNTIME_EXTRA_KEYS_JSON` (IC-1). Behavior-identical: capture a golden of
  `bash run_pair.sh --plan` BEFORE the refactor and assert byte-equality after.
- **Order of work (TDD)**: (1) write `test_lib_bench.py` — lib sources cleanly
  in a bare bash (`set -u`), defines every IC-1 function, sourcing executes no
  phase (stub kubectl on PATH, assert zero invocations), `run_pair.sh` sources
  it, `RUNTIME_EXTRA_KEYS_JSON` merge + collision rejection + empty ⇒
  byte-identical map — observe RED; (2) extract; (3) all green.
- **Depends**: none. **Concurrent**: yes (Wave 1).
- **Acceptance**:
  `python3 -m pytest benchmarks/e2e/orchestration/tests/ -q` fully green;
  `bash -n run_pair.sh lib_bench.sh`; `shellcheck` both;
  `bash run_pair.sh --plan` output identical to the pre-refactor golden;
  `git diff` of test_orchestration.py shows only the T1-R source-location
  change.
- **Interface**: IC-1 (consumed by T8, T9, T10, T11).

### T2 — `chaos/ch-cluster.yaml` (self-hosted CH cluster manifests)

- **Files**: NEW `benchmarks/e2e/chaos/ch-cluster.yaml`; NEW
  `benchmarks/e2e/chaos/tests/test_ch_cluster_yaml.py`; NEW
  `benchmarks/e2e/chaos/tests/__init__.py` (+ conftest if needed).
- **What**: spec §2. Translate
  `src/testFixtures/docker/clickhouse/cluster/{docker-compose.yml,config.xml,users.xml}`
  to k8s per IC-2: 3-replica StatefulSet with **embedded keeper 3-node quorum**
  (raft members = the three headless DNS names, `server_id`/`SHARD_NUM=1`/
  `REPLICA_NUM=<ordinal+1>` derived from the pod hostname ordinal via the
  container command), `one_shard_three_replicas` remote_servers with
  `internal_replication=true` (the survivable-by-design topology; the
  3-shard shape is deliberately NOT carried over), `<keeper_map_path_prefix>`
  identical to the fixture, users.xml carried over, readiness probe = the
  fixture's `/ping`. Includes the `clickbench.hits_chaos` ReplicatedMergeTree
  DDL as a ConfigMap entry (applied by T8's lifecycle, keyed on the fixture
  macros).
- **Lockstep test** (the "derived from the test fixture" law): pytest parses
  BOTH the fixture `config.xml` and the ConfigMap's `config.xml` and asserts
  the invariant elements are equal: `keeper_map_path_prefix`, keeper
  `coordination_settings`, ports (8123/9000/9009/9181/9234), macro key names,
  3 raft members, `internal_replication`. Drift in the fixture breaks this
  test by construction.
- **Depends**: none. **Concurrent**: yes (Wave 1).
- **Acceptance**: `pytest benchmarks/e2e/chaos/tests/test_ch_cluster_yaml.py`;
  `CHAOS_CH_IMAGE=clickhouse/clickhouse-server:latest envsubst < ch-cluster.yaml |
  kubectl apply --dry-run=client --validate=false -f -` succeeds (and
  kubeconform if installed; test skips gracefully when kubectl is absent —
  the pytest structural assertions are the primary gate).
- **Interface**: IC-2 (consumed by T8).

### T3 — oracle hardening (`check_integrity.py` + `integrity_math.py`)

- **Files**: MOD `benchmarks/e2e/capture/integrity_math.py`; MOD
  `benchmarks/e2e/capture/check_integrity.py`; MOD (add-only)
  `benchmarks/e2e/capture/tests/test_integrity_math.py`,
  `benchmarks/e2e/capture/tests/test_check_integrity.py`.
- **What**: spec §5 + decision §10.6, per IC-6. Add `chaos_verdict()` (pure) to
  integrity_math; add `--direct` mode to check_integrity with
  `select_sequential_consistency=1`, the retry/backoff envelope (reuse the
  existing `_attempts`/`_backoffs`), transient-undercount re-read ⇒
  CHECK_ERROR, dlq_depth>0 ⇒ MISMATCH, fault_observed=0 ⇒ unverified-never-PASS,
  JSON emission. Existing read-back mode and its tests untouched.
- **Fixtures (fail-then-pass, every branch)**: exact-match PASS; loss;
  duplicates; uniqueness violation on a dup-bearing source (U<N — proves the
  corrected formula, would false-fail under the banned one); dlq_depth>0 ⇒
  MISMATCH; fault-not-observed ⇒ unverified; raising client ⇒ CHECK_ERROR
  after retries; undercount-then-correct on re-read ⇒ CHECK_ERROR not
  MISMATCH (mocked client returning a lagging first read). Plus the
  negative-control math: double-delivery (delivered = 2×expected) ⇒
  duplicate_rows>0 ⇒ MISMATCH.
- **Depends**: none. **Concurrent**: yes (Wave 1).
- **Acceptance**: `python3 -m pytest benchmarks/e2e/capture/tests/ -q` green
  (old + new).
- **Interface**: IC-6 (consumed by T11, T6).

### T4 — producer continuous streaming mode

- **Files**: MOD `benchmarks/e2e/producer/producer.py`; NEW
  `benchmarks/e2e/producer/test_streaming.py`.
- **What**: spec §8 "Continuous producer" per IC-7: `--stream`,
  `--rate-limit`, token-bucket pacing, single-pass no-wrap, SIGTERM →
  flush → final summary with `unique_sent`; `enable.idempotence` and the
  memory bounds unchanged. Refactor the send loop minimally so pacing and the
  WatchID ledger are pure/injectable.
- **Tests first**: token-bucket math (rate ± tolerance over synthetic clock);
  argparse (`--stream` requires `--rate-limit`; default path unchanged);
  no-wrap exhaustion error; unique_sent ledger correctness incl. duplicate
  WatchIDs; SIGTERM handler drives flush-then-summary (subprocess against a
  tiny local parquet fixture + a stubbed confluent_kafka Producer via
  monkeypatch/injection — no broker). `test_mapping.py` stays green.
- **Depends**: none. **Concurrent**: yes (Wave 1).
- **Acceptance**: `python3 -m pytest benchmarks/e2e/producer/ -q` green.
- **Interface**: IC-7 (consumed by T11's fence and IC-6's unique_expected).

### T5 — seeded monkey schedule (`chaos/schedule.py`)

- **Files**: NEW `benchmarks/e2e/chaos/schedule.py`; NEW
  `benchmarks/e2e/chaos/tests/test_schedule.py`.
- **What**: spec §3.3 determinism per IC-5. Pre-generates the whole run's
  fault sequence + intervals + target windows from `random.Random(seed)`.
  Rejects an empty/unknown fault list; supports `--faults`, quorum-loss and
  aggressive profiles change only the *allowed* set/caps metadata, never the
  RNG stream shape for a given flag combination.
- **Tests first**: same seed ⇒ byte-identical JSON; different seed ⇒ differs;
  intervals within [t_min, t_max]; fault_types ⊆ enabled; round count exact;
  unknown fault rejected loudly.
- **Depends**: none. **Concurrent**: yes (Wave 1).
- **Acceptance**: `pytest benchmarks/e2e/chaos/tests/test_schedule.py`.
- **Interface**: IC-5 (consumed by T11).

### T6 — result-artifact writer (`chaos/write_artifact.py`)

- **Files**: NEW `benchmarks/e2e/chaos/write_artifact.py`; NEW
  `benchmarks/e2e/chaos/tests/test_write_artifact.py`.
- **What**: spec §11 / decision §10.2 per IC-9. Assembles the per-run JSON from
  (a) run-level fields (env/args), (b) the rounds JSONL (IC-4), (c) the oracle
  JSON (IC-6). Hard-fails on missing mandatory fields, malformed round lines,
  or an `outcome`/`run_conclusion` outside the vocabulary. Never writes
  `perf.*`, never exports.
- **Tests first**: golden full artifact; every mandatory-field omission fails
  loudly; rounds ordering preserved; verdict/outcome/run_conclusion vocabulary
  enforcement; filename `chaos-result-<chaos_id>-<mode>-eo<0|1>.json`.
- **Depends**: none (IC-4/IC-6/IC-9 are pinned here). **Concurrent**: Wave 1.
- **Acceptance**: `pytest benchmarks/e2e/chaos/tests/test_write_artifact.py`.
- **Interface**: IC-9 (consumed by T11, T12).

### T7 — chaos connector template + config-combination validation

- **Files**: NEW `benchmarks/e2e/chaos/templates/chaos-connector.json.tmpl`;
  MOD `benchmarks/e2e/orchestration/render_connector.py` (optional args,
  defaults preserve pair behavior); NEW
  `benchmarks/e2e/chaos/tests/test_chaos_connector.py`.
- **What**: spec §3.2 + §8. Template (derived from
  `orchestration/templates/kafkaconnector.json.tmpl`, same `//`-doc-key and
  `${env:CH_*}` conventions): CR name `chaos-clickhouse-sink`, topic
  `hits-chaos`, `topic2TableMap: hits-chaos=hits_chaos`, database `clickbench`,
  plaintext in-cluster port 8123 / `ssl=false` (IC-2 target),
  `exactlyOnce=${EXACTLY_ONCE}`, `zkPath=/kafka-connect-chaos`,
  `zkDatabase=connect_state_chaos`, empty `keeperOnCluster`,
  `tolerateStateMismatch=false` (decision §10.4), pinned `bufferCount=0`,
  `ignorePartitionsWhenBatching=${IPWB}` (default false), and the DLQ pin that
  makes §3.6b's depth meaningful: `errors.tolerance=all`,
  `errors.deadletterqueue.topic.name=hits-chaos-dlq`,
  `errors.deadletterqueue.topic.replication.factor=1`,
  `errors.deadletterqueue.context.headers.enable=true`. Renderer grows
  `--exactly-once`, `--ipwb`, `--template`-driven topic/group and a
  `validate_chaos_config()` pure function enforcing §3.2: `bufferCount>0` +
  exactlyOnce ⇒ reject (mirrors `ClickHouseSinkTask.java:54-62`);
  `ignorePartitionsWhenBatching=true` + `exactlyOnce=true` ⇒ reject (the
  silently-ignored trap, `ClickHouseSinkTask.java:150`); IPWB=true allowed
  only with `--watch-cell` in at-least-once. Worker MUST verify the config-key
  spellings against `sink/ClickHouseSinkConfig.java` before writing the
  template.
- **Tests first**: render both modes and assert the emitted CR config;
  every `validate_chaos_config` rejection fail-then-pass; WATCH-cell carve-out;
  existing render tests in test_orchestration.py green (pair path unchanged);
  cross-check test that the template's pinned keys exist in
  `ClickHouseSinkConfig.java` source text (spelling drift guard).
- **Depends**: none. **Concurrent**: yes (Wave 1).
- **Acceptance**: `pytest benchmarks/e2e/chaos/tests/test_chaos_connector.py`
  + `pytest benchmarks/e2e/orchestration/tests/ -q` green.
- **Interface**: rendered CR + `validate_chaos_config` (consumed by T11).

### T8 — CH-cluster lifecycle lib (`chaos/lib_ch_cluster.sh`)

- **Files**: NEW `benchmarks/e2e/chaos/lib_ch_cluster.sh`; NEW
  `benchmarks/e2e/chaos/tests/test_lib_ch_cluster.py`.
- **What**: spec §8 "CH-cluster lifecycle" + §2. Sources `lib_bench.sh`
  (log/die). Functions: `deploy_ch_cluster` (envsubst `CHAOS_CH_IMAGE`
  per IC-8, apply IC-2 manifests, wait 3/3 Ready, wait keeper quorum — 4lw
  `mntr`/`ruok` via `kubectl exec` until a leader + 2 followers report),
  `teardown_ch_cluster` (delete STS/Services/ConfigMap + PVCs, **verify gone**
  — §4 rule 4), `resolve_keeper_leader` (echoes the pod name whose `mntr`
  reports `zk_server_state leader` — C5's target), `keeper_map_reset` (drops
  the exactly-once KeeperMap state table per `KeeperStateProvider.init()`
  semantics — worker verifies the exact object (`zkDatabase` table) against
  `KeeperStateProvider.java:74-92` — recreated lazily by the connector; §2.4
  stale-state trap), `keeper_map_smoke` (CREATE/DROP a KeeperMap table —
  acceptance §9.2's probe), `apply_chaos_ddl` (hits_chaos ReplicatedMergeTree
  from the T2 ConfigMap), `await_replica_sync` (`system.replicas` queue empty
  and/or `SYSTEM SYNC REPLICA`), `read_ch_image_digest` (IC-8 read-back).
- **Tests first** (PATH-stubbed `kubectl` recording invocations + canned
  outputs): deploy waits for BOTH readiness and quorum before returning;
  teardown deletes PVCs and fails loud if the STS survives; leader resolution
  picks the `leader` pod from mixed `mntr` outputs (and errors on 0 or 2
  leaders); keeper_map_reset issues the drop against the target service; each
  function fail-then-pass on its error path.
- **Depends**: T1 (lib_bench), T2 (manifests). **Concurrent**: with T9, T10
  (Wave 2).
- **Acceptance**: `pytest benchmarks/e2e/chaos/tests/test_lib_ch_cluster.py`;
  `bash -n` + `shellcheck`.
- **Interface**: function names above (consumed by T9 `resolve_keeper_leader`,
  T11).

### T9 — fault injectors + window racer

- **Files**: NEW `benchmarks/e2e/chaos/lib_faults.sh`; NEW
  `benchmarks/e2e/chaos/window_racer.py`; NEW
  `benchmarks/e2e/chaos/tests/test_lib_faults.py`,
  `benchmarks/e2e/chaos/tests/test_window_racer.py`.
- **What**: spec §3.1, §3.4, §3.7. One `inject_<fault>` per primitive, each
  self-cleaning in a nested trap and each emitting a fault-took-effect
  evidence JSON fragment (IC-4 `evidence`):
  C1 `inject_connect_pod_kill` (`kubectl delete pod --grace-period=0 --force`
  on the resolved connect pod; evidence = new pod UID observed);
  C2 `inject_task_restart` (Connect REST POST via the poller pod, as
  `wait_tasks_running` does; evidence = 2xx + a task-state transition or lag
  pause in the next samples); C3 `inject_broker_pod_kill` (evidence = broker
  pod UID change + transient lag-read failure); C4 `inject_ch_node_kill`
  (random `ch-chaos-N`, **refuses while any CH pod is NotReady** — the ≤1-CH
  cap lives at the injector too, not only the loop; evidence = pod recreated +
  `insert_errors_during_fault` from the target's `system.query_log`);
  C5 `inject_ch_keeper_leader_kill` (resolve_keeper_leader → C4 path on it);
  `inject_quorum_loss` (§3.7: kills 2 keeper pods at once — explicitly
  overriding the cap ONLY under `--quorum-loss` — holds, then waits the STS
  restore; asserts the stall was observed while quorum was down).
  `window_racer.py`: tails the connector log stream (a `kubectl logs -f`
  subprocess, injectable as a file/pipe for tests), watches the state-marker
  grammar — exactly-once: consecutive INFO "Write state record:" lines
  (`KeeperStateProvider.java:141`) delimit W1/W2/W3; at-least-once: the DEBUG
  `doInsert` marker or interval-only fallback — fires the supplied kill
  command inside the target window, and reports the window **actually landed**
  (`W1|W2|W3|post_after|na`) — recorded, never assumed.
- **Tests first**: window classification over synthetic log streams (every
  window + the fallback + marker-never-seen ⇒ `na`); racer fires the command
  exactly once; injector cap-refusal (C4 while a stub reports NotReady);
  evidence JSON shape per injector; quorum-loss requires the explicit flag.
- **Depends**: T1; T8's `resolve_keeper_leader` (interface pinned — may build
  against a stub in the same wave). **Concurrent**: with T8, T10 (Wave 2).
- **Acceptance**: `pytest benchmarks/e2e/chaos/tests/test_lib_faults.py
  test_window_racer.py`; `shellcheck`.
- **Interface**: `inject_*` + racer CLI (consumed by T11).

### T10 — recovery gate + fence/quiescence (`chaos/gates.py` + thin bash glue)

- **Files**: NEW `benchmarks/e2e/chaos/gates.py`; NEW
  `benchmarks/e2e/chaos/tests/test_gates.py`.
- **What**: spec §3.6a/§3.6b per IC-4. `recovery`: blocks until ALL signals
  hold sustained for `Q_SECONDS` — tasks RUNNING (Connect REST /status via a
  probe cmd; the failing task's `trace` captured on FAILED — the REST
  equivalent of `getFirstTaskFailureOpt`), killed pod back Ready, lag strictly
  decreasing again (forward progress AFTER the fault, or already 0), no new
  FAILED during Q, and (CH faults only) replica caught up. Bounded by
  `T_RECOVER`; on timeout classifies **stuck** (a task pinned FAILED with a
  trace → exit 21) vs **infra stall** (exit 22). Transient FAILED→RUNNING
  inside Q is tolerated and counted into `task_restart_count` (§3.6a, the
  W1–W3 CONTAINS/ERROR retry shapes). `quiescence`: after the fence, asserts
  lag=0 sustained `W_SECONDS` + tasks RUNNING + replication queues empty +
  **DLQ depth == 0** (probe = `broker_topic_row_count hits-chaos-dlq`),
  bounded by `T_SETTLE` (exit 23 on timeout ⇒ `t_settle_timeout`, never a
  silent pass); optionally chains `capture/wait_for_settle.py` (the existing
  `settle-*.status` mechanism) for merge-settle. `drain-progress`: the smoke
  gate's 50%-drained trigger. All probes injected (IC-4); decision logic =
  pure functions over sample sequences.
- **Tests first** (synthetic probe streams, every branch fail-then-pass):
  recovery happy path; momentary-lag-dip-not-progress rejected; transient
  FAILED→RUNNING tolerated + counted; pinned FAILED ⇒ 21 with trace; no-FAILED
  timeout ⇒ 22; quiescence happy path; dlq>0 blocks quiescence; sustained-W
  reset on a lag blip; T_settle ⇒ 23; drain-progress 50% trigger.
- **Depends**: T1 (probe glue uses lib_bench helpers). **Concurrent**: with
  T8, T9 (Wave 2).
- **Acceptance**: `pytest benchmarks/e2e/chaos/tests/test_gates.py`.
- **Interface**: IC-4 exit codes + emitted round fields (consumed by T11).

### T11 — `chaos_run.sh` assembly (the orchestrator)

- **Files**: NEW `benchmarks/e2e/orchestration/chaos_run.sh`; NEW
  `benchmarks/e2e/orchestration/tests/test_chaos_run.py`; MOD
  `benchmarks/e2e/infra/cluster.yaml` (bench-ng `maxSize: 4 → 5`, decision
  §10.1 — a local edit; the live nodegroup update is the later phase) with the
  in-file "keep in sync" comments updated.
- **What**: spec §3.3, §3.5, §3.6, §8, §9 — the phase machine, per IC-3.
  Sources `lib_bench.sh` + `chaos/lib_ch_cluster.sh` + `chaos/lib_faults.sh`
  with chaos identities (IC-3 names; `BENCH_LOG_PREFIX=chaos_run`,
  `SCALE_UP_NODES=5`). Sequence: arg/flag parsing → image validation (digest
  law on ARM/PRODUCER; IC-8 for CH) → **isolation guard** (refuse to start if
  a `bench-clickhouse-sink` KafkaConnector or a pair's Connect CR exists — §4
  rule 1) → `validate_chaos_config` (T7) → [live phases: scale-up 5+1,
  deploy_ch_cluster + DDL + keeper_map smoke, keeper_map_reset (eo=1), topic +
  DLQ topic, preload (smoke: one-shot; monkey: base preload + `--stream`
  producer as a long-lived Job), poller host, Connect + chaos connector,
  smoke: drain-progress 50% → single fault → recovery → fence… ; monkey:
  schedule.py → per-round inject (window racer for crash-class) → recovery
  gate → record round → …] → fence (SIGTERM the stream producer Job, wait its
  final summary, `rows_expected := broker_topic_row_count hits-chaos`,
  `SOURCE_UNIQUE_EXPECTED` per IC-7) → quiescence → oracle
  (`check_integrity.py --direct`) → `write_artifact.py` → teardown (connector,
  Connect, groups, topics, poller, `teardown_ch_cluster`, scale-down; verify
  nodegroups 0 AND STS gone — §9.8) with a cleanup trap mirroring
  run_pair.sh's. Every gate/oracle outcome maps to IC-3 exit codes; the
  artifact is written on every path (including timeouts) before exit.
  `--plan` prints the full phase plan for the resolved flags and executes
  nothing.
- **Tests first** (pytest, run_pair-test patterns — no cluster): `--plan`
  goldens for smoke/monkey/quorum-loss/aggressive; flag validation
  (`--quorum-loss` forces eo=1; `--watch-cell` rejected with eo=1; missing
  `--seed` in monkey rejected); source-text assertions: isolation guard runs
  before any mutating phase, image validation before phases, teardown verifies
  STS gone + nodegroups 0, artifact written on the timeout paths, no
  `perf.`/export invocation anywhere, digest rejection of bare tags (source
  the script, call the shared validator — proves law inheritance); trap
  present; keeper_map_reset only on eo=1 path.
- **Depends**: T1, T3, T5, T6, T7, T8, T9, T10 (T2, T4 transitively).
  **Concurrent**: with T12 (Wave 3).
- **Acceptance**: `python3 -m pytest benchmarks/e2e/orchestration/tests/ -q`
  green (old + new); `bash -n`; `shellcheck`;
  `bash chaos_run.sh --plan --mode monkey --seed 1 --exactly-once 1` exits 0
  with the golden plan.
- **Interface**: IC-3 CLI + exit codes (consumed by T12).

### T12 — GitHub Actions workflow

- **Files**: NEW `.github/workflows/benchmark-chaos.yml`; NEW test in
  `benchmarks/e2e/orchestration/tests/test_chaos_workflow.py` (or appended to
  test_chaos_run.py).
- **What**: spec §8 "GitHub Actions wiring" + §11. `workflow_dispatch` ONLY
  (never scheduled — cadence is on-demand, §7); inputs: `mode`, `seed`,
  `rounds`, `exactly_once`, `chaos_ch_version` (default `latest`),
  `quorum_loss`. **Same `concurrency:` group as `benchmark-nightly.yml`** so a
  chaos run can never overlap a pair (§4 rule 1, CI-level). Steps: creds →
  chaos_run.sh (smoke gate; optional monkey per inputs) → upload
  `chaos-result-*.json` + rounds JSONL + logs as CI artifacts (`if: always()`)
  → teardown + scale-down as separate `if: always()` steps (run_pair
  convention). Job fails iff chaos_run.sh exits non-zero — which IC-3 defines
  as `integrity_ok=false` or any `run_conclusion` timeout. Phase-1: the
  workflow file exists and validates; it is NOT dispatched.
- **Tests first**: YAML parses; `on:` contains only `workflow_dispatch`;
  concurrency group string-equal to benchmark-nightly.yml's (read both files);
  artifact-upload and teardown steps carry `if: always()`; the run step
  invokes `orchestration/chaos_run.sh`. `actionlint` if installed (skip
  gracefully).
- **CI dependency union (REQUIRED — T14 finding)**: the workflow's install-deps
  step MUST install the full union the combined harness imports —
  `pyarrow`, `confluent_kafka`, `pyyaml`, `clickhouse_connect`, `pytest`. As
  landed, neither `clickhouse_connect` nor `pytest` is in
  `producer/requirements.txt` (T4 added `pytest` locally; T14 installed
  `clickhouse_connect` locally to run the gate), so a CI run that relies on the
  producer image alone fails at pytest collection. Preferred fix: a pinned
  `benchmarks/e2e/chaos/requirements.txt` the workflow installs; acceptable
  alternative: install the explicit union inline. Do NOT assume the producer
  image carries them.
- **Depends**: T11 (IC-3 contract; the cross-check test reads chaos_run.sh).
  **Concurrent**: with T11 in Wave 3 if built against IC-3, with its
  cross-check test finalized once chaos_run.sh lands.
- **Acceptance**: `pytest .../test_chaos_workflow.py`.

### T13 — dup-bearing dataset tooling

- **Files**: NEW `benchmarks/e2e/chaos/prepare_dup_dataset.py`; NEW
  `benchmarks/e2e/chaos/tests/test_prepare_dup_dataset.py`.
- **What**: decision §10.5 + §5. Carves the first K rows (stable file+row
  order, reusing producer.py's `list_dataset_files` ordering — import, don't
  copy) from a parquet source into a staged prefix; computes `rows=N` and
  `unique_watchids=U` exactly; writes the prefix + a manifest JSON
  `{source, rows, unique_watchids, files[]}` that chaos_run.sh feeds as
  `SOURCE_UNIQUE_EXPECTED` for smoke/dup runs. Works on any
  pyarrow-dataset URI; Phase-1 runs it only on local fixtures (the S3 staging
  execution is the live phase).
- **Tests first**: in-test-generated parquet with known duplicate WatchIDs ⇒
  exact N/U in the manifest; K > available rows fails loud; ordering
  stability (two runs ⇒ identical manifest); U < N on the dup fixture (the
  property §5's dup-dataset PASS depends on).
- **Depends**: none (reads producer.py). **Concurrent**: yes (Wave 1).
- **Acceptance**: `pytest benchmarks/e2e/chaos/tests/test_prepare_dup_dataset.py`.

### T14 — integration & verification pass (no new features)

- **Files**: none intended (fixes only, if the sweep finds drift).
- **What**: the whole-tree gate before handing Phase 1 to the principal.
  Runs every suite together, re-verifies the cross-task contracts (IC-1…IC-9)
  against the code as landed, and audits the diff.
- **Depends**: all of T1–T13. **Concurrent**: no (final).
- **Acceptance (all must pass, command-level)**:
  - `python3 -m pytest benchmarks/e2e/orchestration/tests
    benchmarks/e2e/capture/tests benchmarks/e2e/chaos/tests
    benchmarks/e2e/producer -q` — fully green.
  - `bash -n` + `shellcheck` on `run_pair.sh`, `lib_bench.sh`,
    `chaos_run.sh`, `chaos/lib_*.sh`.
  - `bash run_pair.sh --plan` == pre-T1 golden;
    `bash chaos_run.sh --plan --mode smoke --exactly-once 0` and the monkey
    variant exit 0.
  - `envsubst < chaos/ch-cluster.yaml | kubectl apply --dry-run=client
    --validate=false -f -` (skip-if-no-kubectl noted, pytest lockstep test is
    authoritative).
  - Diff audit: no existing test assertion weakened/deleted (git diff of
    `tests/`); no `perf.*` insert or DWH export reachable from chaos_run.sh;
    no phase-1 test path invokes `aws`/`eksctl` or requires a kubeconfig;
    banned formula `count() − uniqExact()` absent from the new code.

### T15 — kill-safe teardown (`--reap` mode) [POST-LIVE, RECOMMENDED]

- **Trigger**: L2 operational incident — a detached background run was SIGKILLed
  mid-teardown, leaking 5+1 nodes + the CH cluster (~50 min, ~$0.5) until torn
  down by hand. `chaos_run.sh`'s cleanup trap fires on normal/`die` exit and
  on INT/TERM, **but not SIGKILL** (nothing can trap SIGKILL) — so a `kill -9`,
  an OOM-kill, or a CI job hard-timeout leaves the cluster scaled up.
- **Files**: MOD `benchmarks/e2e/orchestration/chaos_run.sh`; MOD/NEW its test.
- **What**: a standalone, **idempotent, state-free** teardown path that force-
  removes every chaos resource regardless of run state. Implement as a
  `--reap` flag on `chaos_run.sh` (DRY — it already sources the teardown
  functions; a separate script would duplicate identities). `--reap` parses
  minimal args, sources `lib_bench.sh` + `chaos/lib_ch_cluster.sh`, and runs
  ONLY: delete chaos connector/Connect CRs, chaos consumer group(s), topic +
  DLQ topic, poller pod, `teardown_ch_cluster` (with PVC delete + STS-gone
  verify), `phase_scale_down`, then assert nodegroups at 0 AND STS gone —
  each step `--ignore-not-found`/best-effort so it is safe to run any number of
  times whether or not a run is live. Exits 0 when everything is confirmed
  absent, non-zero (loud) if a resource survives. This is the operator's
  belt-and-suspenders after a kill and the recommended body of the CI
  `if: always()` teardown step (T12), so both the local-detached gap and the
  CI-timeout gap are covered by one code path.
- **Tests first** (PATH-stubbed kubectl): `--reap` runs teardown with zero live
  resources and exits 0 (pure idempotence); `--reap` with a leaked STS present
  issues the delete + scale-down and verifies gone; `--reap` never touches a
  running *pair's* CRs (only chaos-namespaced identities); `--reap --plan`
  prints the reap sequence and executes nothing.
- **Depends**: T11 (shares its teardown functions). **Concurrent**: no (edits
  chaos_run.sh). Land before the first unattended/CI live run.
- **Acceptance**: `pytest .../test_chaos_run.py -q`; `bash -n`; `shellcheck`;
  `bash chaos_run.sh --reap --plan` exits 0 with the golden reap plan.

### F-L2b — smoke preload timeout (fail-fast) [POST-LIVE, RECOMMENDED]

- **Files**: MOD `benchmarks/e2e/orchestration/chaos_run.sh` (+ test).
- **What**: `phase_preload` (via `lib_bench.sh`) inherits the pair's
  `PRODUCER_TIMEOUT` default of **21600 s (6 h)** — correct for a 10M-row pair
  preload, absurd for a chaos smoke run on a ~2M clean prefix, where a failed
  preload should fail in minutes. Introduce a chaos-scoped
  `CHAOS_PRODUCER_TIMEOUT` (default **900 s** for smoke's one-shot base
  preload) that `chaos_run.sh` exports as `PRODUCER_TIMEOUT` before calling
  `phase_preload`; the pair default is untouched. (Monkey's long-lived
  `--stream` producer is not governed by this terminal-wait — it is fenced by
  SIGTERM, IC-7 — so this only affects the smoke/base one-shot preload.)
- **Tests first**: `chaos_run.sh` sets `PRODUCER_TIMEOUT` from
  `CHAOS_PRODUCER_TIMEOUT` (default 900) before preload; overridable; the pair
  path's 21600 default is unchanged (source-text assertion on run_pair.sh).
- **Depends**: T11. **Concurrent**: with T15 possible (adjacent edits;
  coordinate the diff). **Acceptance**: `pytest .../test_chaos_run.py -q`.

---

## Concurrency waves

| Wave | Tasks (parallel) | Notes |
|---|---|---|
| **1** | T1, T2, T3, T4, T5, T6, T7, T13 | 8 independent workers; all contracts pre-pinned (IC-1…IC-9) |
| **2** | T8, T9, T10 | need T1 (+T2 for T8); T9 builds against T8's pinned `resolve_keeper_leader` |
| **3** | T11, T12 | T11 assembles everything; T12 builds against IC-3 concurrently, cross-check test finalized against the landed chaos_run.sh |
| **4** | T14 | serial final gate |

**Critical path**: T1 → T8 → T9 → T11 → T14 (T1 → T10 → T11 is the parallel
near-critical branch). T1 is the single fan-out blocker for Wave 2 — dispatch
it first and largest; everything else in Wave 1 hides behind it.

## Housekeeping loose ends (do before/with the first commit)

- **H1 — gitignore `__pycache__`**: `benchmarks/e2e/chaos/__pycache__/` (and any
  new `tests/__pycache__/`) is untracked. Add `__pycache__/` coverage for the
  chaos tree to `.gitignore` (the repo already ignores it elsewhere; extend the
  pattern). Trivial; not a task, just don't commit the bytecode.
- **H2 — CI dependency list**: land the T12 dependency union as a pinned
  `benchmarks/e2e/chaos/requirements.txt` (see T12) so both CI and any operator
  reproduce the T14 gate with one `pip install -r`.

---

## DOWNSTREAM — live-EKS phase (gated on the principal's approval)

Everything below spends money and touches AWS/EKS/CH. It runs ONLY after the
principal approves. Ordered so each step is gated by the cheapest thing that
could invalidate the next. Costs from spec §7 (fully-scaled ≈ $0.58–0.68/hr;
a 20-round monkey run ≈ $0.30–0.50).

**Recommended order:**

| # | Step | Spec | Gate to clear before proceeding | Est. cost |
|---|---|---|---|---|
| L0 | **Read-only capacity check** + bench-ng max 4→5 nodegroup update (eksctl) — the T11 `cluster.yaml` edit is already local; apply it live | §10.1 | Capacity probe confirms 5×m6i.large schedulable; no live workload disturbed | ~$0 (control-plane only until scale-up) |
| L1 | **CH cluster first deploy + keeper_map smoke** — scale up, `deploy_ch_cluster`, wait 3/3 Ready + quorum, `keeper_map_smoke` CREATE/DROP, `apply_chaos_ddl`, then `teardown_ch_cluster` + scale-down | §2, §9.2 | 3-replica STS Ready, keeper quorum forms, KeeperMap table CREATE/DROP succeeds, teardown verifies STS gone + nodegroups 0 | ~$0.10–0.20 (short) |
| L2 | **4-run smoke gate** — {C1, C4}×{eo=0, eo=1} at 50% drained on the clean dataset; each must be `integrity_ok`, `duplicate_rows=0`, `rows_delivered==rows_expected`, `unique_delivered==SOURCE_UNIQUE_EXPECTED` | §3.5, §9.3 | All 4 green; proves both state providers, a Connect-side + a CH-side fault, the whole oracle path end-to-end | ~$0.50–1.00 |

↑ **L0–L2 are the MINIMAL FIRST LIVE MILESTONE.** They prove the self-hosted
target, keeper quorum, both delivery modes, and the full oracle at the lowest
spend. Stop and report here before the longer runs.

| # | Step | Spec | Gate | Est. cost |
|---|---|---|---|---|
| L3 | **Dup-dataset S3 staging + live negative control** — run `prepare_dup_dataset.py` against real ClickBench `hits` to stage a U\<N prefix to S3; prove the dup-dataset PASSes fault-free AND the deliberate double-delivery negative control FAILs (`duplicate_rows>0`) | §5, §9.5, §10.5 | Dup dataset PASS + negative control FAIL both as designed — the oracle-can-fail proof, now live | ~$0.30 |
| L4 | **Monkey runs, both modes + seed replay** — ≥1 seed, ≥10 rounds, eo=0 and eo=1; then replay a seed and confirm the fault sequence/timings reproduce; recovery gate (`t_recover_timeout` path) and fence+quiescence (incl. DLQ depth 0) both exercised | §3.3, §3.6, §9.4 | Both modes reach a final PASS with per-round records; a same-seed replay reproduces the sequence; a gate timeout concludes (never hangs) | ~$0.60–1.00 |
| L5 | **Quorum-loss experiment** (opt-in `--quorum-loss`, eo=1 only) — kill 2 keeper nodes, assert stall-while-down then zero-loss/zero-dup recovery once quorum returns; artifact tagged `chaos_mode='quorum_loss'` | §3.7, §10.8 | Stall observed during quorum loss; standard oracle PASS at quiescence after restore; result filterable, never averaged into the default runs | ~$0.30 |
| L6 | **Workflow dispatch enablement** — flip `benchmark-chaos.yml` from validate-only to actually dispatchable; first real `workflow_dispatch` runs the smoke gate in CI, uploads artifacts, fails the job on `integrity_ok=false` or a `run_conclusion` timeout | §8, §11 | A green CI smoke run with published artifacts; confirmed same-concurrency-group lockout vs. a nightly pair | ~$0.50/dispatch |

**Later-milestone summary**: L3 → L4 → L5 → L6, run after L0–L2 pass. L5 and L6
are independent of each other and may be reordered per the principal's
priority (L6 = "make it routine", L5 = "extra durability evidence"). None is a
prerequisite for the core zero-loss claim, which L2+L4 already establish.

**Kill-safety prerequisite (from the L2 incident)**: land **T15 (`--reap`)** and
**F-L2b (smoke preload timeout)** before any *unattended* live run — mandatory
before L6 (CI `workflow_dispatch`, where a job hard-timeout SIGKILLs the run)
and strongly recommended before L4 (long monkey runs launched detached). The
CI `if: always()` teardown step (T12) should call `chaos_run.sh --reap` so the
CI-kill path and the local-detached path share one idempotent teardown.

**Deferred beyond this phase** (spec §10.7, unchanged): NetworkPolicy faults
(`netpol_*`) — blocked on the CNI-enforcement substrate change; ship C1–C5
first.
