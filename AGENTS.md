# AGENTS.md — ClickHouse Kafka Connect

This file provides guidance for AI coding agents (Claude Code and others) working in this repository.

## Project Overview

`clickhouse-kafka-connect` is the official Apache Kafka Connect **sink connector** for ClickHouse. It consumes records from Kafka topics and writes them to ClickHouse tables with support for **exactly-once delivery semantics**.

Key design goals:
- Independent of ClickHouse internals — runs as a standard Kafka Connect worker
- Push-based — ClickHouse never connects to Kafka
- Supports all ClickHouse types (including complex types: Array, Map, Tuple)
- Exactly-once delivery via a state machine + ClickHouse's native block deduplication

Full design rationale: [`docs/DESIGN.md`](./docs/DESIGN.md)

---

## Build & Test

**Prerequisites**: OpenJDK 17+, Docker Desktop (for integration tests)

```bash
# Compile and run unit tests (no external dependencies)
./gradlew clean test

# Run integration tests (requires a running ClickHouse instance)
./gradlew clean integrationTest \
  -Dclickhouse.host=<HOST> \
  -Dclickhouse.port=<PORT> \
  -Dclickhouse.password=<PASSWORD>

# Apply code formatting (required before submitting a PR)
./gradlew spotlessApply

# Build the distributable JAR
./gradlew createConfluentArchive
# Output: /build/confluent/
```

Formatting is enforced via the **Spotless** plugin (Google Java Style). Always run `spotlessApply` after making changes.

---

## Architecture

### Data Flow

```
Kafka → ClickHouseSinkTask → ProxySinkTask → Processing (state machine) → ClickHouseWriter → ClickHouse
```

| Class | Role |
|-------|------|
| `ClickHouseSinkConnector` | Kafka Connect entry point; validates config, creates tasks |
| `ClickHouseSinkTask` | Implements `SinkTask`; optional record buffering; delegates to `ProxySinkTask` |
| `ProxySinkTask` | Wires together state provider, writer, and state machine; implements `put()` |
| `Processing` | Executes the per-topic/partition state machine; decides what to insert or skip |
| `ClickHouseWriter` | Converts records to ClickHouse binary format; executes INSERT statements |
| `StateProvider` | Persists offset state; two implementations: `KeeperStateProvider` (production) and `InMemoryState` (tests only) |

### Exactly-Once Semantics

The connector achieves exactly-once delivery by combining two mechanisms:

1. **ClickHouse block deduplication** (`replicated_deduplication_window`): identical blocks inserted within the dedup window are silently ignored by ClickHouse.
2. **Connector-side state machine**: for each topic/partition, the connector records the offset range (`minOffset`/`maxOffset`) and two flags (`BEFORE` insert, `AFTER` successful insert) in **ClickHouse Keeper** (via the `KeeperMap` table engine). On retry, the state machine ensures identical batches are always reconstructed.

This means:
- `BEFORE` flag set → insert may or may not have reached ClickHouse → safe to retry (dedup handles it)
- `AFTER` flag set → insert confirmed → overlapping data is dropped

**Do not break this invariant.** Changes to `Processing.java` or `KeeperStateProvider.java` must preserve the guarantee that on any failure and restart, the batch sent to ClickHouse is byte-for-byte identical to the previous attempt.

### State Machine States

| State | Meaning |
|-------|---------|
| `NEW` | New data past the previous max offset — normal case, insert |
| `SAME` | Same offset range as before — retry if `BEFORE`, drop if `AFTER` |
| `OVERLAPPING` | New batch starts at same min but extends further — split and handle each chunk |
| `CONTAINS` | New batch fully contained within previous — safe to skip if `AFTER`, DLQ if `BEFORE` |
| `PREVIOUS` | New batch is older data — crop and re-evaluate |
| `ZERO` | Topic deleted/recreated — treat as fresh start |
| `ERROR` | Irrecoverable offset inconsistency (e.g. external offset manipulation) |

### Scaling

Processing is single-threaded per topic/partition, matching Kafka's own offset-tracking granularity. The `KeeperMap` state store allows workers to be rebalanced freely — any worker can pick up any partition and resume from strongly consistent state.

---

## Package Structure

All source code lives under `com.clickhouse.kafka.connect`:

| Package | Contents |
|---------|----------|
| `sink` | `ClickHouseSinkTask`, `ClickHouseSinkConfig`, `ProxySinkTask` |
| `sink.data` | `Record`, `Data`, `SchemaType`, record converters |
| `sink.data.convert` | Per-format converters: schema, schemaless, string, empty |
| `sink.db` | `ClickHouseWriter`, `DBWriter` interface, `ClickHouseHelperClient`, table/column mapping |
| `sink.kafka` | `RangeState`, `RangeContainer`, `OffsetContainer` — offset tracking |
| `sink.processing` | `Processing` — state machine execution |
| `sink.state` | `StateProvider` interface, `StateRecord`, `KeeperStateProvider`, `InMemoryState` |
| `sink.dlq` | `ErrorReporter`, `DuplicateException` |
| `transforms` | `KeyToValue`, `ExtractTopic` — optional Kafka Connect transforms |
| `util` | `Utils`, `QueryIdentifier`, `Mask`, JMX metrics |

---

## Data Format Support

The connector handles four record formats, each with its own converter:

| Format | Converter | Insert format |
|--------|-----------|---------------|
| Avro / Protobuf (with Schema Registry) | `SchemaRecordConvertor` | RowBinary |
| Schemaless JSON | `SchemalessRecordConvertor` | JSON |
| String | `StringRecordConvertor` | JSON |
| Null / empty | `EmptyRecordConvertor` | — |

---

## Coding Conventions

- **Java 17**, Gradle Kotlin DSL (`build.gradle.kts`)
- **Lombok** for boilerplate: use `@Getter`, `@Setter`, `@Accessors` on data classes
- **SLF4J** for logging: use TRACE for per-record detail, DEBUG for batch-level, INFO for lifecycle events
- **JUnit 5** (Jupiter) + **Mockito** for unit tests
- **TestContainers** for integration tests — no manual Docker management needed

---

## Testing Guidelines

### Unit tests (`src/test/`)
- Use `InMemoryState` as the state provider — no ClickHouse instance required
- Use `InMemoryDLQ` (in test fixtures) to capture DLQ output
- Cover all state machine transitions in `ProcessingTest`
- Do not mock `ClickHouseWriter` in state machine tests — use `InMemoryDBWriter`

### Integration tests (`src/integrationTest/`)
- Require a real ClickHouse instance (local Docker or ClickHouse Cloud)
- Test assumptions: username = `default`, database = `default`
- Cloud integration tests are automatically skipped if cloud properties are not set
- Run nightly in CI via GitHub Actions

### Before submitting a PR
1. `./gradlew clean test` passes
2. `./gradlew spotlessApply` has been run (or `spotlessCheck` passes)
3. New functionality has unit test coverage
4. `VERSION` file is bumped and `CHANGELOG.md` is updated

---

## High-Risk Areas

Changes in these areas require extra care and thorough testing:

| File | Risk |
|------|------|
| `sink/processing/Processing.java` | Core state machine — bugs here break exactly-once guarantees |
| `sink/state/provider/KeeperStateProvider.java` | Consistency-critical; any bug may cause data duplication or loss |
| `sink/db/ClickHouseWriter.java` | Binary format inserts; type mapping errors cause data corruption |
| `sink/ClickHouseSinkConfig.java` | Config changes affect all downstream behavior and user deployments |
| `sink/kafka/RangeContainer.java` | Offset range arithmetic — edge cases in overlap detection |

When modifying the state machine or state persistence, verify that the `BEFORE`/`AFTER` flag sequence is preserved under all failure scenarios (crash before insert, crash after insert but before `AFTER` flag, crash during flag write).

---

## What to Avoid

- **Do not use `InMemoryState` in production paths** — it provides no exactly-once guarantees on restart
- **Do not combine `bufferCount`/`bufferFlushTime` with `exactlyOnce=true`** — the connector validates and rejects this at startup
- **Do not store connector state in ClickHouse tables** — latency and consistency issues; use `KeeperMap` only
- **Do not change batch composition after setting `BEFORE`** — the retry must send the identical block for deduplication to work
