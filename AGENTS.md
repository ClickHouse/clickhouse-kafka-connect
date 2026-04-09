# ClickHouse Kafka Connect Development Guide

This file provides guidance for AI coding agents working in this repository.

## Project Overview

`clickhouse-kafka-connect` is the official Apache Kafka Connect **sink connector** for ClickHouse. It is implemented according to the sink connector specification (`SinkConnector`) and writes messages to ClickHouse tables using the official ClickHouse Java client with support for **exactly-once delivery semantics**. 

Key design goals:
- Independent of ClickHouse internals — runs as a standard Kafka Connect worker
- Push-based — ClickHouse never connects to Kafka
- Supports all ClickHouse types (including complex types: Array, Map, Tuple)
  - Converts accurately from `SinkRecord` to ClickHouse data types
- Exactly-once delivery via a state machine + ClickHouse's native block deduplication
- Eager schema validation
- Supports all kafka record formats (JSON, Avro, etc)

Full design rationale: [`docs/DESIGN.md`](./docs/DESIGN.md)

### Package Structure

All source code lives under `src/main/java` in the `com.clickhouse.kafka.connect` package:

| Sub-package           | Contents                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `sink`                | - `ClickHouseSinkTask`: main Kafka Connect `SinkTask` implementation, delegates to `ProxySinkTask` <br/> - `ClickHouseSinkConfig`: defines `ConfigDef` for the connector<br/> - `ProxySinkTask`: defines main start/stop/put logic                                                                                                                                                                                                                         |
| `sink.data`           | - `Record`: in-memory representation of a record read from kafka (`SinkRecord`) + metadata<br/> - `Data`: generic wrapper of data (`java.lang.Object`) with a schema<br/> - `SchemaType`: enum that determines the write path of a `Record`                                                                                                                                                                                                                |
| `sink.data.convert`   | - `RecordConvertor`: abstract class that converts `SinkRecord` (Kafka Connect API) to `Record` (clickhouse connector API)<br/> All other classes in this package are implementations of `RecordConverter` for each `SchemaType`                                                                                                                                                                                                                            |
| `sink.db`             | - `DBWriter`: main interface describing the API for start/stop/insert to ClickHouse<br/> - `ClickHouseWriter`: primary implementation of `DBWriter` <br/> - `InMemoryDBWriter`: test implementation of `DBWriter`<br/>- `TableMappingRefresher`: periodic task to update in-memory mapping between ClickHouse table names and their descriptions (`Table`) in `ClickHouseWriter`                                                                           |
| `sink.db.mapping`     | - `Column`: description of a ClickHouse table column (name + `Type` + metadata) <br/>- `Table`: description of a ClickHouse table (name + database + `List<Column>` + metadata) <br/>- `Type`: enum of ClickHouse data types the connector can serialize                                                                                                                                                                                                   |
| `sink.db.helper`      | Contains utilities for writing to ClickHouse                                                                                                                                                                                                                                                                                                                                                                                                               |
| `sink.dedup`          | Contains offset tracking object (`DeDup`) for different dedup modes (`DeDupStrategy`). **NOTE: This is currently unused**                                                                                                                                                                                                                                                                                                                                  |
| `sink.kafka`          | - `RangeState`: enum describing the state machine of offset ranges for exactly-once implementation <br/>- `TopicPartitionContainer`: wrapper over a topic and a partition<br/>- `RangeContainer`: a `TopicPartitionContainer` with min and max offsets (represents an offset range)<br/>- `OffsetContainer`: a `TopicPartitionContainer` with a single offset                                                                                              |
| `sink.processing`     | - `Processing`: exactly-once implementation (insert records + offset state management). Main logic is in `doLogic`, invoked by `ProxySinkTask`.                                                                                                                                                                                                                                                                                                            |
| `sink.state`          | - `StateProvider`: interface defining state management API<br/>- `BaseStateProviderImpl`: abstract base implementation of `StateProvider` (NOTE: most, if not all, implementations should inherit from this class) <br/>- `State`: enum of the possible states a topic & partition can be in the exactly-once protocol<br/>- `StateRecord`: in-memory wrapper over a State and a topic name. These are written to the state store in `Processing.doLogic`. |
| `sink.state.provider` | Contains implementations of `BaseStateProviderImpl`:<br/>- `InMemoryState`: buffers state in-memory (`Map<String, StateRecord>`), used for non exactly-once mode<br/>- `KeeperStateProvider`: stores state in ClickHouse Keeper, used for exactly-once mode                                                                                                                                                                                                |
| `sink.dlq`            | - `ErrorReporter`: interface that defines how to report errors when records are sent to the DLQ                                                                                                                                                                                                                                                                                                                                                            |
| `transforms`          | Contains implementations of Kafka Connect `Transformation`. **NOTE: these are currently unused**                                                                                                                                                                                                                                                                                                                                                           |
| `util`                | Contains miscellaneous utils                                                                                                                                                                                                                                                                                                                                                                                                                               |

### Key Files

Changes in these areas are considered **high risk** and require extra care and thorough testing:

| File                                           | Risk                                                               |
|------------------------------------------------|--------------------------------------------------------------------|
| `sink/processing/Processing.java`              | Core state machine — bugs here break exactly-once guarantees       |
| `sink/state/provider/KeeperStateProvider.java` | Consistency-critical; any bug may cause data duplication or loss   |
| `sink/db/ClickHouseWriter.java`                | Binary format inserts; type mapping errors cause data corruption   |
| `sink/ClickHouseSinkConfig.java`               | Config changes affect all downstream behavior and user deployments |
| `sink/kafka/RangeContainer.java`               | Offset range arithmetic — edge cases in overlap detection          |

When modifying the state machine or state persistence, verify that the `BEFORE`/`AFTER` flag sequence is preserved under all failure scenarios (crash before insert, crash after insert but before `AFTER` flag, crash during flag write).

---

### Usage

The connector can be configured in the Kafka Connect framework as follows (modify individual fields as necessary):

```json
{
  "name": "clickhouse-connect",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "tasks.max": "1",
    "database": "{{ClickHouse_database}}",
    "hostname": "{{ClickHouse_hostname}}",
    "password": "{{ClickHouse_password}}",
    "port": "{{ClickHouse_port}}",
    "errors.retry.timeout": "60",
    "exactlyOnce": "false",
    "ssl": "true",
    "topics": "{{topics}}",
    "errors.tolerance": "none",
    "errors.deadletterqueue.topic.name": "{{DLQ_topic_name}}",
    "username": "default",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "errors.log.enable": "true",
    "clickhouseSettings": "",
    "topic2TableMap": "",
    "jdbcConnectionProperties": "?ssl=true&sslmode=strict",
    "consumer.override.max.poll.records": "50"
  }
}
```

## Architecture

### Data Flow

```
Kafka → ClickHouseSinkTask → ProxySinkTask → Processing (state machine) → ClickHouseWriter → ClickHouse
```
See the Package Structure section above for descriptions of these components.

### Exactly-Once Semantics

The connector achieves exactly-once delivery by combining two mechanisms:

1. **ClickHouse block deduplication** (`replicated_deduplication_window`): identical blocks inserted within the dedup window are silently ignored by ClickHouse.
2. **Connector-side state machine**: for each topic/partition, the connector records the offset range (`minOffset`/`maxOffset`) and two flags (`BEFORE` insert, `AFTER` successful insert) in **ClickHouse Keeper** (via the `KeeperMap` table engine). On retry, the state machine ensures identical batches are always reconstructed.

This means:
- `BEFORE` flag set → insert may or may not have reached ClickHouse → safe to retry (dedup handles it)
- `AFTER` flag set → insert confirmed → overlapping data is dropped

**Do not break this invariant.** Changes to `Processing.java` or `KeeperStateProvider.java` must preserve the guarantee that on any failure and restart, the batch sent to ClickHouse is byte-for-byte identical to the previous attempt.

### State Machine States

| State         | Meaning                                                                              |
|---------------|--------------------------------------------------------------------------------------|
| `NEW`         | New data past the previous max offset — normal case, insert                          |
| `SAME`        | Same offset range as before — retry if `BEFORE`, drop if `AFTER`                     |
| `OVERLAPPING` | New batch starts at same min but extends further — split and handle each chunk       |
| `CONTAINS`    | New batch fully contained within previous — safe to skip if `AFTER`, DLQ if `BEFORE` |
| `PREVIOUS`    | New batch is older data — crop and re-evaluate                                       |
| `ZERO`        | Topic deleted/recreated — treat as fresh start                                       |
| `ERROR`       | Irrecoverable offset inconsistency (e.g. external offset manipulation)               |

### Scaling

Processing is single-threaded per topic/partition, matching Kafka's own offset-tracking granularity. The `KeeperMap` state store allows workers to be rebalanced freely — any worker can pick up any partition and resume from strongly consistent state.

---

## Data Format Support

The connector handles four record formats, each with its own converter:

| Format                                 | Converter                   | Insert format |
|----------------------------------------|-----------------------------|---------------|
| Avro / Protobuf (with Schema Registry) | `SchemaRecordConvertor`     | RowBinary     |
| Schemaless JSON                        | `SchemalessRecordConvertor` | JSON          |
| String                                 | `StringRecordConvertor`     | JSON          |
| Null / empty                           | `EmptyRecordConvertor`      | —             |

---

## Testing

### Running Tests

**Prerequisites**: 
- OpenJDK 17+
- Docker

```bash
# Compile and run unit tests
./gradlew clean test

# Compile and run individual tests
./gradlew clean test --tests "*xyz"

# Run integration tests (requires cloud credentials to run against ClickHouse Cloud)
./gradlew clean integrationTest \
  -Dclickhouse.host=<YOUR_CH_HOST> \
  -Dclickhouse.port=<YOUR_CH_PORT> \
  -Dclickhouse.password=<YOUR_CH_PASSWORD> \
  -Dclickhouse.cloud.organization=<YOUR_CH_CLOUD_ORG> \
  -Dclickhouse.cloud.apiKey=<YOUR_CH_CLOUD_API_KEY> \
  -Dclickhouse.cloud.secret=<YOUR_CH_CLOUD_SECRET> \
  -Dclickhouse.cloud.serviceId=<YOUR_CH_CLOUD_SERVICE_ID> \
  -Dclickhouse.cloud.host=<YOUR_CH_CLOUD_API_HOST>
```

**Environment variables**:
- CLICKHOUSE_VERSION (default: `latest`)
- CLIENT_VERSION (default: `V2`)

### Writing Tests

##### Unit tests (`src/test/java`)
- Use `InMemoryState` as the state provider if necessary — no ClickHouse instance required
- Use `InMemoryDLQ` (in test fixtures) to capture DLQ output if necessary
- Cover all state machine transitions in `ProcessingTest`
- Do not mock `ClickHouseWriter` in state machine tests — use `InMemoryDBWriter`
- When writing records to ClickHouse, always read back the data and `assertEquals` against the original records
- Follow existing patterns in the code, do not repeat yourself

##### Integration tests (`src/integrationTest/java`)
- Require a real ClickHouse instance (local Docker or ClickHouse Cloud)
- Cloud integration tests are automatically skipped if cloud properties are not set
- When writing records to ClickHouse, always read back the data and `assertEquals` against the original records
- Follow existing patterns in the code, do not repeat yourself

---

## Development/Contribution Rules

### Before submitting a PR
1. `./gradlew clean test` and `./gradlew clean integrationTest` passes (see the Running Tests section above)
2. `./gradlew spotlessApply` has been run (or `spotlessCheck` passes)
3. Any new functionality **must have unit test coverage**, optionally integration test coverage - see the Writing Tests section above
4. `VERSION` file is bumped and `CHANGELOG.md` is updated

Formatting is enforced via the **Spotless** plugin (Google Java Style). Always run `spotlessApply` after making changes.

### What to Avoid
- **Do not use `InMemoryState` in production paths** — it provides no exactly-once guarantees on restart
- **Do not combine `bufferCount`/`bufferFlushTime` with `exactlyOnce=true`** — the connector validates and rejects this at startup
- **Do not store connector state in ClickHouse tables** — latency and consistency issues; use `KeeperMap` only
- **Do not change batch composition after setting `BEFORE`** — the retry must send the identical block for deduplication to work

### Coding Conventions
- **Java 17**, Gradle Kotlin DSL (`build.gradle.kts`)
- **Lombok** for boilerplate: use `@Getter`, `@Setter`, `@Accessors` on data classes
- **SLF4J** for logging: use TRACE for per-record detail, DEBUG for batch-level, INFO for lifecycle events
- **JUnit 5** (Jupiter) + **Mockito** for unit tests

### Correctness & Safety First
- **Protocol fidelity**: Correct serialization/deserialization of ClickHouse types across all supported versions
- **Type mapping**: ClickHouse has 60+ specialized types - ensure correct mapping between Java/Kafka Connect types and ClickHouse types. **No data loss is acceptable.**
- **Thread safety**: all concurrent operations must happen safely
- **Avoid resource leaks**: use try-with-resources statements for all `AutoCloseable` objects

### Stability & Backward Compatibility
- **ClickHouse version support**: Ensure changes do not break against older LTS ClickHouse versions by validating explicitly across versions (run `yq '.jobs.build.strategy.matrix.clickhouse' .github/workflows/tests.yaml` from the repository root for the versions you must develop against)
- **Client version**: Use java client V2 by default
- **Serialization hotpath changes**: Serialization changes require extensive test coverage

### Performance Characteristics
- **Hot paths**: Core code in `Processing.java`, `ClickHouseSinkTask.java`, `ProxySinkTask.java`, and `ClickHouseWriter.java` - avoid unnecessary heap allocations, copies, autoboxing, and per-record processing
- **Streaming**: Maintain streaming behavior, avoid buffering entire responses
- **Connection pooling**: Respect HTTP connection pool behavior, avoid connection leaks

### Code Style
- **DRY**: do not repeat yourself 
- **Google Java Style**: you should write all code in this style
- **Static string constants**: extract String constants into top-level static strings whenever necessary 

### Configuration & Settings
- **Client configuration**: Connection string or `ClickHouseClientSettings` for client-level settings
- **Per-query options**: `QueryOptions` for query-specific settings (QueryId, CustomSettings, Roles, BearerToken)
- **Parameters**: Use `ClickHouseParameterCollection` with `ClickHouseDbParameter` for parameterized queries
- **Feature flags**: Consider adding optional behavior behind connection string settings
- **Useful code comments**: Only write code comments that convey important context or invariants that are not immediately obvious from the code. **Do not just summarize the logic in English.**

### Observability & Diagnostics
- **Error messages**: Must be clear, actionable, include context (server version, client version, etc)

### Security
- **Do not log secrets or data in plaintext**: Includes passwords, API keys, and records
- **Deprecated libraries**: Notice and suggest upgrading deprecated libraries. **DO NOT try to upgrade dependencies without asking the user first**.
- **CVE**: Notice and suggest upgrading dependencies that have open CVE's. **DO NOT try to upgrade dependencies without asking the user first**.

### Testing Discipline
- **Test utilities**: before writing tests, read `ClickHouseTestHelpers.java` to understand existing config and utility patterns.
- **Test matrix**: run `yq '.jobs.build.strategy.matrix.clickhouse' .github/workflows/tests.yaml` from the repository root for the ClickHouse versions you must test against
- **Negative tests**: Error handling, edge cases, concurrency scenarios
- **Existing tests**: Only add new tests, never delete/weaken existing ones
- **Test organization**: See the Writing Tests section above
- **Test naming**: Write descriptive test names that capture what the test validates 

---

## Review

Use the review skill.
