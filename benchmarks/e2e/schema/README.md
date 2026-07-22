# ClickBench `hits` Avro schema (Benchmark v2)

`hits.avsc` is the Avro record schema for the ClickBench `hits` dataset (105 columns), used by the
Benchmark v2 producer to publish typed records through Confluent Schema Registry + `AvroConverter`
into the ClickHouse Kafka Connect sink.

**Source of truth for names / types / order:**
`spark-clickhouse-connector/benchmarks/sql/clickbench/02_create_hits.sql`. The Avro field order
matches the DDL column order exactly.

The whole point of this schema (and its CI test) is the milestone from the plan: *all 105 column
mappings are validated once in CI, not discovered at 200K rows/s.*

## Why the mapping is non-obvious

The sink DESCRIBEs the target ClickHouse table, then serializes each Kafka Connect value into
RowBinary with a **strict Java cast per column width**
(`ClickHouseWriter.doWritePrimitive`): `Int16 -> (Short)`, `Int32 -> (Integer)`,
`Int64 -> (Long)`. If the Connect value class does not match the CH integer width, the insert
throws at runtime. So the Avro type must produce a Connect value of exactly the right width.

Avro has no 16-bit integer. ClickBench has **48** `SMALLINT` (= CH `Int16`) columns. A bare Avro
`int` becomes a Connect `INT32` (a Java `Integer`) and fails the `(Short)` cast. The fix is the
Kafka Connect Avro data convention: annotate the Avro `int` with `"connect.type": "int16"`, which
`AvroConverter.toConnectData` turns into a Connect `INT16` schema and a Java `Short` value. This is
proven by the CI test (a plain `int` would have failed).

The DDL aliases resolve to these CH types (verified with `clickhouse local` DESCRIBE):
`SMALLINT -> Int16`, `INTEGER -> Int32`, `BIGINT -> Int64`, `TEXT`/`VARCHAR(n)`/`CHAR -> String`
(note: `CHAR` is **String**, not `FixedString`), `TIMESTAMP -> DateTime`, `Date -> Date`.

## Type-mapping table

| ClickHouse type (DDL alias) | Count | Avro type | Connect type | Rationale |
|---|---|---|---|---|
| `Int64` (`BIGINT`) | 6 | `long` | INT64 | Direct; sink casts `(Long)`. |
| `Int32` (`INTEGER`) | 19 | `int` | INT32 | Direct; sink casts `(Integer)`. |
| `Int16` (`SMALLINT`) | 48 | `int` + `"connect.type":"int16"` | INT16 | Avro has no 16-bit type; the `connect.type` hint makes AvroConverter yield a Java `Short` so the sink's `(Short)` cast succeeds. A bare `int` fails. |
| `String` (`TEXT`, `VARCHAR(255)`, `CHAR`) | 28 | `string` | STRING | Direct. `CHAR` maps to CH `String`, not `FixedString`. |
| `DateTime` (`TIMESTAMP`) | 3 | `long` (bare epoch **seconds**) | INT64 | **EventTime decision** — see below. |
| `Date` (`Date`) | 1 | `int` + `logicalType:"date"` | Connect Date (INT32, days since epoch) | Sink writes CH `Date` as UInt16 days; a logical-date Connect value (or a bare int of days) is what it accepts. |

Total: 105 columns.

## EventTime (and all DateTime columns) decision

`EventTime`, `ClientEventTime`, `LocalEventTime` are encoded as **epoch-seconds in a bare Avro
`long`** (INT64), NOT as `timestamp-micros` / a Connect `Timestamp` logical type.

Reason: the sink maps a Connect `Timestamp` logical type to CH `DateTime64(3)`
(`Column.resolveBaseType`), and for a CH `DateTime` column it accepts an INT32/INT64 epoch-seconds
value (or a parseable string), writing it via `writeUnsignedInt32(epochSeconds)`
(`ClickHouseWriter.doWriteDates`). A bare epoch-seconds `long` is therefore the correct, lossless,
and highest-throughput encoding for a CH `DateTime` column. This matches the Benchmark v2 plan,
section 6 (`EventTime` row).

Server constraint reflected in the CI edge values: CH `DateTime` is UInt32 seconds, valid range
`[0, 4294967295]` (1970-01-01 .. 2106), so no negative epoch values. CH `Date` is UInt16 days,
range `[0, 65535]`.

## Validation

`src/test/java/com/clickhouse/kafka/connect/benchmark/HitsAvroMappingTest.java` (in the connector
repo's `src/test`) builds edge-value records (zero, negatives on signed widths, width maxima, empty
/ unicode / long strings, epoch boundaries) conforming to `hits.avsc`, runs them through the real
`AvroConverter` (mock schema registry) into `ClickHouseSinkTask.put` against a Testcontainers
ClickHouse whose `hits` table uses the CH types above, then SELECTs every row back and asserts all
105 columns round-trip. CI: `.github/workflows/benchmark-mapping-test.yml`.

Run locally (Docker required):

```
./gradlew test --tests "com.clickhouse.kafka.connect.benchmark.HitsAvroMappingTest"
```

## Regenerating / extending

`hits.avsc` is generated from the DDL. To regenerate after a DDL change, re-run the generator logic
(read `02_create_hits.sql`, map each `NOT NULL` column via the table above, emit fields in DDL
order) and keep `HitsAvroMappingTest.COLUMNS` in sync with the new DDL. The test asserts the schema
has exactly 105 fields and that `COLUMNS` has exactly 105 entries, so a drift on either side fails
CI.
