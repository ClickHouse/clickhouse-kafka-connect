# Choosing a Kafka Connect converter

This guide helps you pick between `JsonConverter` and `StringConverter` for the ClickHouse Kafka Connect Sink, and documents the connector settings that go with each path (`value.converter.schemas.enable`, `customInsertFormat`, `insertFormat`).

For Avro / Protobuf (Schema Registry) recipes, see the [public documentation](https://clickhouse.com/docs/en/integrations/kafka/clickhouse-kafka-connect-sink). Design and exactly-once behavior live in [`DESIGN.md`](./DESIGN.md).

## Quick decision guide

| Your Kafka value payload | Recommended converter | Key connector settings |
| --- | --- | --- |
| JSON objects (typical apps / CDC flattened JSON) | `org.apache.kafka.connect.json.JsonConverter` | `value.converter.schemas.enable=false` (usual case) |
| JSON with an embedded Connect schema envelope | `JsonConverter` | `value.converter.schemas.enable=true` |
| Pre-formatted line-oriented text (CSV / TSV / JSONEachRow lines) you want ClickHouse to parse as-is | `org.apache.kafka.connect.storage.StringConverter` | `customInsertFormat=true` **and** `insertFormat` = `CSV`, `TSV`, or `JSON` |
| Avro / Protobuf with Schema Registry | Avro / Protobuf converter | See public docs; uses RowBinary unless `bypassRowBinary=true` |

**Rule of thumb**

- Prefer **JsonConverter (schemas disabled)** for structured JSON that should map to table columns via `JSONEachRow`.
- Prefer **StringConverter + custom insert format** when producers already emit ClickHouse-ready text and you want a lower-overhead passthrough path.
- Do **not** use StringConverter without setting `insertFormat` — the sink fails at runtime (see [Failure modes](#failure-modes)).

## How converters map inside the connector

| Kafka Connect value converter | Internal convertor | ClickHouse insert format |
| --- | --- | --- |
| `JsonConverter` with `schemas.enable=false` | Schemaless JSON path | `JSONEachRow` |
| `JsonConverter` with `schemas.enable=true` (or Avro / Protobuf) | Schema-based path | `RowBinary` / `RowBinaryWithDefaults` by default; `JSONEachRow` when `bypassRowBinary=true` |
| `StringConverter` | String path | `CSV`, `TSV`, or `JSONEachRow` from `insertFormat` |

The sink always writes into an **existing** ClickHouse table. Column names / order must match what the chosen format expects (JSON field names for JSON paths; column order for CSV/TSV).

## JsonConverter

### Schemaless JSON (recommended default)

Use when topic values are plain JSON objects (no Connect `schema` / `payload` wrapper).

```properties
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
```

JSON example:

```json
{
  "name": "clickhouse-connect",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "hostname": "<hostname>",
    "port": "8443",
    "ssl": "true",
    "database": "default",
    "username": "default",
    "password": "<password>",
    "topics": "<topic_name>",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

**Behavior**

- Each record is serialized and inserted with ClickHouse `JSONEachRow`.
- Unknown JSON fields are skipped when `input_format_skip_unknown_fields=1` (set by the connector by default).
- Missing columns use ClickHouse defaults / nullability rules.

**Pros**

- Natural fit for application JSON and most Kafka JSON topics.
- Field names map to columns without relying on CSV/TSV column order.
- Matches the examples in the README, Support Scripts, and public docs.

**Cons / limits**

- Connect-side JSON serialize/deserialize cost on every record (see [Performance](#performance-tradeoffs)).
- Nested JSON may need SMTs (for example `Flatten`) or a matching ClickHouse nested / JSON column design.
- `auto.evolve` requires a Connect schema; schemaless JSON cannot use it.

### JSON with schemas enabled

```properties
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true
```

**When to use**

- Producers emit the Connect schema envelope (`schema` + `payload`).
- You want schema-driven typing and the RowBinary insert path (same family as Avro/Protobuf).

**Failure modes if mis-set**

| Misconfiguration | Symptom | Fix |
| --- | --- | --- |
| Topic has plain JSON but `schemas.enable=true` | Converter / parse errors; connector cannot read values as Connect schema envelopes | Set `value.converter.schemas.enable=false` |
| Topic has schema envelopes but `schemas.enable=false` | Wrong structure inserted, blank/zero columns, or ClickHouse type errors | Enable schemas **or** strip the envelope upstream (SMT / producer change) |
| Field names do not match table columns | Blank/zero values or insert errors | Align names, or use Flatten / `topic2TableMap` as appropriate |

Also ensure worker-level defaults do not override the connector: if the worker forces a different `value.converter` / schema flag, set the converter explicitly on the connector config.

## StringConverter

StringConverter treats each Kafka value as a raw string and sends those bytes to ClickHouse in the format you select. This is the passthrough path for pre-formatted rows.

### Required settings

```properties
value.converter=org.apache.kafka.connect.storage.StringConverter
customInsertFormat=true
insertFormat=<CSV|TSV|JSON>
```

| Setting | Purpose |
| --- | --- |
| `customInsertFormat` | Declares that StringConverter should use a custom ClickHouse insert format (ConfigDef default: `false`). Set to `true` whenever you use StringConverter with this sink. |
| `insertFormat` | One of `CSV`, `TSV`, or `JSON` (case-insensitive). Maps to ClickHouse `CSV`, `TSV`, or `JSONEachRow`. Default / unset behaves as `NONE` and fails at runtime for string records. |

### Choosing `insertFormat`

| `insertFormat` | ClickHouse format | Choose when |
| --- | --- | --- |
| `CSV` | `CSV` | Producers emit CSV rows; column **order** must match the table |
| `TSV` | `TSV` | Same as CSV but tab-separated |
| `JSON` | `JSONEachRow` | Each Kafka message is one JSON object **per line** (not a Connect schema envelope). Useful when you want JSON text passthrough without JsonConverter parsing into Connect structs |

The connector appends a trailing newline for CSV/TSV rows when the payload does not already end with `\n`.

### Config snippets

**CSV**

```json
{
  "name": "clickhouse-connect-string-csv",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "hostname": "<hostname>",
    "port": "8443",
    "ssl": "true",
    "database": "default",
    "username": "default",
    "password": "<password>",
    "topics": "<topic_name>",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "customInsertFormat": "true",
    "insertFormat": "CSV"
  }
}
```

**TSV** — same as above with `"insertFormat": "TSV"`.

**JSONEachRow lines** — same as above with `"insertFormat": "JSON"`.

### Pros

- Lower Connect CPU than JsonConverter when values are already ClickHouse-ready text (no JSON→Connect struct→JSON round trip).
- Fits dump / ETL pipelines that already produce CSV, TSV, or JSONEachRow.
- ClickHouse performs parsing; useful for high-throughput, low-latency ingest when batching is tuned.

### Cons / limits

- **Must** set `customInsertFormat` + a non-`NONE` `insertFormat` or the task throws (see below).
- CSV/TSV are positional: column order and escaping must match the table; renames break silently or fail inserts.
- No Connect schema on the string path — `auto.evolve` is not supported.
- You are responsible for producing valid rows for the chosen ClickHouse format (quotes, escapes, one row per message unless you intentionally batch lines).

## Failure modes

### StringConverter without custom insert format

If string records are ingested while `insertFormat` resolves to `NONE` (the default when unset), `ClickHouseWriter` throws:

```text
using org.apache.kafka.connect.storage.StringConverter, but did not enable.
```

**Fix**

```properties
customInsertFormat=true
insertFormat=JSON
```

(or `CSV` / `TSV` to match the payload). Restart the connector task after updating config.

### Schema flag mismatches (JsonConverter)

- **Schemas enabled unintentionally** on plain JSON topics → converter exceptions before the sink runs. Disable `value.converter.schemas.enable`.
- **Schemas disabled** on schema-envelope topics → malformed inserts / empty columns. Enable schemas or change the producer format.
- Worker defaults differ from connector config → set both `value.converter` and `value.converter.schemas.enable` on the **connector** so they are unambiguous.

### `insertFormat` mismatch

| Situation | Likely result |
| --- | --- |
| CSV payload with `insertFormat=JSON` (or the reverse) | ClickHouse parse errors; with `errors.tolerance=all`, batches go to the DLQ if configured |
| Wrong CSV/TSV column order | Values land in the wrong columns or type conversion errors |
| Multi-line or partial rows in one Kafka message | Truncated / merged rows depending on format rules |

Validate a sample offline with `clickhouse-client` (`FORMAT CSV` / `TSV` / `JSONEachRow`) before pointing the connector at a high-volume topic.

### Related knobs (schema-based path only)

- `bypassRowBinary=true` — force `JSONEachRow` for schema-based data (Avro/Protobuf/JSON-with-schema) when RowBinary is a poor fit (for example sparse columns where Nullable/Default are unacceptable). Not a substitute for StringConverter settings.
- `bypassSchemaValidation` — advanced escape hatch; prefer fixing schema/table alignment.

## Performance tradeoffs

| Path | Connect-side work | ClickHouse-side work | Guidance |
| --- | --- | --- | --- |
| JsonConverter (schemaless) | Deserialize JSON to Connect structures, then serialize to `JSONEachRow` | Parse `JSONEachRow` | Best default for structured JSON; tune `max.poll.records`, fetch sizes, and optional `bufferCount` for throughput |
| StringConverter + `insertFormat` | Minimal (string bytes + optional newline) | Parse CSV / TSV / `JSONEachRow` | Prefer for high-throughput when producers already emit the target text format; reduces Connect CPU and GC pressure |
| Schema + RowBinary | Typed conversion to binary | Efficient binary insert | Prefer for Avro/Protobuf; usually better than JSON text for dense typed pipelines |

**High-throughput tips**

1. Pick the converter that matches the **bytes already on the topic** — avoid JsonConverter if the payload is already CSV destined for `insertFormat=CSV`.
2. Prefer larger batches (`consumer.override.max.poll.records`, fetch byte limits) so ClickHouse sees fewer, larger inserts.
3. Consider ClickHouse `async_insert` via `clickhouseSettings` when batches stay small; keep `wait_for_async_insert=1` with `exactlyOnce=true`.
4. Measure with the repo `benchmark` module and connector JMX metrics (`recordProcessingTime`, `taskProcessingTime`) rather than guessing.

StringConverter is **not** automatically faster if ClickHouse then spends more time parsing awkward CSV or if you shrink batches. Always validate end-to-end lag and CPU.

## Troubleshooting checklist

1. Confirm `value.converter` on the **connector** (not only the worker).
2. For JSON: confirm `value.converter.schemas.enable` matches the payload shape.
3. For strings: confirm `customInsertFormat=true` and `insertFormat` is `CSV`, `TSV`, or `JSON`.
4. Reproduce one record with `clickhouse-client` using the same FORMAT.
5. If values are blank/zero, check field names vs table columns (Flatten SMT is a common fix for nested CDC JSON).
6. Enable a DLQ (`errors.tolerance=all` + `errors.deadletterqueue.topic.name`) while debugging bad rows.

## Examples and cross-links

- Public docs (install, full config table, string support): [ClickHouse Kafka Connect Sink](https://clickhouse.com/docs/en/integrations/kafka/clickhouse-kafka-connect-sink)
- Local design / exactly-once: [`docs/DESIGN.md`](./DESIGN.md)
- Ready-made connector JSON (note which converter each recipe uses): [`Support Scripts.md`](../Support%20Scripts.md)
- Contribution workflow: [`CONTRIBUTING.md`](../CONTRIBUTING.md)
