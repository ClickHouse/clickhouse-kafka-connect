# ClickHouse Kafka Connect Sink

## About
clickhouse-kafka-connect is the official Kafka Connect sink connector for [ClickHouse](https://clickhouse.com/).

The Kafka connector delivers data from a Kafka topic to a ClickHouse table.
## Documentation

See the [ClickHouse website](https://clickhouse.com/docs/en/integrations/kafka/clickhouse-kafka-connect-sink) for the full documentation entry.

## Design
For a full overview of the design and how exactly-once delivery semantics are achieved, see the [design document](./docs/DESIGN.md).

## Help
For additional help, please [file an issue in the repository](https://github.com/ClickHouse/clickhouse-kafka-connect/issues) or raise a question in [ClickHouse public Slack](https://clickhouse.com/slack).

## KeyToValue Transformation
We've created a transformation that allows you to convert a Kafka message key into a value.
This is useful when you want to store the key in a separate column in ClickHouse - by default, the column is `_key` and the type is String.

```sql
CREATE TABLE your_table_name
(
    `your_column_name` String,
    ...
    ...
    ...
    `_key` String
) ENGINE = MergeTree()
```

Simply add the transformation to your connector configuration:
    
```properties
transforms=keyToValue
transforms.keyToValue.type=com.clickhouse.kafka.connect.transforms.KeyToValue
transforms.keyToValue.field=_key
```

## FieldToJsonString Transformation
This transformation serializes one or more record fields into their JSON string representation.
It is useful when a source message contains a nested object (or array) but the destination column
is a plain `String` - for example when you want to keep a free-form, schema-less payload in a single
ClickHouse `String` column instead of modelling it as a nested `Tuple`/`Map`.

Given a message like:

```json
{
  "userId": "u1",
  "clientContext": { "initiator": "System", "updatedByIp": null },
  "account": {
    "registrationData": {
      "marketingData": { "metadata": { "org": "direct", "utm_source": "remarketing" } }
    }
  }
}
```

and a ClickHouse table where `clientContext` and `metadata` are `String` columns, the transformation
replaces those objects with their JSON string values so they fit the target columns.

Add it to your connector configuration and list the fields to serialize (nested fields use dot notation):

```properties
transforms=fieldToJsonString
transforms.fieldToJsonString.type=com.clickhouse.kafka.connect.transforms.FieldToJsonString
transforms.fieldToJsonString.fields=clientContext,account.registrationData.marketingData.metadata
```

### Configuration

| Property                | Type    | Default | Description                                                                                                  |
|-------------------------|---------|---------|--------------------------------------------------------------------------------------------------------------|
| `fields`                | list    | (required) | Comma-separated list of field paths to serialize. Nested fields are addressed with dot notation.          |
| `ignore.missing`        | boolean | `true`  | When `true`, a configured path that is missing or `null` is left untouched. When `false`, a missing path throws. |
| `schema_cache_max_size` | int     | `32`    | Maximum number of value schemas to cache (schema-based records only). Value range is `[16, 1000]`.           |

Notes:
- Works with both schema-less records (JSON converter, `value.converter.schemas.enable=false`) and records that carry a Connect schema. For schema-based records the target field's type is rewritten to `String`.
- A field value that is already a `String` is left unchanged.
- Fields are serialized with the connector's shared JSON mapper, which omits `null` sub-fields.

## Performance Testing

There is a dedicated gradle project in this repo - `benchmark` for performance testing. 
Please see its [README](./benchmark/README.md) for more information and how to run.