package com.clickhouse.kafka.connect.sink.data.convert;

import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.data.SchemaType;
import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts Debezium CDC envelope records into flat Records suitable for
 * insertion into a ClickHouse ReplacingMergeTree(_version, is_deleted) table.
 *
 * Detection: activated when the SinkRecord value schema name ends with ".Envelope".
 *
 * Routing:
 *   op = "c" / "r" / "u" → after struct, is_deleted = 0
 *   op = "d"             → before struct, is_deleted = 1
 *   op = "t"             → skip (truncate not supported)
 *
 * Injected columns:
 *   _version   UInt64  — source.lsn (PostgreSQL) or parsed source.gtid (MySQL)
 *   is_deleted UInt8   — 0 for upserts, 1 for deletes
 *
 * Type conversions (mirrors Altinity ClickHouseDataTypeMapper):
 *   STRING (no logical type)                 → String (pass-through)
 *   STRING + ZonedTimestamp                  → String (ISO8601 normalized)
 *   INT64  + MicroTimestamp / Timestamp      → String datetime
 *   INT32  + Date                            → java.sql.Date
 *   BYTES  + Decimal                         → BigDecimal
 *   BYTES  (no logical type)                 → String (hex)
 *   STRING (numeric, no logical type)        → String kept, BigDecimal attempted in writer
 */
public class DebeziumRecordConvertor extends RecordConvertor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumRecordConvertor.class);

    private static final String FIELD_OP     = "op";
    private static final String FIELD_BEFORE = "before";
    private static final String FIELD_AFTER  = "after";
    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_LSN        = "lsn";
    private static final String FIELD_GTID       = "gtid";
    private static final String FIELD_POS        = "pos";
    private static final String FIELD_CHANGE_LSN  = "change_lsn";   // SQL Server streaming
    private static final String FIELD_COMMIT_LSN  = "commit_lsn";   // SQL Server snapshot + streaming fallback

    private static final String OP_DELETE   = "d";
    private static final String OP_TRUNCATE = "t";

    private static final String COL_VERSION    = "_version";
    private static final String COL_IS_DELETED = "is_deleted";

    // Debezium logical type names
    private static final String LOGICAL_MICRO_TIMESTAMP  = "io.debezium.time.MicroTimestamp";
    private static final String LOGICAL_TIMESTAMP        = "org.apache.kafka.connect.data.Timestamp";
    private static final String LOGICAL_DATE             = "io.debezium.time.Date";
    private static final String LOGICAL_DECIMAL          = "org.apache.kafka.connect.data.Decimal";

    @Override
    public Record doConvert(SinkRecord sinkRecord, String topic, String database) {
        Struct envelope = (Struct) sinkRecord.value();
        String op = envelope.getString(FIELD_OP);

        if (op == null || op.equalsIgnoreCase(OP_TRUNCATE)) {
            LOGGER.debug("Skipping Debezium record with op={} for topic={}", op, topic);
            return Record.newRecord(SchemaType.DEBEZIUM_CDC,
                    topic, sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset(),
                    Collections.emptyList(), Collections.emptyMap(), database, sinkRecord);
        }

        boolean isDelete = op.equalsIgnoreCase(OP_DELETE);
        Struct dataStruct = isDelete
                ? envelope.getStruct(FIELD_BEFORE)
                : envelope.getStruct(FIELD_AFTER);

        if (dataStruct == null) {
            LOGGER.warn("Debezium record op={} has null {} struct — skipping. topic={}",
                    op, isDelete ? FIELD_BEFORE : FIELD_AFTER, topic);
            return Record.newRecord(SchemaType.DEBEZIUM_CDC,
                    topic, sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset(),
                    Collections.emptyList(), Collections.emptyMap(), database, sinkRecord);
        }

        Map<String, Data> jsonMap = toDebeziumJsonMap(dataStruct);

        long version = extractVersion(envelope);
        jsonMap.put(COL_VERSION,    new Data(Schema.INT64_SCHEMA, version));
        jsonMap.put(COL_IS_DELETED, new Data(Schema.INT8_SCHEMA, isDelete ? (byte) 1 : (byte) 0));

        // Include synthetic fields in the fields list so validateDataSchema() can look them up.
        List<Field> fields = new ArrayList<>(dataStruct.schema().fields());
        fields.add(new Field(COL_VERSION,    fields.size(), SchemaBuilder.int64().build()));
        fields.add(new Field(COL_IS_DELETED, fields.size(), SchemaBuilder.int8().build()));

        return Record.newRecord(SchemaType.DEBEZIUM_CDC,
                topic, sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset(),
                fields, jsonMap, database, sinkRecord);
    }

    /**
     * Converts a Debezium after/before Struct into Map<String, Data> with
     * type-aware conversion for each Debezium logical type — mirrors the
     * conversion logic in Altinity's ClickHouseDataTypeMapper.
     */
    private Map<String, Data> toDebeziumJsonMap(Struct struct) {
        Map<String, Data> result = new HashMap<>();
        for (Field field : struct.schema().fields()) {
            String name       = field.name();
            Schema schema     = field.schema();
            Schema.Type type  = schema.type();
            String logicalName = schema.name();
            Object raw        = struct.get(field);

            if (raw == null) {
                result.put(name, new Data(schema, null));
                continue;
            }

            try {
                result.put(name, new Data(schema, convertValue(type, logicalName, raw)));
            } catch (Exception e) {
                LOGGER.warn("Could not convert field [{}] type={} logical={} — keeping raw value. Error: {}",
                        name, type, logicalName, e.getMessage());
                result.put(name, new Data(schema, raw));
            }
        }
        return result;
    }

    /**
     * Converts a single field value from its Debezium Java type to the Java type
     * that ClickHouseWriter.doWriteColValue() expects for each ClickHouse column type.
     */
    private Object convertValue(Schema.Type type, String logicalName, Object raw) {
        switch (type) {
            case BYTES:
                // Decimal logical type → BigDecimal
                if (LOGICAL_DECIMAL.equals(logicalName)) {
                    if (raw instanceof BigDecimal) return raw;
                    if (raw instanceof byte[])      return new BigDecimal(new BigInteger((byte[]) raw));
                    if (raw instanceof ByteBuffer) {
                        ByteBuffer buf = (ByteBuffer) raw;
                        byte[] bytes = new byte[buf.remaining()];
                        buf.get(bytes);
                        buf.rewind();
                        return new BigDecimal(new BigInteger(bytes));
                    }
                    return new BigDecimal(raw.toString());
                }
                // Raw bytes → hex string
                if (raw instanceof byte[])    return bytesToHex((byte[]) raw);
                if (raw instanceof ByteBuffer) {
                    ByteBuffer buf = (ByteBuffer) raw;
                    byte[] bytes = new byte[buf.remaining()];
                    buf.get(bytes);
                    buf.rewind();
                    return bytesToHex(bytes);
                }
                return raw;

            case INT64:
                // doWriteDates handles Date natively in the INT64 branch.
                // For MicroTimestamp, normalize to microseconds Long so DateTime64(6) gets the right unit.
                if (LOGICAL_MICRO_TIMESTAMP.equals(logicalName)) {
                    if (raw instanceof java.util.Date) {
                        return ((java.util.Date) raw).getTime() * 1_000L;
                    }
                    return raw; // already Long microseconds
                }
                // For Timestamp, normalize to Date so doWriteDates uses doWriteDate(stream, date, precision).
                if (LOGICAL_TIMESTAMP.equals(logicalName)) {
                    if (raw instanceof java.util.Date) return raw;
                    return new java.util.Date(((Number) raw).longValue());
                }
                return raw;

            case INT32:
                // Days since epoch → java.sql.Date
                if (LOGICAL_DATE.equals(logicalName)) {
                    return java.sql.Date.valueOf(
                            java.time.LocalDate.ofEpochDay(((Number) raw).longValue()));
                }
                return raw;

            case STRING:
                // ZonedTimestamp is already a valid ISO8601 string — pass through as-is.
                // doWriteDates handles string input for DateTime64 columns.
                return raw;

            default:
                return raw;
        }
    }

    private static long packSqlServerLsn(String lsn) {
        String[] parts = lsn.split(":");
        if (parts.length != 3) return 0L;
        long p1 = Long.parseLong(parts[0].trim(), 16) & 0xFFFFFFL;     // 24 bits
        long p2 = Long.parseLong(parts[1].trim(), 16) & 0xFFFFFFFFL;   // 32 bits
        long p3 = Long.parseLong(parts[2].trim(), 16) & 0xFFL;         //  8 bits
        return (p1 << 40) | (p2 << 8) | p3;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    /**
     * Extracts the replication position from the Debezium source struct.
     * Priority: PostgreSQL LSN → MySQL GTID sequence number → MySQL binlog pos.
     */
    private long extractVersion(Struct envelope) {
        Struct source = envelope.getStruct(FIELD_SOURCE);
        if (source == null) return 0L;

        // PostgreSQL: lsn is a Long
        try {
            Object lsn = source.get(FIELD_LSN);
            if (lsn instanceof Long) return (Long) lsn;
        } catch (Exception e) {
            LOGGER.debug("Could not read source.lsn — not a PostgreSQL source or field absent: {}", e.getMessage());
        }

        // MySQL: gtid = "uuid:N" — extract the sequence number N
        try {
            Object gtid = source.get(FIELD_GTID);
            if (gtid instanceof String) {
                String[] parts = ((String) gtid).split(":");
                if (parts.length == 2) return Long.parseLong(parts[1].trim());
            }
        } catch (Exception e) {
            LOGGER.debug("Could not parse source.gtid — not a MySQL source or unexpected format: {}", e.getMessage());
        }

        // MySQL fallback: binlog position
        try {
            Object pos = source.get(FIELD_POS);
            if (pos instanceof Long) return (Long) pos;
        } catch (Exception e) {
            LOGGER.debug("Could not read source.pos — not a MySQL source or field absent: {}", e.getMessage());
        }

        // SQL Server: change_lsn = "00085734:000fd88d:0003" (VLF:block:entry)
        // Null during snapshot (op=r); commit_lsn is used as fallback in that case.
        // Pack into 64 bits: 24 bits VLF | 32 bits block | 8 bits entry
        // Preserves correct LSN ordering within the 64-bit space.
        try {
            Object changeLsn = source.get(FIELD_CHANGE_LSN);
            if (changeLsn instanceof String) {
                long packed = packSqlServerLsn((String) changeLsn);
                if (packed > 0) return packed;
            }
        } catch (Exception e) {
            LOGGER.debug(
                    "Could not parse source.change_lsn — not a SQL Server source or unexpected format: {}",
                    e.getMessage());
        }

        // SQL Server fallback: commit_lsn is always present (streaming + snapshot).
        try {
            Object commitLsn = source.get(FIELD_COMMIT_LSN);
            if (commitLsn instanceof String) {
                long packed = packSqlServerLsn((String) commitLsn);
                if (packed > 0) return packed;
            }
        } catch (Exception e) {
            LOGGER.debug(
                    "Could not parse source.commit_lsn — not a SQL Server source or unexpected format: {}",
                    e.getMessage());
        }

        LOGGER.warn("Could not extract version from Debezium source struct — defaulting to 0");
        return 0L;
    }
}
