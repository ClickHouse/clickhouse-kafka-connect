package com.clickhouse.kafka.connect.sink.data.convert;

import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.data.SchemaType;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DebeziumRecordConvertorTest {

    private static final String TOPIC = "server.db.orders.Envelope";
    private static final String DATABASE = "test_db";

    // --- Schema builders ---

    private static Schema rowSchema(SchemaBuilder... fields) {
        SchemaBuilder b = SchemaBuilder.struct();
        for (SchemaBuilder f : fields) {
            b.field(f.name(), f.build());
        }
        return b.build();
    }

    private static Schema sourceSchema() {
        return SchemaBuilder.struct().optional()
                .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
                .field("gtid", Schema.OPTIONAL_STRING_SCHEMA)
                .field("pos", Schema.OPTIONAL_INT64_SCHEMA)
                .field("change_lsn", Schema.OPTIONAL_STRING_SCHEMA)
                .field("commit_lsn", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    private static Schema envelopeSchema(Schema rowSchema) {
        return SchemaBuilder.struct().name("server.db.orders.Envelope")
                .field("op", Schema.STRING_SCHEMA)
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("source", sourceSchema())
                .build();
    }

    private static Struct sourceStruct(Schema schema, Long lsn) {
        Struct s = new Struct(schema);
        s.put("lsn", lsn);
        return s;
    }

    private static SinkRecord sinkRecord(Struct envelope) {
        return new SinkRecord(TOPIC, 0, null, null,
                envelope.schema(), envelope, 42L,
                System.currentTimeMillis(), TimestampType.CREATE_TIME);
    }

    // --- Helpers ---

    private static Record convert(Struct envelope) {
        return new DebeziumRecordConvertor().doConvert(sinkRecord(envelope), TOPIC, DATABASE);
    }

    // ===========================================================================
    // Op routing
    // ===========================================================================

    @Test
    @DisplayName("op=c routes to after struct with is_deleted=0")
    void opCreate_routesToAfter() {
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        Schema env = envelopeSchema(row);
        Struct after = new Struct(row).put("id", 1);
        Struct envelope = new Struct(env)
                .put("op", "c")
                .put("after", after)
                .put("source", sourceStruct(sourceSchema(), 100L));

        Record record = convert(envelope);

        assertEquals(SchemaType.DEBEZIUM_CDC, record.getSchemaType());
        assertEquals(1, record.getJsonMap().get("id").getObject());
        assertEquals((byte) 0, record.getJsonMap().get("is_deleted").getObject());
    }

    @Test
    @DisplayName("op=u routes to after struct with is_deleted=0")
    void opUpdate_routesToAfter() {
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        Schema env = envelopeSchema(row);
        Struct after = new Struct(row).put("id", 2);
        Struct envelope = new Struct(env)
                .put("op", "u")
                .put("after", after)
                .put("source", sourceStruct(sourceSchema(), 200L));

        Record record = convert(envelope);

        assertEquals((byte) 0, record.getJsonMap().get("is_deleted").getObject());
        assertEquals(2, record.getJsonMap().get("id").getObject());
    }

    @Test
    @DisplayName("op=r (read/snapshot) routes to after struct with is_deleted=0")
    void opRead_routesToAfter() {
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        Schema env = envelopeSchema(row);
        Struct after = new Struct(row).put("id", 3);
        Struct envelope = new Struct(env)
                .put("op", "r")
                .put("after", after)
                .put("source", sourceStruct(sourceSchema(), 300L));

        Record record = convert(envelope);

        assertEquals((byte) 0, record.getJsonMap().get("is_deleted").getObject());
    }

    @Test
    @DisplayName("op=d routes to before struct with is_deleted=1")
    void opDelete_routesToBefore() {
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        Schema env = envelopeSchema(row);
        Struct before = new Struct(row).put("id", 5);
        Struct envelope = new Struct(env)
                .put("op", "d")
                .put("before", before)
                .put("source", sourceStruct(sourceSchema(), 500L));

        Record record = convert(envelope);

        assertEquals((byte) 1, record.getJsonMap().get("is_deleted").getObject());
        assertEquals(5, record.getJsonMap().get("id").getObject());
    }

    @Test
    @DisplayName("op=t (truncate) produces empty record")
    void opTruncate_producesEmptyRecord() {
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        Schema env = envelopeSchema(row);
        Struct envelope = new Struct(env)
                .put("op", "t")
                .put("source", sourceStruct(sourceSchema(), 600L));

        Record record = convert(envelope);

        assertTrue(record.getJsonMap().isEmpty());
        assertTrue(record.getFields().isEmpty());
    }

    @Test
    @DisplayName("null after struct on op=c produces empty record")
    void nullAfterStruct_producesEmptyRecord() {
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        Schema env = envelopeSchema(row);
        Struct envelope = new Struct(env)
                .put("op", "c")
                .put("source", sourceStruct(sourceSchema(), 700L));
        // after is intentionally not set → null

        Record record = convert(envelope);

        assertTrue(record.getJsonMap().isEmpty());
    }

    // ===========================================================================
    // _version extraction
    // ===========================================================================

    @Test
    @DisplayName("PostgreSQL lsn Long is used as _version")
    void version_postgresLsn() {
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        Schema env = envelopeSchema(row);
        Struct after = new Struct(row).put("id", 1);
        Struct envelope = new Struct(env)
                .put("op", "c")
                .put("after", after)
                .put("source", sourceStruct(sourceSchema(), 12345L));

        Record record = convert(envelope);

        assertEquals(BigInteger.valueOf(12345L), record.getJsonMap().get("_version").getObject());
    }

    @Test
    @DisplayName("MySQL gtid 'uuid:N' sequence number is used as _version")
    void version_mysqlGtid() {
        Schema sourceWithGtid = SchemaBuilder.struct().optional()
                .field("gtid", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        Schema env = SchemaBuilder.struct().name("server.db.orders.Envelope")
                .field("op", Schema.STRING_SCHEMA)
                .field("before", row)
                .field("after", row)
                .field("source", sourceWithGtid)
                .build();
        Struct source = new Struct(sourceWithGtid).put("gtid", "3E11FA47-71CA-11E1-9E33-C80AA9429562:100");
        Struct after = new Struct(row).put("id", 1);
        Struct envelope = new Struct(env)
                .put("op", "c")
                .put("after", after)
                .put("source", source);

        Record record = convert(envelope);

        assertEquals(BigInteger.valueOf(100L), record.getJsonMap().get("_version").getObject());
    }

    @Test
    @DisplayName("SQL Server change_lsn is bit-packed into _version")
    void version_sqlServerChangeLsn() {
        Schema sourceWithChangeLsn = SchemaBuilder.struct().optional()
                .field("commit_lsn", Schema.OPTIONAL_STRING_SCHEMA)
                .field("change_lsn", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        Schema env = SchemaBuilder.struct().name("server.db.orders.Envelope")
                .field("op", Schema.STRING_SCHEMA)
                .field("before", row)
                .field("after", row)
                .field("source", sourceWithChangeLsn)
                .build();
        Struct source = new Struct(sourceWithChangeLsn)
                .put("commit_lsn", null)
                .put("change_lsn", "00000001:00000002:0003");
        Struct after = new Struct(row).put("id", 1);
        Struct envelope = new Struct(env)
                .put("op", "c")
                .put("after", after)
                .put("source", source);

        Record record = convert(envelope);

        // commit_lsn absent → commitPacked=0; change_lsn packed into low 64 bits
        long changePacked = (1L << 40) | (2L << 8) | 3L;
        BigInteger expected = BigInteger.ZERO.shiftLeft(64).or(BigInteger.valueOf(changePacked));
        assertEquals(expected, record.getJsonMap().get("_version").getObject());
    }

    @Test
    @DisplayName("SQL Server snapshot: change_lsn=null falls back to commit_lsn for _version")
    void version_sqlServerSnapshot_commitLsnFallback() {
        Schema sqlServerSource = SchemaBuilder.struct().optional()
                .field("change_lsn", Schema.OPTIONAL_STRING_SCHEMA)
                .field("commit_lsn", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        Schema env = SchemaBuilder.struct().name("server.db.orders.Envelope")
                .field("op", Schema.STRING_SCHEMA)
                .field("before", row)
                .field("after", row)
                .field("source", sqlServerSource)
                .build();
        Struct source = new Struct(sqlServerSource)
                .put("change_lsn", null)
                .put("commit_lsn", "0000006f:00000ab7:0003");
        Struct after = new Struct(row).put("id", 1);
        Struct envelope = new Struct(env)
                .put("op", "r")
                .put("after", after)
                .put("source", source);

        Record record = convert(envelope);

        // commit_lsn="0000006f:00000ab7:0003" packed into high 64 bits; change_lsn=null → 0 in low 64 bits
        long commitPacked = (0x6fL << 40) | (0xab7L << 8) | 0x03L;
        BigInteger expected = BigInteger.valueOf(commitPacked).shiftLeft(64);
        assertEquals(expected, record.getJsonMap().get("_version").getObject());
    }

    @Test
    @DisplayName("null source struct defaults _version to 0")
    void version_noSource_defaultsToZero() {
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        // source field present in schema but value is null — realistic for some connectors
        Schema env = SchemaBuilder.struct().name("server.db.orders.Envelope")
                .field("op", Schema.STRING_SCHEMA)
                .field("before", row)
                .field("after", row)
                .field("source", sourceSchema())
                .build();
        Struct after = new Struct(row).put("id", 1);
        Struct envelope = new Struct(env).put("op", "c").put("after", after);
        // source intentionally not set → null

        Record record = convert(envelope);

        assertEquals(BigInteger.ZERO, record.getJsonMap().get("_version").getObject());
    }

    // ===========================================================================
    // Type conversions
    // ===========================================================================

    @Test
    @DisplayName("BYTES + Decimal logical type converts to BigDecimal")
    void typeConversion_decimal_fromBytes() {
        Schema decimalSchema = Decimal.schema(2);
        Schema row = SchemaBuilder.struct().field("price", decimalSchema).build();
        Schema env = envelopeSchema(row);
        BigDecimal expected = new BigDecimal("12.34");
        Struct after = new Struct(row).put("price", expected);
        Struct envelope = new Struct(env)
                .put("op", "c").put("after", after)
                .put("source", sourceStruct(sourceSchema(), 1L));

        Record record = convert(envelope);

        Object val = record.getJsonMap().get("price").getObject();
        assertInstanceOf(BigDecimal.class, val);
        assertEquals(0, expected.compareTo((BigDecimal) val));
    }

    @Test
    @DisplayName("BYTES without logical type converts to hex string")
    void typeConversion_rawBytes_toHex() {
        Schema row = SchemaBuilder.struct().field("hash", Schema.BYTES_SCHEMA).build();
        Schema env = envelopeSchema(row);
        Struct after = new Struct(row).put("hash", new byte[]{(byte) 0xDE, (byte) 0xAD});
        Struct envelope = new Struct(env)
                .put("op", "c").put("after", after)
                .put("source", sourceStruct(sourceSchema(), 1L));

        Record record = convert(envelope);

        assertEquals("dead", record.getJsonMap().get("hash").getObject());
    }

    @Test
    @DisplayName("BYTES ByteBuffer without logical type converts to hex string")
    void typeConversion_rawByteBuffer_toHex() {
        Schema row = SchemaBuilder.struct().field("hash", Schema.BYTES_SCHEMA).build();
        Schema env = envelopeSchema(row);
        Struct after = new Struct(row).put("hash", ByteBuffer.wrap(new byte[]{(byte) 0xBE, (byte) 0xEF}));
        Struct envelope = new Struct(env)
                .put("op", "c").put("after", after)
                .put("source", sourceStruct(sourceSchema(), 1L));

        Record record = convert(envelope);

        assertEquals("beef", record.getJsonMap().get("hash").getObject());
    }

    @Test
    @DisplayName("INT64 + Timestamp (java.util.Date) normalizes to Date")
    void typeConversion_timestampAsDate() {
        Schema tsSchema = Timestamp.builder().build();
        Schema row = SchemaBuilder.struct().field("created_at", tsSchema).build();
        Schema env = envelopeSchema(row);
        Date now = new Date(1_700_000_000_000L);
        Struct after = new Struct(row).put("created_at", now);
        Struct envelope = new Struct(env)
                .put("op", "c").put("after", after)
                .put("source", sourceStruct(sourceSchema(), 1L));

        Record record = convert(envelope);

        Object val = record.getJsonMap().get("created_at").getObject();
        assertInstanceOf(Date.class, val);
        assertEquals(now, val);
    }

    @Test
    @DisplayName("INT64 + Timestamp (Long millis) normalizes to Date")
    void typeConversion_timestampAsLong() {
        Schema tsSchema = Timestamp.builder().build();
        Schema row = SchemaBuilder.struct().field("created_at", tsSchema).build();
        Schema env = envelopeSchema(row);
        long millis = 1_700_000_000_000L;
        Struct after = new Struct(row).put("created_at", new Date(millis));
        Struct envelope = new Struct(env)
                .put("op", "c").put("after", after)
                .put("source", sourceStruct(sourceSchema(), 1L));

        Record record = convert(envelope);

        Object val = record.getJsonMap().get("created_at").getObject();
        assertInstanceOf(Date.class, val);
        assertEquals(millis, ((Date) val).getTime());
    }

    @Test
    @DisplayName("INT64 + MicroTimestamp (Long micros) passes through as Long")
    void typeConversion_microTimestampAsLong() {
        Schema microTsSchema = SchemaBuilder.int64().name("io.debezium.time.MicroTimestamp").build();
        Schema row = SchemaBuilder.struct().field("ts_micros", microTsSchema).build();
        Schema env = envelopeSchema(row);
        long micros = 1_700_000_000_000_000L;
        Struct after = new Struct(row).put("ts_micros", micros);
        Struct envelope = new Struct(env)
                .put("op", "c").put("after", after)
                .put("source", sourceStruct(sourceSchema(), 1L));

        Record record = convert(envelope);

        assertEquals(micros, record.getJsonMap().get("ts_micros").getObject());
    }

    @Test
    @DisplayName("INT32 + Date (days since epoch) converts to java.sql.Date")
    void typeConversion_dateAsDays() {
        Schema dateSchema = SchemaBuilder.int32().name("io.debezium.time.Date").build();
        Schema row = SchemaBuilder.struct().field("event_date", dateSchema).build();
        Schema env = envelopeSchema(row);
        Struct after = new Struct(row).put("event_date", 19000); // days since 1970-01-01
        Struct envelope = new Struct(env)
                .put("op", "c").put("after", after)
                .put("source", sourceStruct(sourceSchema(), 1L));

        Record record = convert(envelope);

        Object val = record.getJsonMap().get("event_date").getObject();
        assertInstanceOf(java.sql.Date.class, val);
        assertEquals(java.time.LocalDate.ofEpochDay(19000), ((java.sql.Date) val).toLocalDate());
    }

    @Test
    @DisplayName("null field value is preserved as null in jsonMap")
    void nullFieldValue_preservedAsNull() {
        Schema row = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("note", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema env = envelopeSchema(row);
        Struct after = new Struct(row).put("id", 1).put("note", null);
        Struct envelope = new Struct(env)
                .put("op", "c").put("after", after)
                .put("source", sourceStruct(sourceSchema(), 1L));

        Record record = convert(envelope);

        assertNotNull(record.getJsonMap().get("note"));
        assertNull(record.getJsonMap().get("note").getObject());
    }

    // ===========================================================================
    // Synthetic fields injected into fields list
    // ===========================================================================

    @Test
    @DisplayName("_version and is_deleted appear in fields list")
    void syntheticFields_presentInFieldsList() {
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        Schema env = envelopeSchema(row);
        Struct after = new Struct(row).put("id", 1);
        Struct envelope = new Struct(env)
                .put("op", "c").put("after", after)
                .put("source", sourceStruct(sourceSchema(), 99L));

        Record record = convert(envelope);

        Map<String, Field> fieldsByName = new java.util.HashMap<>();
        for (Field f : record.getFields()) fieldsByName.put(f.name(), f);

        assertTrue(fieldsByName.containsKey("_version"));
        assertTrue(fieldsByName.containsKey("is_deleted"));
        assertEquals(Schema.Type.BYTES, fieldsByName.get("_version").schema().type());
        assertEquals(Schema.Type.INT8, fieldsByName.get("is_deleted").schema().type());
    }

    // ===========================================================================
    // opt-in: disabled by default
    // ===========================================================================

    @Test
    @DisplayName("Envelope schema without debeziumCDCEnabled routes to SchemaRecordConvertor")
    void optIn_disabled_routesToSchemaConvertor() {
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        Schema env = envelopeSchema(row);
        Struct after = new Struct(row).put("id", 1);
        Struct envelope = new Struct(env)
                .put("op", "c").put("after", after)
                .put("source", sourceStruct(sourceSchema(), 1L));

        SinkRecord sinkRecord = sinkRecord(envelope);
        // debeziumCDCEnabled=false — envelope goes through SchemaRecordConvertor, not Debezium
        Record record = Record.convert(sinkRecord, false, "_", DATABASE, false);

        assertEquals(SchemaType.SCHEMA, record.getSchemaType());
    }

    @Test
    @DisplayName("Envelope schema with debeziumCDCEnabled=true routes to DebeziumRecordConvertor")
    void optIn_enabled_routesToDebeziumConvertor() {
        Schema row = rowSchema(SchemaBuilder.int32().name("id"));
        Schema env = envelopeSchema(row);
        Struct after = new Struct(row).put("id", 1);
        Struct envelope = new Struct(env)
                .put("op", "c").put("after", after)
                .put("source", sourceStruct(sourceSchema(), 1L));

        SinkRecord sinkRecord = sinkRecord(envelope);
        Record record = Record.convert(sinkRecord, false, "_", DATABASE, true);

        assertEquals(SchemaType.DEBEZIUM_CDC, record.getSchemaType());
    }
}
