package com.clickhouse.kafka.connect.sink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the failing-column context that {@code ClickHouseWriter} attaches to
 * cast/conversion failures. These do not require a running ClickHouse instance because the
 * failure happens during serialization, before any bytes reach the server.
 */
public class ClickHouseWriterColumnContextTest {

    private static final String CONTEXT_PREFIX = "Failed to write column";

    private static ClickHouseWriter newWriter() {
        return new ClickHouseWriter(new SinkTaskStatistics(0));
    }

    @Test
    public void conversionErrorSurfacesColumnNameAndTypesWithoutLeakingRecordValue() {
        ClickHouseWriter writer = newWriter();
        // A UInt64 column expects a Number; a String value reproduces the bare ClassCastException
        // that previously only named Java types in the container logs (see issue #729).
        Column column = Column.extractColumn("effective_at", "UInt64", false, false, false);
        String recordValue = "definitely-not-a-number";
        Data value = new Data(Schema.STRING_SCHEMA, recordValue);

        DataException thrown = assertThrows(DataException.class,
                () -> writer.doWriteColValue(column, new ByteArrayOutputStream(), value, false));

        String message = thrown.getMessage();
        assertTrue(message.contains("effective_at"), "should name the failing column: " + message);
        assertTrue(message.contains("UINT64"), "should name the target ClickHouse type: " + message);
        assertTrue(message.contains("STRING"), "should name the source Kafka type: " + message);
        assertFalse(message.contains(recordValue), "must not leak the record value into the error: " + message);
        assertNotNull(thrown.getCause(), "should preserve the original failure as the cause");
        assertInstanceOf(ClassCastException.class, thrown.getCause(), "cause should be the original cast failure");
        // Asserting DataException above is itself the non-retryable guarantee: DataException extends
        // ConnectException, not RetriableException, so the framework routes a bad record to the DLQ
        // rather than retrying it forever.
    }

    @Test
    public void alreadyContextualizedNestedFailureIsNotWrappedTwice() {
        ClickHouseWriter writer = newWriter();
        // An Array(UInt64) whose element is a String fails on the inner element write; the outer
        // Array frame must surface the inner column context once rather than nesting it again.
        Column column = Column.extractColumn("amounts", "Array(UInt64)", false, false, false);
        Schema arraySchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        Data value = new Data(arraySchema, List.of("nope"));

        DataException thrown = assertThrows(DataException.class,
                () -> writer.doWriteColValue(column, new ByteArrayOutputStream(), value, false));

        String message = thrown.getMessage();
        assertTrue(message.contains(CONTEXT_PREFIX), "should carry the column context: " + message);
        assertTrue(message.indexOf(CONTEXT_PREFIX) == message.lastIndexOf(CONTEXT_PREFIX),
                "context prefix should not be duplicated on nested types: " + message);
    }

    @Test
    public void integerValueWithinRangeSucceeds() {
        ClickHouseWriter writer = newWriter();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        Column colInt64 = Column.extractColumn("val", "Int64", false, false, false);
        Column colInt32 = Column.extractColumn("val", "Int32", false, false, false);
        Column colInt16 = Column.extractColumn("val", "Int16", false, false, false);
        Column colInt8 = Column.extractColumn("val", "Int8", false, false, false);

        Data val100 = new Data(Schema.INT32_SCHEMA, 100);

        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> writer.doWriteColValue(colInt64, out, val100, false));
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> writer.doWriteColValue(colInt32, out, val100, false));
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> writer.doWriteColValue(colInt16, out, val100, false));
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> writer.doWriteColValue(colInt8, out, val100, false));
    }

    @Test
    public void validateDataSchemaAllowsNarrowerOrEqualDataType() {
        ClickHouseWriter writer = newWriter();

        Column col64 = Column.extractColumn("val64", "Int64", false, false, false);
        Column col32 = Column.extractColumn("val32", "Int32", false, false, false);
        Table table = new Table("default", "test_table", false, List.of(col64, col32), 2);

        Schema schema = SchemaBuilder.struct()
                .field("val64", Schema.INT32_SCHEMA)
                .field("val32", Schema.INT16_SCHEMA)
                .build();
        Struct struct = new Struct(schema)
                .put("val64", 100)
                .put("val32", (short) 50);

        SinkRecord sr = new SinkRecord("test_table", 0, null, null, schema, struct, 0);
        com.clickhouse.kafka.connect.sink.data.Record record =
                com.clickhouse.kafka.connect.sink.data.Record.convert(sr, false, ".", "default", false);

        assertTrue(writer.validateDataSchema(table, record, false),
                "Schema validation should pass when data types are narrower or equal to column types");
    }

    @Test
    public void validateDataSchemaRejectsWiderDataType() {
        ClickHouseWriter writer = newWriter();

        Column col32 = Column.extractColumn("val", "Int32", false, false, false);
        Table table = new Table("default", "test_table", false, List.of(col32), 1);

        Schema schema = SchemaBuilder.struct()
                .field("val", Schema.INT64_SCHEMA)
                .build();
        Struct struct = new Struct(schema)
                .put("val", 123456789L);

        SinkRecord sr = new SinkRecord("test_table", 0, null, null, schema, struct, 0);
        com.clickhouse.kafka.connect.sink.data.Record record =
                com.clickhouse.kafka.connect.sink.data.Record.convert(sr, false, ".", "default", false);

        assertFalse(writer.validateDataSchema(table, record, false),
                "Schema validation should fail when data type (INT64) is wider than column type (Int32)");
    }

    @Test
    public void validateDataSchemaExhaustiveBitWidthMatrix() {
        ClickHouseWriter writer = newWriter();

        // Check narrower or equal numeric conversions pass
        String[] allowedColsForInt8 = {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float32", "Float64"};
        for (String chType : allowedColsForInt8) {
            Table t = new Table("default", "test_table", false, List.of(Column.extractColumn("v", chType, false, false, false)), 1);
            Schema s = SchemaBuilder.struct().field("v", Schema.INT8_SCHEMA).build();
            com.clickhouse.kafka.connect.sink.data.Record r = com.clickhouse.kafka.connect.sink.data.Record.convert(
                    new SinkRecord("test_table", 0, null, null, s, new Struct(s).put("v", (byte) 1), 0), false, ".", "default", false);
            assertTrue(writer.validateDataSchema(t, r, false), "INT8 -> " + chType + " should be allowed");
        }

        String[] allowedColsForInt16 = {"Int16", "Int32", "Int64", "UInt16", "UInt32", "UInt64", "Float32", "Float64"};
        for (String chType : allowedColsForInt16) {
            Table t = new Table("default", "test_table", false, List.of(Column.extractColumn("v", chType, false, false, false)), 1);
            Schema s = SchemaBuilder.struct().field("v", Schema.INT16_SCHEMA).build();
            com.clickhouse.kafka.connect.sink.data.Record r = com.clickhouse.kafka.connect.sink.data.Record.convert(
                    new SinkRecord("test_table", 0, null, null, s, new Struct(s).put("v", (short) 1), 0), false, ".", "default", false);
            assertTrue(writer.validateDataSchema(t, r, false), "INT16 -> " + chType + " should be allowed");
        }

        String[] allowedColsForInt32 = {"Int32", "Int64", "UInt32", "UInt64", "Float32", "Float64"};
        for (String chType : allowedColsForInt32) {
            Table t = new Table("default", "test_table", false, List.of(Column.extractColumn("v", chType, false, false, false)), 1);
            Schema s = SchemaBuilder.struct().field("v", Schema.INT32_SCHEMA).build();
            com.clickhouse.kafka.connect.sink.data.Record r = com.clickhouse.kafka.connect.sink.data.Record.convert(
                    new SinkRecord("test_table", 0, null, null, s, new Struct(s).put("v", 1), 0), false, ".", "default", false);
            assertTrue(writer.validateDataSchema(t, r, false), "INT32 -> " + chType + " should be allowed");
        }

        String[] allowedColsForInt64 = {"Int64", "UInt64", "Float64"};
        for (String chType : allowedColsForInt64) {
            Table t = new Table("default", "test_table", false, List.of(Column.extractColumn("v", chType, false, false, false)), 1);
            Schema s = SchemaBuilder.struct().field("v", Schema.INT64_SCHEMA).build();
            com.clickhouse.kafka.connect.sink.data.Record r = com.clickhouse.kafka.connect.sink.data.Record.convert(
                    new SinkRecord("test_table", 0, null, null, s, new Struct(s).put("v", 1L), 0), false, ".", "default", false);
            assertTrue(writer.validateDataSchema(t, r, false), "INT64 -> " + chType + " should be allowed");
        }

        // Check wider into narrower numeric conversions fail
        String[] rejectedColsForInt64 = {"Int32", "Int16", "Int8", "UInt32", "UInt16", "UInt8", "Float32"};
        for (String chType : rejectedColsForInt64) {
            Table t = new Table("default", "test_table", false, List.of(Column.extractColumn("v", chType, false, false, false)), 1);
            Schema s = SchemaBuilder.struct().field("v", Schema.INT64_SCHEMA).build();
            com.clickhouse.kafka.connect.sink.data.Record r = com.clickhouse.kafka.connect.sink.data.Record.convert(
                    new SinkRecord("test_table", 0, null, null, s, new Struct(s).put("v", 1L), 0), false, ".", "default", false);
            assertFalse(writer.validateDataSchema(t, r, false), "INT64 -> " + chType + " should be rejected");
        }

        String[] rejectedColsForInt32 = {"Int16", "Int8", "UInt16", "UInt8"};
        for (String chType : rejectedColsForInt32) {
            Table t = new Table("default", "test_table", false, List.of(Column.extractColumn("v", chType, false, false, false)), 1);
            Schema s = SchemaBuilder.struct().field("v", Schema.INT32_SCHEMA).build();
            com.clickhouse.kafka.connect.sink.data.Record r = com.clickhouse.kafka.connect.sink.data.Record.convert(
                    new SinkRecord("test_table", 0, null, null, s, new Struct(s).put("v", 1), 0), false, ".", "default", false);
            assertFalse(writer.validateDataSchema(t, r, false), "INT32 -> " + chType + " should be rejected");
        }

        // Float data into Integer column should be rejected
        String[] rejectedColsForFloat64 = {"Int64", "Int32", "Int16", "Int8", "UInt64", "UInt32", "UInt16", "UInt8"};
        for (String chType : rejectedColsForFloat64) {
            Table t = new Table("default", "test_table", false, List.of(Column.extractColumn("v", chType, false, false, false)), 1);
            Schema s = SchemaBuilder.struct().field("v", Schema.FLOAT64_SCHEMA).build();
            com.clickhouse.kafka.connect.sink.data.Record r = com.clickhouse.kafka.connect.sink.data.Record.convert(
                    new SinkRecord("test_table", 0, null, null, s, new Struct(s).put("v", 1.23d), 0), false, ".", "default", false);
            assertFalse(writer.validateDataSchema(t, r, false), "FLOAT64 -> " + chType + " should be rejected");
        }
    }

    @Test
    public void integerSerializationValuesAndTypes() throws Exception {
        ClickHouseWriter writer = newWriter();

        // 1. INT8 value widening (-128, 127) -> Int8, Int16, Int32, Int64
        Column colInt8 = Column.extractColumn("val", "Int8", false, false, false);
        Column colInt16 = Column.extractColumn("val", "Int16", false, false, false);
        Column colInt32 = Column.extractColumn("val", "Int32", false, false, false);
        Column colInt64 = Column.extractColumn("val", "Int64", false, false, false);

        Data dataByteMin = new Data(Schema.INT8_SCHEMA, (byte) -128);

        ByteArrayOutputStream out8 = new ByteArrayOutputStream();
        writer.doWriteColValue(colInt8, out8, dataByteMin, false);
        assertEquals(-128, out8.toByteArray()[0]);

        ByteArrayOutputStream out16 = new ByteArrayOutputStream();
        writer.doWriteColValue(colInt16, out16, dataByteMin, false);
        assertEquals((short) -128, ByteBuffer.wrap(out16.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getShort());

        ByteArrayOutputStream out32 = new ByteArrayOutputStream();
        writer.doWriteColValue(colInt32, out32, dataByteMin, false);
        assertEquals(-128, ByteBuffer.wrap(out32.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getInt());

        ByteArrayOutputStream out64 = new ByteArrayOutputStream();
        writer.doWriteColValue(colInt64, out64, dataByteMin, false);
        assertEquals(-128L, ByteBuffer.wrap(out64.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getLong());

        // 2. INT16 value widening (-32768, 32767) -> Int16, Int32, Int64
        Data dataShortMin = new Data(Schema.INT16_SCHEMA, (short) -32768);

        out32 = new ByteArrayOutputStream();
        writer.doWriteColValue(colInt32, out32, dataShortMin, false);
        assertEquals(-32768, ByteBuffer.wrap(out32.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getInt());

        out64 = new ByteArrayOutputStream();
        writer.doWriteColValue(colInt64, out64, dataShortMin, false);
        assertEquals(-32768L, ByteBuffer.wrap(out64.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getLong());

        // 3. INT32 value widening (Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 0, 100) -> Int32, Int64, UInt32, Float64
        Data dataIntMin = new Data(Schema.INT32_SCHEMA, Integer.MIN_VALUE);
        Data dataIntMax = new Data(Schema.INT32_SCHEMA, Integer.MAX_VALUE);
        Data dataNegOne = new Data(Schema.INT32_SCHEMA, -1);

        out64 = new ByteArrayOutputStream();
        writer.doWriteColValue(colInt64, out64, dataIntMin, false);
        assertEquals((long) Integer.MIN_VALUE, ByteBuffer.wrap(out64.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getLong());

        out64 = new ByteArrayOutputStream();
        writer.doWriteColValue(colInt64, out64, dataIntMax, false);
        assertEquals((long) Integer.MAX_VALUE, ByteBuffer.wrap(out64.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getLong());

        Column colUInt32 = Column.extractColumn("val", "UInt32", false, false, false);
        out32 = new ByteArrayOutputStream();
        writer.doWriteColValue(colUInt32, out32, dataNegOne, false);
        assertEquals(-1, ByteBuffer.wrap(out32.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getInt()); // 0xFFFFFFFF bitwise

        Column colFloat64 = Column.extractColumn("val", "Float64", false, false, false);
        out64 = new ByteArrayOutputStream();
        writer.doWriteColValue(colFloat64, out64, new Data(Schema.INT32_SCHEMA, 12345), false);
        assertEquals(12345.0d, ByteBuffer.wrap(out64.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getDouble());

        // 4. INT64 values (Long.MIN_VALUE, Long.MAX_VALUE)
        Data dataLongMin = new Data(Schema.INT64_SCHEMA, Long.MIN_VALUE);
        Data dataLongMax = new Data(Schema.INT64_SCHEMA, Long.MAX_VALUE);

        out64 = new ByteArrayOutputStream();
        writer.doWriteColValue(colInt64, out64, dataLongMin, false);
        assertEquals(Long.MIN_VALUE, ByteBuffer.wrap(out64.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getLong());

        out64 = new ByteArrayOutputStream();
        writer.doWriteColValue(colInt64, out64, dataLongMax, false);
        assertEquals(Long.MAX_VALUE, ByteBuffer.wrap(out64.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getLong());
    }
}
