package com.clickhouse.kafka.connect.sink.db;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import java.io.ByteArrayOutputStream;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
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
}
