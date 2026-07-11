package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers.newDescriptor;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClickHouseWriterSerializationTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void emptyStringIsNullForNullableUUID(boolean defaultsSupport) throws IOException {
        ClickHouseWriter writer = new ClickHouseWriter(new SinkTaskStatistics(0));
        Column column = Column.extractColumn(newDescriptor("uuid", "Nullable(UUID)"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        writer.doWriteCol(new Data(Schema.STRING_SCHEMA, ""), true, column, out, defaultsSupport);

        byte[] expected = defaultsSupport ? new byte[]{0, 1} : new byte[]{1};
        assertArrayEquals(expected, out.toByteArray());
    }

    @Test
    public void emptyStringIsRejectedForNonNullableUUID() {
        ClickHouseWriter writer = new ClickHouseWriter(new SinkTaskStatistics(0));
        Column column = Column.extractColumn(newDescriptor("uuid", "UUID"));

        assertThrows(IllegalArgumentException.class,
                () -> writer.doWriteCol(new Data(Schema.STRING_SCHEMA, ""), true, column,
                        new ByteArrayOutputStream(), false));
    }
}
