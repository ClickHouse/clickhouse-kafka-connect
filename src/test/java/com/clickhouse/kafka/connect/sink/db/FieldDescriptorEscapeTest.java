package com.clickhouse.kafka.connect.sink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseFieldDescriptor;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class FieldDescriptorEscapeTest {

    private static final String ROW =
            "{\"name\":\"a\\/b\",\"type\":\"Nullable(Float64)\",\"default_type\":\"\","
                    + "\"default_expression\":\"\",\"comment\":\"\",\"codec_expression\":\"\","
                    + "\"ttl_expression\":\"\",\"is_subcolumn\":0}";

    @Test
    public void parsesEscapedSlashInColumnName() throws Exception {
        assertEquals("a/b", ClickHouseFieldDescriptor.fromJsonRow(ROW).getName());
    }

    @ParameterizedTest
    @MethodSource("escapedNames")
    public void preservesOtherEscapesInColumnNames(String escapedName, String expected)
            throws Exception {
        String json = "{\"name\":\"" + escapedName + "\",\"type\":\"String\"}";

        assertEquals(expected, ClickHouseFieldDescriptor.fromJsonRow(json).getName());
    }

    private static Stream<Arguments> escapedNames() {
        return Stream.of(
                Arguments.of("a/b", "a/b"),
                Arguments.of("a\\\\b", "a\\b"),
                Arguments.of("a\\\"b", "a\"b"),
                Arguments.of("a\\nb", "a\nb"),
                Arguments.of("a\\tb", "a\tb"),
                Arguments.of("caf\\u00e9\\/\\u6e29\\u5ea6", "café/温度"));
    }

    @Test
    public void preservesEscapedCommentsAndDefaultExpressions() throws Exception {
        String json =
                "{\"name\":\"a\\/b\",\"type\":\"String\",\"default_type\":\"DEFAULT\","
                        + "\"default_expression\":\"'a\\\\\\\\b'\","
                        + "\"comment\":\"backslash: \\\\, quote: \\\", newline: \\n, tab: \\t, café/温度\"}";

        ClickHouseFieldDescriptor descriptor = ClickHouseFieldDescriptor.fromJsonRow(json);

        assertEquals("a/b", descriptor.getName());
        assertEquals("DEFAULT", descriptor.getDefaultType());
        assertEquals("'a\\\\b'", descriptor.getDefaultExpression());
        assertEquals("backslash: \\, quote: \", newline: \n, tab: \t, café/温度", descriptor.getComment());
    }

    @Test
    public void rejectsMalformedJson() {
        assertThrows(
                JsonProcessingException.class,
                () -> ClickHouseFieldDescriptor.fromJsonRow("{\"name\":\"unterminated}"));
    }
}
