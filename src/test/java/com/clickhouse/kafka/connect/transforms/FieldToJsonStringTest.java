package com.clickhouse.kafka.connect.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

public class FieldToJsonStringTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Map<String, Object> config(String fields) {
        Map<String, Object> config = new HashMap<>();
        config.put("fields", fields);
        return config;
    }

    @Test
    public void schemalessTopLevelObjectTest() {
        try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
            smt.configure(config("clientContext"));

            Map<String, Object> clientContext = new LinkedHashMap<>();
            clientContext.put("initiator", "System");
            clientContext.put("updatedByIp", null);
            Map<String, Object> value = new HashMap<>();
            value.put("userId", "u1");
            value.put("clientContext", clientContext);

            SinkRecord record = new SinkRecord("topic", 0, null, "u1", null, value, 0);
            SinkRecord out = smt.apply(record);

            @SuppressWarnings("unchecked")
            Map<String, Object> outValue = (Map<String, Object>) out.value();
            Assertions.assertEquals("u1", outValue.get("userId"));
            Assertions.assertTrue(outValue.get("clientContext") instanceof String);

            Map<String, Object> roundTripped = readJsonAsMap((String) outValue.get("clientContext"));
            Assertions.assertEquals("System", roundTripped.get("initiator"));
            Assertions.assertFalse(roundTripped.containsKey("updatedByIp"));
        }
    }

    @Test
    public void schemalessNestedPathTest() {
        try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
            smt.configure(config("account.registrationData.marketingData.metadata"));

            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put("org", "direct");
            Map<String, Object> nc = new LinkedHashMap<>();
            nc.put("utm_source", "remarketing");
            metadata.put("nc", nc);

            Map<String, Object> marketingData = new HashMap<>();
            marketingData.put("promoCode", "string");
            marketingData.put("metadata", metadata);
            Map<String, Object> registrationData = new HashMap<>();
            registrationData.put("marketingData", marketingData);
            Map<String, Object> account = new HashMap<>();
            account.put("registrationData", registrationData);
            Map<String, Object> value = new HashMap<>();
            value.put("userId", "u1");
            value.put("account", account);

            SinkRecord record = new SinkRecord("topic", 0, null, "u1", null, value, 0);
            SinkRecord out = smt.apply(record);

            @SuppressWarnings("unchecked")
            Map<String, Object> outAccount =
                    (Map<String, Object>) ((Map<String, Object>) out.value()).get("account");
            @SuppressWarnings("unchecked")
            Map<String, Object> outMarketing =
                    (Map<String, Object>) ((Map<String, Object>) outAccount.get("registrationData")).get("marketingData");

            Assertions.assertEquals("string", outMarketing.get("promoCode"));
            Assertions.assertTrue(outMarketing.get("metadata") instanceof String);
            Assertions.assertEquals(metadata, readJsonAsMap((String) outMarketing.get("metadata")));
        }
    }

    @Test
    public void schemalessMultipleFieldsTest() {
        try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
            smt.configure(config("clientContext,account.metadata"));

            Map<String, Object> clientContext = new LinkedHashMap<>();
            clientContext.put("initiator", "System");
            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put("k", "v");
            Map<String, Object> account = new HashMap<>();
            account.put("metadata", metadata);
            Map<String, Object> value = new HashMap<>();
            value.put("clientContext", clientContext);
            value.put("account", account);

            SinkRecord out = smt.apply(new SinkRecord("topic", 0, null, "u1", null, value, 0));

            @SuppressWarnings("unchecked")
            Map<String, Object> outValue = (Map<String, Object>) out.value();
            @SuppressWarnings("unchecked")
            Map<String, Object> outAccount = (Map<String, Object>) outValue.get("account");
            Assertions.assertTrue(outValue.get("clientContext") instanceof String);
            Assertions.assertTrue(outAccount.get("metadata") instanceof String);
        }
    }

    @Test
    public void schemalessAlreadyStringIsUntouchedTest() {
        try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
            smt.configure(config("metadata"));
            Map<String, Object> value = new HashMap<>();
            value.put("metadata", "{\"already\":\"json\"}");

            SinkRecord out = smt.apply(new SinkRecord("topic", 0, null, "u1", null, value, 0));

            @SuppressWarnings("unchecked")
            Map<String, Object> outValue = (Map<String, Object>) out.value();
            Assertions.assertEquals("{\"already\":\"json\"}", outValue.get("metadata"));
        }
    }

    @Test
    public void schemalessMissingAndNullAreIgnoredTest() {
        try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
            smt.configure(config("clientContext,account.metadata"));
            Map<String, Object> value = new HashMap<>();
            value.put("clientContext", null);
            value.put("userId", "u1");

            SinkRecord out = smt.apply(new SinkRecord("topic", 0, null, "u1", null, value, 0));

            @SuppressWarnings("unchecked")
            Map<String, Object> outValue = (Map<String, Object>) out.value();
            Assertions.assertNull(outValue.get("clientContext"));
            Assertions.assertEquals("u1", outValue.get("userId"));
            Assertions.assertFalse(outValue.containsKey("account"));
        }
    }

    @Test
    public void schemalessMissingThrowsWhenIgnoreDisabledTest() {
        try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
            Map<String, Object> config = config("clientContext");
            config.put("ignore.missing", false);
            smt.configure(config);
            Map<String, Object> value = new HashMap<>();
            value.put("userId", "u1");

            SinkRecord record = new SinkRecord("topic", 0, null, "u1", null, value, 0);
            Assertions.assertThrows(DataException.class, () -> smt.apply(record));
        }
    }

    @Test
    public void nullRecordValueIsPassedThroughTest() {
        try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
            smt.configure(config("clientContext"));
            SinkRecord record = new SinkRecord("topic", 0, null, "u1", null, null, 0);
            SinkRecord out = smt.apply(record);
            Assertions.assertNull(out.value());
        }
    }

    @Test
    public void schemaTopLevelStructBecomesStringTest() {
        try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
            smt.configure(config("clientContext"));

            Schema clientContextSchema = SchemaBuilder.struct()
                    .field("initiator", Schema.OPTIONAL_STRING_SCHEMA)
                    .optional()
                    .build();
            Schema valueSchema = SchemaBuilder.struct()
                    .field("userId", Schema.STRING_SCHEMA)
                    .field("clientContext", clientContextSchema)
                    .build();
            Struct clientContext = new Struct(clientContextSchema).put("initiator", "System");
            Struct value = new Struct(valueSchema).put("userId", "u1").put("clientContext", clientContext);

            SinkRecord out = smt.apply(new SinkRecord("topic", 0, null, "u1", valueSchema, value, 0));

            Struct outValue = (Struct) out.value();
            Assertions.assertEquals(Schema.Type.STRING, outValue.schema().field("clientContext").schema().type());
            Assertions.assertTrue(outValue.get("clientContext") instanceof String);
            Assertions.assertEquals("System",
                    readJsonAsMap((String) outValue.get("clientContext")).get("initiator"));
            Assertions.assertEquals("u1", outValue.get("userId"));
        }
    }

    @Test
    public void schemaNestedStructBecomesStringTest() {
        try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
            smt.configure(config("account.metadata"));

            Schema metadataSchema = SchemaBuilder.struct()
                    .field("org", Schema.OPTIONAL_STRING_SCHEMA)
                    .optional()
                    .build();
            Schema accountSchema = SchemaBuilder.struct()
                    .field("currency", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("metadata", metadataSchema)
                    .build();
            Schema valueSchema = SchemaBuilder.struct()
                    .field("userId", Schema.STRING_SCHEMA)
                    .field("account", accountSchema)
                    .build();
            Struct metadata = new Struct(metadataSchema).put("org", "direct");
            Struct account = new Struct(accountSchema).put("currency", "EUR").put("metadata", metadata);
            Struct value = new Struct(valueSchema).put("userId", "u1").put("account", account);

            SinkRecord out = smt.apply(new SinkRecord("topic", 0, null, "u1", valueSchema, value, 0));

            Struct outValue = (Struct) out.value();
            Struct outAccount = (Struct) outValue.get("account");
            Assertions.assertEquals(Schema.Type.STRUCT, outValue.schema().field("account").schema().type());
            Assertions.assertEquals(Schema.Type.STRING, outAccount.schema().field("metadata").schema().type());
            Assertions.assertEquals("EUR", outAccount.get("currency"));
            Assertions.assertEquals("direct",
                    readJsonAsMap((String) outAccount.get("metadata")).get("org"));
        }
    }

    @Test
    public void schemaNullLeafStaysNullTest() {
        try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
            smt.configure(config("clientContext"));

            Schema clientContextSchema = SchemaBuilder.struct()
                    .field("initiator", Schema.OPTIONAL_STRING_SCHEMA)
                    .optional()
                    .build();
            Schema valueSchema = SchemaBuilder.struct()
                    .field("userId", Schema.STRING_SCHEMA)
                    .field("clientContext", clientContextSchema)
                    .build();
            Struct value = new Struct(valueSchema).put("userId", "u1").put("clientContext", null);

            SinkRecord out = smt.apply(new SinkRecord("topic", 0, null, "u1", valueSchema, value, 0));

            Struct outValue = (Struct) out.value();
            Assertions.assertEquals(Schema.Type.STRING, outValue.schema().field("clientContext").schema().type());
            Assertions.assertNull(outValue.get("clientContext"));
        }
    }

    @Test
    public void schemaCacheIsReusedTest() {
        try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
            smt.configure(config("clientContext"));

            Schema clientContextSchema = SchemaBuilder.struct()
                    .field("initiator", Schema.OPTIONAL_STRING_SCHEMA)
                    .optional()
                    .build();
            Schema valueSchema = SchemaBuilder.struct()
                    .field("userId", Schema.STRING_SCHEMA)
                    .field("clientContext", clientContextSchema)
                    .build();

            for (int i = 0; i < 5; i++) {
                Struct value = new Struct(valueSchema)
                        .put("userId", "u" + i)
                        .put("clientContext", new Struct(clientContextSchema).put("initiator", "System"));
                smt.apply(new SinkRecord("topic", 0, null, "u" + i, valueSchema, value, i));
            }

            Assertions.assertEquals(1, smt.cacheMisses.get());
        }
    }

    @ParameterizedTest(name = "fields={0}, shouldSucceed={1}")
    @MethodSource("fieldsConfigurations")
    public void configureFieldsValidationTest(String fields, boolean shouldSucceed) {
        try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
            Map<String, Object> config = new HashMap<>();
            if (fields != null) {
                config.put("fields", fields);
            }
            if (shouldSucceed) {
                smt.configure(config);
                return;
            }
            Assertions.assertThrows(Exception.class, () -> smt.configure(config));
        }
    }

    private static Stream<Arguments> fieldsConfigurations() {
        return Stream.of(
                Arguments.of("clientContext", true),
                Arguments.of("a.b.c,d.e", true),
                Arguments.of("", false),
                Arguments.of(null, false));
    }

    @ParameterizedTest(name = "cacheSize={0}, shouldSucceed={1}")
    @MethodSource("cacheSizeConfigurations")
    public void configureCacheSizeValidationTest(Integer cacheSize, boolean shouldSucceed) {
        try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
            Map<String, Object> config = config("clientContext");
            if (cacheSize != null) {
                config.put("schema_cache_max_size", cacheSize);
            }
            if (shouldSucceed) {
                smt.configure(config);
                return;
            }
            Assertions.assertThrows(IllegalArgumentException.class, () -> smt.configure(config));
        }
    }

    private static Stream<Arguments> cacheSizeConfigurations() {
        return Stream.of(
                Arguments.of(null, true),
                Arguments.of(16, true),
                Arguments.of(1000, true),
                Arguments.of(15, false),
                Arguments.of(1001, false),
                Arguments.of(0, false));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> readJsonAsMap(String json) {
        try {
            JsonNode node = MAPPER.readTree(json);
            return MAPPER.convertValue(node, Map.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
