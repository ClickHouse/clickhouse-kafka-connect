package com.clickhouse.kafka.connect.sink.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.clickhouse.kafka.connect.util.DataJson.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that the Gson→Jackson migration produces byte-equivalent output
 * for all Map shapes used in the JSON insert paths (doInsertJsonV1/V2).
 *
 * @author Gaurav Miglani
 */
public class JsonSerializationEquivalenceTest {

    private static final Gson GSON = new Gson();
    private static final java.lang.reflect.Type GSON_MAP_TYPE =
            new TypeToken<HashMap<String, Object>>() {}.getType();

    @Test
    void flatMapWithPrimitives() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", "Alice");
        map.put("age", 30);
        map.put("score", 99.5);
        map.put("active", true);

        byte[] gsonBytes = GSON.toJson(map, GSON_MAP_TYPE).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(map);

        assertArrayEquals(gsonBytes, jacksonBytes,
                "Flat map with primitives should produce identical bytes");
    }

    /**
     * Gson silently drops null map values; Jackson serializes them as explicit
     * {@code "field":null}. This is the desired behaviour — ClickHouse handles
     * explicit nulls correctly for nullable columns.
     */
    @Test
    void nullMapValueHandling() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", "Alice");
        map.put("optional", null);

        String gsonJson = GSON.toJson(map, GSON_MAP_TYPE);
        String jacksonJson = new String(OBJECT_MAPPER.writeValueAsBytes(map), StandardCharsets.UTF_8);

        assertFalse(gsonJson.contains("optional"),
                "Gson drops null values from maps");
        assertTrue(jacksonJson.contains("\"optional\":null"),
                "Jackson preserves null values as explicit null");
    }

    @Test
    void emptyMap() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();

        byte[] gsonBytes = GSON.toJson(map, GSON_MAP_TYPE).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(map);

        assertArrayEquals(gsonBytes, jacksonBytes,
                "Empty map should produce identical bytes");
    }

    @Test
    void mapWithNestedMap() throws Exception {
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("city", "Berlin");
        inner.put("zip", 10115);

        Map<String, Object> outer = new LinkedHashMap<>();
        outer.put("id", 1);
        outer.put("address", inner);

        byte[] gsonBytes = GSON.toJson(outer, GSON_MAP_TYPE).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(outer);

        assertArrayEquals(gsonBytes, jacksonBytes,
                "Nested map should produce identical bytes");
    }

    @Test
    void mapWithIntegerTypes() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("int_val", 100000);
        map.put("long_val", 9999999999L);

        byte[] gsonBytes = GSON.toJson(map, GSON_MAP_TYPE).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(map);

        assertArrayEquals(gsonBytes, jacksonBytes,
                "Map with various integer types should produce identical bytes");
    }

    @Test
    void mapWithStringArray() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("tags", Arrays.asList("a", "b", "c"));
        map.put("count", 3);

        byte[] gsonBytes = GSON.toJson(map, GSON_MAP_TYPE).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(map);

        assertArrayEquals(gsonBytes, jacksonBytes,
                "Map with array values should produce identical bytes");
    }

    @Test
    void mapWithSpecialChars() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("text", "hello \"world\" \\ / \n\t");
        map.put("unicode", "שלום");

        byte[] gsonBytes = GSON.toJson(map, GSON_MAP_TYPE).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(map);

        assertArrayEquals(gsonBytes, jacksonBytes,
                "Map with special characters should produce identical bytes");
    }

    @Test
    void structSerializesToCleanJson() throws Exception {
        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("active", Schema.BOOLEAN_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("name", "Bob")
                .put("age", 25)
                .put("active", false);

        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(struct);

        ObjectMapper reader = new ObjectMapper();
        Map<?, ?> parsed = reader.readValue(jacksonBytes, Map.class);
        assertEquals("Bob", parsed.get("name"));
        assertEquals(25, parsed.get("age"));
        assertEquals(false, parsed.get("active"));
    }

    @Test
    void nestedStructSerializesToCleanJson() throws Exception {
        Schema addressSchema = SchemaBuilder.struct()
                .field("city", Schema.STRING_SCHEMA)
                .field("zip", Schema.INT32_SCHEMA)
                .build();

        Schema personSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("address", addressSchema)
                .build();

        Struct address = new Struct(addressSchema)
                .put("city", "NYC")
                .put("zip", 10001);

        Struct person = new Struct(personSchema)
                .put("name", "Charlie")
                .put("address", address);

        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(person);
        ObjectMapper reader = new ObjectMapper();
        Map<?, ?> parsed = reader.readValue(jacksonBytes, Map.class);

        assertEquals("Charlie", parsed.get("name"));
        @SuppressWarnings("unchecked")
        Map<String, Object> addr = (Map<String, Object>) parsed.get("address");
        assertEquals("NYC", addr.get("city"));
        assertEquals(10001, addr.get("zip"));
    }

    @Test
    void deepNestedStructSerializesToCleanJson() throws Exception {
        Schema coordsSchema = SchemaBuilder.struct()
                .field("lat", Schema.FLOAT64_SCHEMA)
                .field("lon", Schema.FLOAT64_SCHEMA)
                .build();

        Schema addressSchema = SchemaBuilder.struct()
                .field("city", Schema.STRING_SCHEMA)
                .field("zip", Schema.INT32_SCHEMA)
                .field("coords", coordsSchema)
                .build();

        Schema companySchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("hq", addressSchema)
                .build();

        Schema personSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("employer", companySchema)
                .build();

        Struct coords = new Struct(coordsSchema)
                .put("lat", 40.7128)
                .put("lon", -74.0060);

        Struct address = new Struct(addressSchema)
                .put("city", "NYC")
                .put("zip", 10001)
                .put("coords", coords);

        Struct company = new Struct(companySchema)
                .put("name", "Acme")
                .put("hq", address);

        Struct person = new Struct(personSchema)
                .put("name", "Eve")
                .put("age", 30)
                .put("employer", company);

        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(person);
        ObjectMapper reader = new ObjectMapper();
        Map<?, ?> parsed = reader.readValue(jacksonBytes, Map.class);

        assertEquals("Eve", parsed.get("name"));
        assertEquals(30, parsed.get("age"));

        @SuppressWarnings("unchecked")
        Map<String, Object> emp = (Map<String, Object>) parsed.get("employer");
        assertEquals("Acme", emp.get("name"));

        @SuppressWarnings("unchecked")
        Map<String, Object> hq = (Map<String, Object>) emp.get("hq");
        assertEquals("NYC", hq.get("city"));
        assertEquals(10001, hq.get("zip"));

        @SuppressWarnings("unchecked")
        Map<String, Object> c = (Map<String, Object>) hq.get("coords");
        assertEquals(40.7128, c.get("lat"));
        assertEquals(-74.0060, c.get("lon"));
    }

    @Test
    void deepNestedStructWithNullAtLeaf() throws Exception {
        Schema innerSchema = SchemaBuilder.struct()
                .field("value", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema middleSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("inner", innerSchema)
                .build();

        Schema outerSchema = SchemaBuilder.struct()
                .field("label", Schema.STRING_SCHEMA)
                .field("middle", middleSchema)
                .build();

        Struct inner = new Struct(innerSchema)
                .put("value", null);

        Struct middle = new Struct(middleSchema)
                .put("id", 7)
                .put("inner", inner);

        Struct outer = new Struct(outerSchema)
                .put("label", "test")
                .put("middle", middle);

        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(outer);
        ObjectMapper reader = new ObjectMapper();
        Map<?, ?> parsed = reader.readValue(jacksonBytes, Map.class);

        assertEquals("test", parsed.get("label"));

        @SuppressWarnings("unchecked")
        Map<String, Object> mid = (Map<String, Object>) parsed.get("middle");
        assertEquals(7, mid.get("id"));

        @SuppressWarnings("unchecked")
        Map<String, Object> inn = (Map<String, Object>) mid.get("inner");
        assertNull(inn.get("value"));
    }

    @Test
    void structWithNullFieldSerializesToCleanJson() throws Exception {
        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("name", "Dana")
                .put("age", null);

        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(struct);
        String json = new String(jacksonBytes, StandardCharsets.UTF_8);

        assertTrue(json.contains("\"name\":\"Dana\""));
        assertTrue(json.contains("\"age\":null"));
    }

    @Test
    void mapWithMixedNestedList() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("items", List.of(
                Map.of("id", 1, "label", "first"),
                Map.of("id", 2, "label", "second")
        ));

        byte[] gsonBytes = GSON.toJson(map, GSON_MAP_TYPE).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(map);

        assertArrayEquals(gsonBytes, jacksonBytes,
                "Map with nested list of maps should produce identical bytes");
    }

    /**
     * Mirrors the exact serialization pattern from doInsertJsonV1/V2:
     * Struct fields extracted into a HashMap via struct.get(field), then
     * serialized through cleanupExtraFields (identity for this test).
     */
    @Test
    void insertPathSimulation_flatStruct() throws Exception {
        Schema schema = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("score", Schema.FLOAT64_SCHEMA)
                .field("active", Schema.BOOLEAN_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("off16", (short) 42)
                .put("name", "test")
                .put("score", 88.5)
                .put("active", true);

        Map<String, Object> data = new LinkedHashMap<>();
        for (org.apache.kafka.connect.data.Field field : struct.schema().fields()) {
            data.put(field.name(), struct.get(field));
        }

        byte[] gsonBytes = GSON.toJson(data, GSON_MAP_TYPE).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(data);

        assertArrayEquals(gsonBytes, jacksonBytes,
                "Insert-path simulation with flat struct should produce identical bytes");
    }
}
