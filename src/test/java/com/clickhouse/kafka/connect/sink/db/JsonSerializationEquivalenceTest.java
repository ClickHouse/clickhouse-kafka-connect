package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.kafka.connect.util.DataJson;
import com.fasterxml.jackson.databind.JsonNode;
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
 * for all Map shapes used in the JSON insert paths (doInsertJsonV1/V2),
 * and semantically equivalent output for Struct serialization.
 *
 * @author Gaurav Miglani
 */
public class JsonSerializationEquivalenceTest {

    private static final Gson GSON = new Gson();
    private static final java.lang.reflect.Type GSON_MAP_TYPE =
            new TypeToken<HashMap<String, Object>>() {}.getType();

    /** DataJson.GSON has the StructSerializer registered (via StructToJsonMap). */
    private static final Gson STRUCT_GSON = DataJson.GSON;

    /** Shared reader for JSON tree comparisons. */
    private static final ObjectMapper JSON_READER = new ObjectMapper();

    /**
     * Asserts that two JSON byte arrays are semantically equivalent (same
     * key-value pairs regardless of field order).  Gson's StructSerializer
     * converts Struct fields via StructToJsonMap into a HashMap whose
     * iteration order is undefined, while Jackson iterates in schema-defined
     * order.  The JSON is semantically identical — ClickHouse JSONEachRow
     * matches by field name, not position.
     */
    private static void assertJsonEquivalent(byte[] gsonBytes, byte[] jacksonBytes, String message) throws Exception {
        JsonNode gsonTree = JSON_READER.readTree(gsonBytes);
        JsonNode jacksonTree = JSON_READER.readTree(jacksonBytes);
        assertEquals(gsonTree, jacksonTree, message);
    }

    // ── Map equivalence tests (byte-identical) ──────────────────────────

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
     * Both Gson and Jackson (configured with NON_NULL) drop null values from
     * maps. This preserves backward compatibility: ClickHouse will use the
     * column DEFAULT for missing fields rather than inserting an explicit NULL.
     */
    @Test
    void nullMapValueHandling() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", "Alice");
        map.put("optional", null);

        byte[] gsonBytes = GSON.toJson(map, GSON_MAP_TYPE).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(map);

        assertArrayEquals(gsonBytes, jacksonBytes,
                "Both Gson and Jackson should drop null values from maps");
        String json = new String(jacksonBytes, StandardCharsets.UTF_8);
        assertFalse(json.contains("optional"),
                "Null map values should be omitted");
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

    // ── Struct equivalence tests (Gson DataJson.GSON vs Jackson) ────────

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

        byte[] gsonBytes = STRUCT_GSON.toJson(struct).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(struct);

        assertJsonEquivalent(gsonBytes, jacksonBytes,
                "Flat struct: Gson StructSerializer and Jackson JacksonStructSerializer should produce equivalent JSON");

        Map<?, ?> parsed = JSON_READER.readValue(jacksonBytes, Map.class);
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

        byte[] gsonBytes = STRUCT_GSON.toJson(person).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(person);

        assertJsonEquivalent(gsonBytes, jacksonBytes,
                "Nested struct: Gson and Jackson should produce equivalent JSON");

        Map<?, ?> parsed = JSON_READER.readValue(jacksonBytes, Map.class);
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

        byte[] gsonBytes = STRUCT_GSON.toJson(person).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(person);

        assertJsonEquivalent(gsonBytes, jacksonBytes,
                "Deep nested struct: Gson and Jackson should produce equivalent JSON");

        Map<?, ?> parsed = JSON_READER.readValue(jacksonBytes, Map.class);
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

        byte[] gsonBytes = STRUCT_GSON.toJson(outer).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(outer);

        // Behavioral difference for null Struct fields:
        // Gson's StructSerializer (via StructToJsonMap) drops null fields,
        // producing {"inner":{}} for a struct where all fields are null.
        // Jackson writes them explicitly: {"inner":{"value":null}}.
        // See structWithNullFieldSerializesToCleanJson for rationale.
        JsonNode gsonTree = JSON_READER.readTree(gsonBytes);
        JsonNode jacksonTree = JSON_READER.readTree(jacksonBytes);

        // Both agree on all non-null fields
        assertEquals("test", jacksonTree.get("label").asText());
        assertEquals("test", gsonTree.get("label").asText());
        assertEquals(7, jacksonTree.get("middle").get("id").asInt());
        assertEquals(7, gsonTree.get("middle").get("id").asInt());

        // Jackson preserves null explicitly; Gson drops it
        assertTrue(jacksonTree.get("middle").get("inner").has("value"),
                "Jackson retains null field in nested struct");
        assertTrue(jacksonTree.get("middle").get("inner").get("value").isNull());
        assertFalse(gsonTree.get("middle").get("inner").has("value"),
                "Gson drops null field from nested struct");
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

        byte[] gsonBytes = STRUCT_GSON.toJson(struct).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(struct);

        // Behavioral difference: Gson's StructSerializer (via StructToJsonMap)
        // drops null fields, producing {"name":"Dana"}. Jackson's
        // JacksonStructSerializer writes them explicitly, producing
        // {"name":"Dana","age":null}. Both are valid for ClickHouse:
        //   - Omitted field → column DEFAULT (null for Nullable with no default)
        //   - Explicit null  → null value in column
        // Jackson's explicit nulls are the safer choice: they prevent a
        // Nullable column's non-null DEFAULT from silently overriding a
        // user's intentional null.
        String gsonJson = new String(gsonBytes, StandardCharsets.UTF_8);
        String jacksonJson = new String(jacksonBytes, StandardCharsets.UTF_8);

        assertFalse(gsonJson.contains("age"),
                "Gson drops null struct fields");
        assertTrue(jacksonJson.contains("\"age\":null"),
                "Jackson writes null struct fields explicitly");
        assertTrue(jacksonJson.contains("\"name\":\"Dana\""));
    }

    // ── Map equivalence continued ───────────────────────────────────────

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

    // ── Insert-path simulations (mirrors doInsertJsonV1/V2) ─────────────

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

    /**
     * Simulates the doInsertJsonV1/V2 path for a Struct with a nested Struct
     * field (e.g. a Tuple column).  The outer fields are extracted into a
     * LinkedHashMap, but the nested Struct stays as-is — Jackson serializes
     * it via JacksonStructSerializer while DataJson.GSON uses StructSerializer.
     */
    @Test
    void insertPathSimulation_nestedStruct() throws Exception {
        Schema tupleSchema = SchemaBuilder.struct()
                .field("off16", Schema.OPTIONAL_INT16_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema topSchema = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("t", tupleSchema)
                .build();

        Struct tuple = new Struct(tupleSchema)
                .put("off16", (short) 7)
                .put("string", "inner");

        Struct top = new Struct(topSchema)
                .put("off16", (short) 1)
                .put("string", "outer")
                .put("t", tuple);

        // Mirror the insert path: extract top-level fields into a map
        Map<String, Object> data = new LinkedHashMap<>();
        for (org.apache.kafka.connect.data.Field field : top.schema().fields()) {
            data.put(field.name(), top.get(field));
        }

        byte[] gsonBytes = STRUCT_GSON.toJson(data).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(data);

        assertJsonEquivalent(gsonBytes, jacksonBytes,
                "Insert-path simulation with nested struct should produce equivalent JSON");
    }

    /**
     * Same as above but with a deeply nested Struct (Tuple inside Tuple).
     */
    @Test
    void insertPathSimulation_deepNestedStruct() throws Exception {
        Schema innerTupleSchema = SchemaBuilder.struct()
                .field("off16", Schema.OPTIONAL_INT16_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema tupleSchema = SchemaBuilder.struct()
                .field("off16", Schema.OPTIONAL_INT16_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("n", innerTupleSchema)
                .build();

        Schema topSchema = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("t", tupleSchema)
                .build();

        Struct innerTuple = new Struct(innerTupleSchema)
                .put("off16", (short) 99)
                .put("string", "leaf");

        Struct tuple = new Struct(tupleSchema)
                .put("off16", (short) 7)
                .put("string", "middle")
                .put("n", innerTuple);

        Struct top = new Struct(topSchema)
                .put("off16", (short) 1)
                .put("string", "outer")
                .put("t", tuple);

        Map<String, Object> data = new LinkedHashMap<>();
        for (org.apache.kafka.connect.data.Field field : top.schema().fields()) {
            data.put(field.name(), top.get(field));
        }

        byte[] gsonBytes = STRUCT_GSON.toJson(data).getBytes(StandardCharsets.UTF_8);
        byte[] jacksonBytes = OBJECT_MAPPER.writeValueAsBytes(data);

        assertJsonEquivalent(gsonBytes, jacksonBytes,
                "Insert-path simulation with deep nested struct should produce equivalent JSON");
    }
}