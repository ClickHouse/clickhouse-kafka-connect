package com.clickhouse.kafka.connect.sink.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class StructToJsonMapTest {

    @Test
    public void optionalMapFieldNullDoesNotThrow() {
        Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build();
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("metadata", mapSchema)
                .build();

        Struct struct = new Struct(schema)
                .put("id", "1")
                .put("metadata", null);

        Map<String, Data> converted = StructToJsonMap.toJsonMap(struct);
        assertEquals("1", converted.get("id").getObject());
        assertNotNull(converted.get("metadata"));
        assertNull(converted.get("metadata").getObject());
        assertEquals(Schema.Type.MAP, converted.get("metadata").getFieldType());
    }

    @Test
    public void optionalMapFieldPresent() {
        Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build();
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("metadata", mapSchema)
                .build();

        Struct struct = new Struct(schema)
                .put("id", "1")
                .put("metadata", Map.of("k", "v"));

        Map<String, Data> converted = StructToJsonMap.toJsonMap(struct);
        @SuppressWarnings("unchecked")
        Map<Object, Object> metadata = (Map<Object, Object>) converted.get("metadata").getObject();
        assertEquals("v", metadata.get("k"));
    }
}
