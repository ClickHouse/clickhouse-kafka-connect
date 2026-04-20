package com.clickhouse.kafka.connect.transforms;

import com.clickhouse.kafka.connect.transforms.KeyToValue;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;

public class KeyToValueTest {
    @Test
    public void applySchemalessTest() {
        try(KeyToValue<SinkRecord> keyToValue = new KeyToValue<>()) {
            keyToValue.configure(new HashMap<>());
            SinkRecord record = new SinkRecord(UUID.randomUUID().toString(), 0, null, "Sample Key", null, new HashMap<>(), 0);
            SinkRecord newRecord = keyToValue.apply(record);
            Assertions.assertTrue(newRecord.value() instanceof HashMap && ((HashMap) newRecord.value()).containsKey("_key"));
        }
    }

    @Test
    public void applyWithSchemaTest() {
        try(KeyToValue<SinkRecord> keyToValue = new KeyToValue<>()) {
            keyToValue.configure(new HashMap<>());
            SinkRecord record = generateSampleRecord(UUID.randomUUID().toString(), 0, 0);
            SinkRecord newRecord = keyToValue.apply(record);
            Assertions.assertTrue(newRecord.value() instanceof Struct &&
                    ((Struct) newRecord.value()).get("off16") instanceof Short &&
                    ((Struct) newRecord.value()).get("_key") != null);
        }
    }



    @Test
    public void applyWithDifferentSchemasTest() {
        try(KeyToValue<SinkRecord> keyToValue = new KeyToValue<>()) {
            keyToValue.configure(new HashMap<>());

            SinkRecord record1 = generateSampleRecord("topic1", 0, 0);
            SinkRecord newRecord1 = keyToValue.apply(record1);

            Schema schema2 = SchemaBuilder.struct()
                    .field("string_field", Schema.STRING_SCHEMA)
                    .build();
            Struct valueStruct2 = new Struct(schema2)
                    .put("string_field", "value");
            SinkRecord record2 = new SinkRecord("topic2", 0, null, "key2", schema2, valueStruct2, 1);
            SinkRecord newRecord2 = keyToValue.apply(record2);

            Assertions.assertTrue(newRecord1.value() instanceof Struct);
            Assertions.assertNotNull(((Struct) newRecord1.value()).get("_key"));
            Assertions.assertNotNull(((Struct) newRecord1.value()).schema().field("off16"));

            Assertions.assertTrue(newRecord2.value() instanceof Struct);
            Assertions.assertNotNull(((Struct) newRecord2.value()).get("_key"));
            Assertions.assertNotNull(((Struct) newRecord2.value()).schema().field("string_field"));
            Assertions.assertNull(((Struct) newRecord2.value()).schema().field("off16"));

            Assertions.assertNotEquals(((Struct) newRecord1.value()).schema(), ((Struct) newRecord2.value()).schema());
        }
    }

    @Test
    public void applyWithSchemaEvolutionOnSameTopicTest() {
        try(KeyToValue<SinkRecord> keyToValue = new KeyToValue<>()) {
            keyToValue.configure(new HashMap<>());

            String topic = "evolution_topic";

            // First schema version - v1 with two fields
            Schema schemaV1 = SchemaBuilder.struct()
                    .name("com.example.Record")
                    .version(1)
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .build();
            Struct valueV1 = new Struct(schemaV1)
                    .put("id", 1)
                    .put("name", "Alice");
            SinkRecord recordV1 = new SinkRecord(topic, 0, null, "key1", schemaV1, valueV1, 0);
            SinkRecord transformedV1 = keyToValue.apply(recordV1);

            // Second schema version - v2 adds a new field (schema evolution)
            Schema schemaV2 = SchemaBuilder.struct()
                    .name("com.example.Record")
                    .version(2)
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .field("email", Schema.OPTIONAL_STRING_SCHEMA)
                    .build();
            Struct valueV2 = new Struct(schemaV2)
                    .put("id", 2)
                    .put("name", "Bob")
                    .put("email", "bob@example.com");
            SinkRecord recordV2 = new SinkRecord(topic, 0, null, "key2", schemaV2, valueV2, 1);
            SinkRecord transformedV2 = keyToValue.apply(recordV2);

            // Third record with v1 schema again (testing cache with different versions)
            Struct valueV1_2 = new Struct(schemaV1)
                    .put("id", 3)
                    .put("name", "Charlie");
            SinkRecord recordV1_2 = new SinkRecord(topic, 0, null, "key3", schemaV1, valueV1_2, 2);
            SinkRecord transformedV1_2 = keyToValue.apply(recordV1_2);

            // Validate V1 transformation
            Assertions.assertTrue(transformedV1.value() instanceof Struct);
            Struct v1Result = (Struct) transformedV1.value();
            Assertions.assertEquals("key1", v1Result.get("_key"));
            Assertions.assertEquals(1, v1Result.get("id"));
            Assertions.assertEquals("Alice", v1Result.get("name"));
            Assertions.assertNull(v1Result.schema().field("email"));

            // Validate V2 transformation
            Assertions.assertTrue(transformedV2.value() instanceof Struct);
            Struct v2Result = (Struct) transformedV2.value();
            Assertions.assertEquals("key2", v2Result.get("_key"));
            Assertions.assertEquals(2, v2Result.get("id"));
            Assertions.assertEquals("Bob", v2Result.get("name"));
            Assertions.assertEquals("bob@example.com", v2Result.get("email"));

            // Validate V1 (second record) transformation
            Assertions.assertTrue(transformedV1_2.value() instanceof Struct);
            Struct v1_2Result = (Struct) transformedV1_2.value();
            Assertions.assertEquals("key3", v1_2Result.get("_key"));
            Assertions.assertEquals(3, v1_2Result.get("id"));
            Assertions.assertEquals("Charlie", v1_2Result.get("name"));
            Assertions.assertNull(v1_2Result.schema().field("email"));

            // Verify that both V1 transformations use the same cached schema
            Assertions.assertEquals(transformedV1.valueSchema(), transformedV1_2.valueSchema());

            // Verify that V1 and V2 schemas are different
            Assertions.assertNotEquals(transformedV1.valueSchema(), transformedV2.valueSchema());
        }
    }

    private SinkRecord generateSampleRecord(String topic, int partition, long offset) {
        Schema schema = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("timestamp_int64",  Timestamp.SCHEMA)
                .field("date_date", Time.SCHEMA)
                .build();
        Struct value_struct = new Struct(schema)
                .put("off16", (short) new Random().nextInt(Short.MAX_VALUE + 1))
                .put("timestamp_int64", new Date(System.currentTimeMillis()))
                .put("date_date", new Date(System.currentTimeMillis()));
        return new SinkRecord(topic, partition, null, "{\"sample\": \"keys\"}", schema, value_struct, offset);
    }
}
