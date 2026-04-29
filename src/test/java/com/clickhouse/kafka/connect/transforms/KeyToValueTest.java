package com.clickhouse.kafka.connect.transforms;

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
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.SchemaAndValue;

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

    @Test
    public void testKeyTypeChange() {
        try(KeyToValue<SinkRecord> keyToValue = new KeyToValue<>()) {
            keyToValue.configure(new HashMap<>());

            Schema valueSchema = SchemaBuilder.struct()
                    .field("string_field", Schema.STRING_SCHEMA)
                    .build();
            Struct valueStruct = new Struct(valueSchema)
                    .put("string_field", "value");

            SinkRecord record1 = new SinkRecord("topic1", 0, Schema.STRING_SCHEMA, "key1", valueSchema, valueStruct, 0);
            SinkRecord newRecord1 = keyToValue.apply(record1);

            SinkRecord record2 = new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 123, valueSchema, valueStruct, 1);
            SinkRecord newRecord2 = keyToValue.apply(record2);

            Assertions.assertEquals("key1", ((Struct) newRecord1.value()).get("_key"));
            Assertions.assertEquals(123, ((Struct) newRecord2.value()).get("_key"));
        }
    }

    @Test
    public void applyWithEvolutionAvroSchemasTest() throws Exception {
        String topic = "test_evolution_topic";
        MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();

        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder.record("TestRecord")
                .fields()
                .name("string_field").type().stringType().noDefault()
                .endRecord();

        org.apache.avro.Schema avroSchema2 = org.apache.avro.SchemaBuilder.record("TestRecord")
                .fields()
                .name("string_field").type().stringType().noDefault()
                .name("string_field2").type().optional().stringType()
                .endRecord();

        schemaRegistry.register(topic + "-value", new AvroSchema(avroSchema1));
        schemaRegistry.register(topic + "-value", new AvroSchema(avroSchema2));

        AvroConverter converter = new AvroConverter(schemaRegistry);
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test-url");
        converter.configure(converterConfig, false);

        KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);

        GenericRecord record1 = new GenericData.Record(avroSchema1);
        record1.put("string_field", "value1");

        GenericRecord record2 = new GenericData.Record(avroSchema2);
        record2.put("string_field", "value2");
        record2.put("string_field2", "value3");

        byte[] bytes1 = serializer.serialize(topic, record1);
        SchemaAndValue sav1 = converter.toConnectData(topic, bytes1);
        SinkRecord sinkRecord1 = new SinkRecord(topic, 0, null, "{\"key1\": \"val1\"}", sav1.schema(), sav1.value(), 0);

        byte[] bytes2 = serializer.serialize(topic, record2);
        SchemaAndValue sav2 = converter.toConnectData(topic, bytes2);
        SinkRecord sinkRecord2 = new SinkRecord(topic, 0, null, "{\"key2\": \"val2\"}", sav2.schema(), sav2.value(), 1);

        try(KeyToValue<SinkRecord> keyToValue = new KeyToValue<>()) {
            keyToValue.configure(new HashMap<>());
            SinkRecord newRecord1 = keyToValue.apply(sinkRecord1);
            SinkRecord newRecord2 = keyToValue.apply(sinkRecord2);

            for (int i = 0; i < 10; i++) {
                keyToValue.apply(sinkRecord1);
                keyToValue.apply(sinkRecord2);
            }

            Assertions.assertEquals(2, keyToValue.cacheMisses.get());

            Assertions.assertTrue(newRecord1.value() instanceof Struct);
            Assertions.assertNotNull(((Struct) newRecord1.value()).get("_key"));
            Assertions.assertEquals("{\"key1\": \"val1\"}", ((Struct) newRecord1.value()).get("_key"));
            Assertions.assertNotNull(((Struct) newRecord1.value()).schema().field("string_field"));
            Assertions.assertNull(((Struct) newRecord1.value()).schema().field("string_field2"));

            Assertions.assertTrue(newRecord2.value() instanceof Struct);
            Assertions.assertNotNull(((Struct) newRecord2.value()).get("_key"));
            Assertions.assertEquals("{\"key2\": \"val2\"}", ((Struct) newRecord2.value()).get("_key"));
            Assertions.assertNotNull(((Struct) newRecord2.value()).schema().field("string_field"));
            Assertions.assertNotNull(((Struct) newRecord2.value()).schema().field("string_field2"));

            Assertions.assertNotEquals(((Struct) newRecord1.value()).schema(), ((Struct) newRecord2.value()).schema());
        }
    }
}
