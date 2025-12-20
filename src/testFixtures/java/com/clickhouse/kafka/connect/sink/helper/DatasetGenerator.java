package com.clickhouse.kafka.connect.sink.helper;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatasetGenerator {

    private final ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
    private final ObjectMapper json = new ObjectMapper(new JsonFactory());

    public DatasetGenerator() {
    }

    public ObjectNode loadDatasetFromResources(String resourceName) {
        try (InputStream in = this.getClass().getResourceAsStream(resourceName)) {

            return yaml.readValue(in, ObjectNode.class);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Dataset generateDataset(ObjectNode dataset) {
        Dataset data = new Dataset();
        // Table definition
        StringBuilder tableDefinition = new StringBuilder();
        tableDefinition.append("CREATE TABLE ");
        tableDefinition.append("`{tableName}`");
        tableDefinition.append(" (");
        dataset.get("database").get("columns").forEach(column -> {
            tableDefinition.append(column.get("name").asText());
            tableDefinition.append(" ");
            tableDefinition.append(column.get("type").asText());
            tableDefinition.append(", ");
        });
        tableDefinition.setLength(tableDefinition.length() - 2);
        tableDefinition.append(") ENGINE = MergeTree ORDER BY ()");

        data.setTableDefinition(tableDefinition.toString());

        // Kafka Connect Schema
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        dataset.get("message").get("fields").forEach(column -> {
            schemaBuilder.field(column.get("name").asText(), SchemaBuilder.type(Schema.Type.valueOf(column.get("type").asText())).build());
        });

        data.setSchema(schemaBuilder.build());

        // Avro Schema
        org.apache.avro.SchemaBuilder.RecordBuilder<org.apache.avro.Schema> recordBuilder = org.apache.avro.SchemaBuilder.record("Event").namespace("com.clickhouse.test");
        org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fieldAccessor = recordBuilder.fields();
        for (JsonNode column : dataset.get("message").get("fields")) {
            Schema.Type type = Schema.Type.valueOf(column.get("type").asText());
            String avroTypeName;
            switch (type) {
                case INT8:
                case INT16:
                case INT32:
                    avroTypeName = org.apache.avro.Schema.Type.INT.getName();
                    break;
                case INT64:
                    avroTypeName = org.apache.avro.Schema.Type.LONG.getName();
                    break;
                case BOOLEAN:
                    avroTypeName = org.apache.avro.Schema.Type.BOOLEAN.getName();
                    break;
                case FLOAT32:
                    avroTypeName = org.apache.avro.Schema.Type.FLOAT.getName();
                    break;
                case FLOAT64:
                    avroTypeName = org.apache.avro.Schema.Type.DOUBLE.getName();
                    break;

                case STRING:
                    avroTypeName = org.apache.avro.Schema.Type.STRING.getName();
                    break;
                default:
                    throw new RuntimeException("Type " + type + " is not supported yet");
            }
            fieldAccessor = fieldAccessor.name(column.get("name").asText()).type(avroTypeName).noDefault();
        }
        data.setAvroSchema(fieldAccessor.endRecord());

        // Protobuf Schema
        final String msgTypeName = "Event";
        DescriptorProtos.DescriptorProto.Builder descriptorProtoBuilder = DescriptorProtos.DescriptorProto.newBuilder()
                .setName(msgTypeName);

        for (Field field : data.getSchema().fields()) {
            Schema.Type type = field.schema().type();
            DescriptorProtos.FieldDescriptorProto.Type protoType;
            switch (type) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                    protoType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32;
                    break;
                case BOOLEAN:
                    protoType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
                    break;
                case FLOAT32:
                    protoType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT;
                    break;
                case FLOAT64:
                    protoType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
                    break;
                case STRING:
                    protoType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
                    break;
                default:
                    throw new RuntimeException("Type " + type + " is not supported yet");
            }
            descriptorProtoBuilder.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName(field.name())
                    .setNumber(field.index() + 1)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED)
                    .setType(protoType)
                    .build());
        }

        DescriptorProtos.FileDescriptorProto fileDescriptorProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("event_schema.proto")
                .addMessageType(descriptorProtoBuilder.build())
                .setPackage("com.clickhouse.kafka.test")
                .build();

        try {
            Descriptors.FileDescriptor dynamicFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, new Descriptors.FileDescriptor[]{});
            Descriptors.Descriptor msgDescriptor = dynamicFileDescriptor.findMessageTypeByName(msgTypeName);

            data.setProtobufSchema(msgDescriptor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return data;
    }

    public Map<String, Object> generateRecordValue(int offset, Schema schema) {
        Map<String, Object> value = new HashMap<>();

        schema.fields().forEach(field -> {
            switch (field.schema().type()) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                    value.put(field.name(), offset);
                    break;
                case BOOLEAN:
                    value.put(field.name(), offset % 2 == 0);
                    break;
                case FLOAT32:
                case FLOAT64:
                    value.put(field.name(), offset * 10.123456);
                    break;
                case STRING:
                    value.put(field.name(), "value_" + offset);
                    break;
                default:
                    throw new RuntimeException("Type " + field.schema().type() + " is not supported yet");
            }
        });

        return value;
    }

    public GenericData.Record generateAvroRecord(int offset, org.apache.avro.Schema schema) {
        GenericData.Record record = new GenericData.Record(schema);
        schema.getFields().forEach(field -> {
            switch (field.schema().getType()) {
                case INT:
                case LONG:
                    record.put(field.name(), offset);
                    break;
                case BOOLEAN:
                    record.put(field.name(), offset % 2 == 0);
                case FLOAT:
                case DOUBLE:
                    record.put(field.name(), offset * 10.123456);
                case STRING:
                    record.put(field.name(), "value_" + offset);
                    break;
                default:
                    throw new RuntimeException("Type " + field.schema().getType() + " is not supported yet");
            }
        });
        return record;
    }

    public Message generateProtobufMessage(int offset, Descriptors.Descriptor descriptor) {
        DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor);

        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            Object value;
            switch (field.getType()) {
                case UINT64:
                case UINT32:
                case INT64:
                case INT32:
                    value = offset;
                    break;
                case BOOL:
                    value = offset % 2 == 0;
                    break;
                case FLOAT:
                    value = offset * 10.123f;
                    break;
                case DOUBLE:
                    value = offset * 10.123456d;
                    break;
                case STRING:
                    value = "value_" + offset;
                    break;
                default:
                    throw new RuntimeException("Type " + field.getType() + " is not supported yet");
            }
            message.setField(field, value);
        }
        return message.build();
    }

    public class Dataset {
        private String tableDefinition;
        private Schema schema;
        private org.apache.avro.Schema avroSchema;
        private Descriptors.Descriptor protobufSchema;

        public Dataset() {
        }

        public Schema getSchema() {
            return schema;
        }

        public void setSchema(Schema schema) {
            this.schema = schema;
        }

        public String getTableDefinition(String tableName) {
            return tableDefinition.replace("{tableName}", tableName);
        }

        public void setTableDefinition(String tableDefinition) {
            this.tableDefinition = tableDefinition;
        }

        public List<SinkRecord> generateSinkRecords(int count, String topic, int partition) {
            List<SinkRecord> records = new ArrayList<>(count);

            for (int i = 0; i < count; i++) {
                records.add(new SinkRecord(topic, partition, null, null, schema, generateRecordValue(i, schema), i));
            }

            return records;
        }

        public List<GenericRecord> generateAvroRecords(int count) {
            List<GenericRecord> records = new ArrayList<>(count);

            for (int i = 0; i < count; i++) {
                records.add(generateAvroRecord(i, avroSchema));
            }

            return records;
        }

        public List<Message> generateProtobufMessages(int count) {
            List<Message> records = new ArrayList<>(count);

            for (int i = 0; i < count; i++) {
                records.add(generateProtobufMessage(i, protobufSchema));
            }

            return records;
        }

        public List<ProducerRecord<String, String>> generateStringProducerRecords(String topic, int count) {
            List<ProducerRecord<String, String>> records = new ArrayList<>(count);

            try {
                for (int i = 0; i < count; i++) {
                    Map<String, Object> value = generateRecordValue(i, schema);
                    records.add(new ProducerRecord<>(topic, "id_" + i, json.writeValueAsString(value)));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            return records;
        }

        @Override
        public String toString() {
            return "Dataset [tableDefinition=\"" + tableDefinition + "\n, schema=" + schema + "]";
        }

        public void setAvroSchema(org.apache.avro.Schema avroSchema) {
            this.avroSchema = avroSchema;
        }

        public org.apache.avro.Schema getAvroSchema() {
            return avroSchema;
        }

        public void setProtobufSchema(Descriptors.Descriptor protobufSchema) {
            this.protobufSchema = protobufSchema;
        }

        public Descriptors.Descriptor getProtobufSchema() {
            return protobufSchema;
        }
    }

    public static void main(String[] args) {
        DatasetGenerator generator = new DatasetGenerator();
        ObjectNode dataset = generator.loadDatasetFromResources("/data01.yaml");
        System.out.println(dataset);
        Dataset data = generator.generateDataset(dataset);
        System.out.println(data);
        System.out.println("Sink Data -----");
        data.generateSinkRecords(10, "test", 0).forEach(System.out::println);
        System.out.println("Avro Data -----");
        data.generateAvroRecords(10).forEach(System.out::println);
        System.out.println("Protobuf Data -----");
        data.generateProtobufMessages(10).forEach(System.out::println);
        System.out.println("String Data -----");
        data.generateStringProducerRecords("test", 10).forEach(System.out::println);
        System.out.println("Example table CREATE stmt ----");
        System.out.println(data.getTableDefinition("test_table_01"));
    }
}
