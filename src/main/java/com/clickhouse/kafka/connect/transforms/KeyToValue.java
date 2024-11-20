package com.clickhouse.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KeyToValue<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyToValue.class.getName());
    public static final ConfigDef CONFIG_DEF = new ConfigDef().define("field", ConfigDef.Type.STRING, "_key", ConfigDef.Importance.LOW,
                    "Field name on the record value to extract the record key into.");

    private String keyFieldName;
    private Schema valueSchema;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        keyFieldName = config.getString("field");
    }

    @Override
    public R apply(R record) {
        LOGGER.debug("Old Key: {}, Old Value: {}", record.key(), record.value());
        if (record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        if (!(record.value() instanceof Map)) {
            throw new IllegalArgumentException("Schemaless record value must be a Map - make sure you're using the JSON Converter for value.");
        }

        final Map<String, Object> value = (Map<String, Object>) record.value();
        value.put(keyFieldName, record.key());
        LOGGER.debug("New schemaless value: {}", value);
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), value, record.timestamp());
    }

    private R applyWithSchema(R record) {
        final Struct oldValue = (Struct) record.value();

        if (valueSchema == null) {
            final SchemaBuilder builder = SchemaBuilder.struct();
            builder.name(oldValue.schema().name());
            builder.version(oldValue.schema().version());
            builder.doc(oldValue.schema().doc());
            oldValue.schema().fields().forEach(f -> {
                builder.field(f.name(), f.schema());
            });
            builder.field(keyFieldName, record.keySchema() == null ? Schema.OPTIONAL_STRING_SCHEMA : record.keySchema());
            valueSchema = builder.build();
            valueSchema.schema().fields().forEach(f -> LOGGER.debug("Field: {}", f));
        }

        Struct newValue = new Struct(valueSchema);
        valueSchema.fields().forEach(f -> {
            if (f.name().equals(keyFieldName)) {
                newValue.put(f, record.key());
            } else {
                newValue.put(f, oldValue.get(f));
            }
        });
        LOGGER.debug("New schema value: {}", newValue);
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), valueSchema, newValue, record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        valueSchema = null;
    }

    public static class SimpleConfig extends AbstractConfig {
        public SimpleConfig(ConfigDef configDef, Map<?, ?> originals) {
            super(configDef, originals, false);
        }
    }
}
