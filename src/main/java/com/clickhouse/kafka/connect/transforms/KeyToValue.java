package com.clickhouse.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.common.config.AbstractConfig;
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
        final Map<String, Object> value = (Map<String, Object>) record.value();
        value.put(keyFieldName, record.key());
        LOGGER.debug("New schemaless value: {}", value);
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), value, record.timestamp());
    }

    private R applyWithSchema(R record) {
        final Struct value = (Struct) record.value();

        if (valueSchema == null) {
            final SchemaBuilder builder = SchemaBuilder.struct();
            builder.name(value.schema().name());
            builder.version(value.schema().version());
            builder.doc(value.schema().doc());
            value.schema().fields().forEach(f -> {
                builder.field(f.name(), f.schema());
            });
            builder.field(keyFieldName, record.keySchema() == null ? Schema.OPTIONAL_STRING_SCHEMA : record.keySchema());
            valueSchema = builder.build();
        }

        value.put(keyFieldName, record.key());
        LOGGER.debug("New schema value: {}", value);
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), valueSchema, value, record.timestamp());
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
