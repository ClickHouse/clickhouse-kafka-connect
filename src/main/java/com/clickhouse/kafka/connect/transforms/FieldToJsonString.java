package com.clickhouse.kafka.connect.transforms;

import com.clickhouse.kafka.connect.util.DataJson;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FieldToJsonString<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldToJsonString.class.getName());

    private static final String FIELDS_CONF = "fields";
    private static final String IGNORE_MISSING_CONF = "ignore.missing";
    private static final String SCHEMA_MAX_SIZE_CONF = "schema_cache_max_size";
    private static final int CACHE_MAX_SIZE_LOWER_BOUND = 16;
    private static final int CACHE_MAX_SIZE_HIGH_BOUND = 1000;
    private static final String CACHE_SIZE_RANGE =
            "[" + CACHE_MAX_SIZE_LOWER_BOUND + " , " + CACHE_MAX_SIZE_HIGH_BOUND + "]";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONF, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH,
                    "Comma-separated list of dot-notation field paths whose object/array value is replaced "
                            + "with its JSON string representation (e.g. clientContext,account.registrationData.marketingData.metadata).")
            .define(IGNORE_MISSING_CONF, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW,
                    "When true, a configured path that is missing or null is left untouched. "
                            + "When false, a missing intermediate node throws.")
            .define(SCHEMA_MAX_SIZE_CONF, ConfigDef.Type.INT, 32, ConfigDef.Importance.MEDIUM,
                    "Maximum number of value schemas to cache. Older values will be discarded. Value range is " + CACHE_SIZE_RANGE);

    private List<String[]> fieldPaths;
    private boolean ignoreMissing;
    private Cache<Schema, Schema> schemaUpdateCache;

    protected AtomicInteger cacheMisses = new AtomicInteger(0);

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        final List<String> rawFields = config.getList(FIELDS_CONF);
        if (rawFields == null || rawFields.isEmpty()) {
            throw new IllegalArgumentException(FIELDS_CONF + " must contain at least one field path");
        }
        fieldPaths = new ArrayList<>(rawFields.size());
        for (String raw : rawFields) {
            final String trimmed = raw.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalArgumentException(FIELDS_CONF + " must not contain empty field paths");
            }
            fieldPaths.add(trimmed.split("\\."));
        }
        ignoreMissing = config.getBoolean(IGNORE_MISSING_CONF);
        final int cacheMaxSize = config.getInt(SCHEMA_MAX_SIZE_CONF);
        if (cacheMaxSize < CACHE_MAX_SIZE_LOWER_BOUND || cacheMaxSize > CACHE_MAX_SIZE_HIGH_BOUND) {
            throw new IllegalArgumentException(SCHEMA_MAX_SIZE_CONF + " should be in range " + CACHE_SIZE_RANGE);
        }
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(cacheMaxSize));
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }
        if (record.valueSchema() == null) {
            return applySchemaless(record);
        }
        return applyWithSchema(record);
    }

    @SuppressWarnings("unchecked")
    private R applySchemaless(R record) {
        if (!(record.value() instanceof Map)) {
            throw new IllegalArgumentException(
                    "Schemaless record value must be a Map - make sure you're using the JSON Converter for value.");
        }
        final Map<String, Object> value = (Map<String, Object>) record.value();
        for (String[] path : fieldPaths) {
            stringifyInMap(value, path, 0);
        }
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                record.valueSchema(), value, record.timestamp());
    }

    @SuppressWarnings("unchecked")
    private void stringifyInMap(Map<String, Object> node, String[] path, int index) {
        final String key = path[index];
        final boolean isLeaf = index == path.length - 1;
        if (!node.containsKey(key)) {
            if (!ignoreMissing) {
                throw new DataException("Field path segment '" + key + "' not found in record value");
            }
            return;
        }
        final Object child = node.get(key);
        if (isLeaf) {
            if (child == null) {
                return;
            }
            if (child instanceof String) {
                return;
            }
            node.put(key, toJsonString(child));
            return;
        }
        if (child == null) {
            return;
        }
        if (!(child instanceof Map)) {
            if (!ignoreMissing) {
                throw new DataException("Field path segment '" + key + "' is not an object");
            }
            return;
        }
        stringifyInMap((Map<String, Object>) child, path, index + 1);
    }

    private R applyWithSchema(R record) {
        final Struct value = (Struct) record.value();
        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = buildUpdatedSchema(value.schema(), 0);
            schemaUpdateCache.put(value.schema(), updatedSchema);
            cacheMisses.incrementAndGet();
        }
        final Struct updatedValue = buildUpdatedStruct(value, updatedSchema);
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                updatedSchema, updatedValue, record.timestamp());
    }

    private boolean pathMatchesLeaf(String[] path, int depth, String fieldName) {
        return depth == path.length - 1 && path[depth].equals(fieldName);
    }

    private boolean pathMatchesIntermediate(String[] path, int depth, String fieldName) {
        return depth < path.length - 1 && path[depth].equals(fieldName);
    }

    private Schema buildUpdatedSchema(Schema schema, int depth) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        if (schema.name() != null) {
            builder.name(schema.name());
        }
        if (schema.version() != null) {
            builder.version(schema.version());
        }
        if (schema.doc() != null) {
            builder.doc(schema.doc());
        }
        if (schema.isOptional()) {
            builder.optional();
        }
        for (Field field : schema.fields()) {
            Schema fieldSchema = field.schema();
            boolean isLeaf = false;
            boolean descend = false;
            for (String[] path : fieldPaths) {
                if (pathMatchesLeaf(path, depth, field.name())) {
                    isLeaf = true;
                } else if (pathMatchesIntermediate(path, depth, field.name())
                        && fieldSchema.type() == Schema.Type.STRUCT) {
                    descend = true;
                }
            }
            if (isLeaf) {
                fieldSchema = field.schema().isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
            } else if (descend) {
                fieldSchema = buildUpdatedSchema(field.schema(), depth + 1);
            }
            builder.field(field.name(), fieldSchema);
        }
        return builder.build();
    }

    private Struct buildUpdatedStruct(Struct source, Schema targetSchema) {
        final Struct target = new Struct(targetSchema);
        for (Field field : source.schema().fields()) {
            final Object sourceValue = source.get(field);
            final Field targetField = targetSchema.field(field.name());
            if (sourceValue == null) {
                target.put(targetField, null);
                continue;
            }
            if (targetField.schema().type() == Schema.Type.STRING
                    && field.schema().type() != Schema.Type.STRING) {
                target.put(targetField, toJsonString(sourceValue));
            } else if (targetField.schema().type() == Schema.Type.STRUCT
                    && field.schema().type() == Schema.Type.STRUCT) {
                target.put(targetField, buildUpdatedStruct((Struct) sourceValue, targetField.schema()));
            } else {
                target.put(targetField, sourceValue);
            }
        }
        return target;
    }

    private String toJsonString(Object value) {
        try {
            return DataJson.OBJECT_MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new DataException("Failed to serialize field value to JSON string", e);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    public static class SimpleConfig extends AbstractConfig {
        public SimpleConfig(ConfigDef configDef, Map<?, ?> originals) {
            super(configDef, originals, false);
        }
    }
}
