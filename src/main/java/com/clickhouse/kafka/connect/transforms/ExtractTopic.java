/*
 * Copyright 2019 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//See https://github.com/Aiven-Open/transforms-for-apache-kafka-connect/tree/master

package com.clickhouse.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class ExtractTopic<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final List<Schema.Type> SUPPORTED_SCHEMA_TYPES_TO_CONVERT_FROM = Arrays.asList(
        Schema.Type.INT8,
        Schema.Type.INT16,
        Schema.Type.INT32,
        Schema.Type.INT64,
        Schema.Type.FLOAT32,
        Schema.Type.FLOAT64,
        Schema.Type.BOOLEAN,
        Schema.Type.STRING
    );

    private static final List<Class<?>> SUPPORTED_VALUE_CLASS_TO_CONVERT_FROM = Arrays.asList(
        Byte.class,
        Short.class,
        Integer.class,
        Long.class,
        Double.class,
        Float.class,
        Boolean.class,
        String.class
    );

    private ExtractTopicConfig config;

    @Override
    public ConfigDef config() {
        return ExtractTopicConfig.config();
    }

    @Override
    public void configure(final Map<String, ?> settings) {
        this.config = new ExtractTopicConfig(settings);
    }

    @Override
    public R apply(final R record) {
        final SchemaAndValue schemaAndValue = getSchemaAndValue(record);

        final Optional<String> newTopic;

        if (schemaAndValue.schema() == null) { // schemaless values (Map)
            if (config.fieldName().isPresent()) {
                newTopic = topicNameFromNamedFieldSchemaless(
                    record.toString(), schemaAndValue.value(), config.fieldName().get());
            } else {
                newTopic = topicNameWithoutFieldNameSchemaless(
                    record.toString(), schemaAndValue.value());
            }
        } else { // schema-based values (Struct)
            if (config.fieldName().isPresent()) {
                newTopic = topicNameFromNamedFieldWithSchema(
                    record.toString(), schemaAndValue.schema(), schemaAndValue.value(), config.fieldName().get());
            } else {
                newTopic = topicNameWithoutFieldNameWithSchema(
                    record.toString(), schemaAndValue.schema(), schemaAndValue.value());
            }
        }

        if (newTopic.isPresent()) {
            return record.newRecord(
                newTopic.get(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                record.headers()
            );
        } else {
            return record;
        }
    }

    protected abstract String dataPlace();

    protected abstract SchemaAndValue getSchemaAndValue(final R record);

    private Optional<String> topicNameFromNamedFieldSchemaless(final String recordStr,
                                                               final Object value,
                                                               final String fieldName) {
        if (value == null) {
            throw new DataException(dataPlace() + " can't be null if field name is specified: " + recordStr);
        }

        if (!(value instanceof Map)) {
            throw new DataException(dataPlace() + " type must be Map if field name is specified: " + recordStr);
        }

        @SuppressWarnings("unchecked") final Map<String, Object> valueMap = (Map<String, Object>) value;

        final Optional<String> result = Optional.ofNullable(valueMap.get(fieldName))
            .map(field -> {
                if (!SUPPORTED_VALUE_CLASS_TO_CONVERT_FROM.contains(field.getClass())) {
                    throw new DataException(fieldName + " type in " + dataPlace()
                        + " " + value
                        + " must be " + SUPPORTED_VALUE_CLASS_TO_CONVERT_FROM
                        + ": " + recordStr);
                }
                return field;
            })
            .map(Object::toString);

        if (result.isPresent() && !result.get().isBlank()) {
            return result;
        } else {
            if (config.skipMissingOrNull()) {
                return Optional.empty();
            } else {
                throw new DataException(fieldName + " in " + dataPlace() + " can't be null or empty: " + recordStr);
            }
        }
    }

    private Optional<String> topicNameWithoutFieldNameSchemaless(final String recordStr,
                                                                 final Object value) {
        if (value == null || value.toString().isBlank()) {
            if (config.skipMissingOrNull()) {
                return Optional.empty();
            } else {
                throw new DataException(dataPlace() + " can't be null or empty: " + recordStr);
            }
        }

        if (!SUPPORTED_VALUE_CLASS_TO_CONVERT_FROM.contains(value.getClass())) {
            throw new DataException("type in " + dataPlace()
                + " " + value
                + " must be " + SUPPORTED_VALUE_CLASS_TO_CONVERT_FROM
                + ": " + recordStr);
        }

        return Optional.of(value.toString());
    }

    private Optional<String> topicNameFromNamedFieldWithSchema(final String recordStr,
                                                               final Schema schema,
                                                               final Object value,
                                                               final String fieldName) {
        if (Schema.Type.STRUCT != schema.type()) {
            throw new DataException(dataPlace() + " schema type must be STRUCT if field name is specified: "
                + recordStr);
        }

        if (value == null) {
            throw new DataException(dataPlace() + " can't be null if field name is specified: " + recordStr);
        }

        final Field field = schema.field(fieldName);
        if (field == null) {
            if (config.skipMissingOrNull()) {
                return Optional.empty();
            } else {
                throw new DataException(fieldName + " in " + dataPlace() + " schema can't be missing: " + recordStr);
            }
        }

        if (!SUPPORTED_SCHEMA_TYPES_TO_CONVERT_FROM.contains(field.schema().type())) {
            throw new DataException(fieldName + " schema type in " + dataPlace()
                + " must be " + SUPPORTED_SCHEMA_TYPES_TO_CONVERT_FROM
                + ": " + recordStr);
        }

        final Struct struct = (Struct) value;

        final Optional<String> result = Optional.ofNullable(struct.get(fieldName))
            .map(Object::toString);
        if (result.isPresent() && !result.get().equals("")) {
            return result;
        } else {
            if (config.skipMissingOrNull()) {
                return Optional.empty();
            } else {
                throw new DataException(fieldName + " in " + dataPlace() + " can't be null or empty: " + recordStr);
            }
        }
    }

    private Optional<String> topicNameWithoutFieldNameWithSchema(final String recordStr,
                                                                 final Schema schema,
                                                                 final Object value) {
        if (!SUPPORTED_SCHEMA_TYPES_TO_CONVERT_FROM.contains(schema.type())) {
            throw new DataException(dataPlace() + " schema type must be "
                + SUPPORTED_SCHEMA_TYPES_TO_CONVERT_FROM
                + " if field name is not specified: "
                + recordStr);
        }

        if (value == null || "".equals(value)) {
            if (config.skipMissingOrNull()) {
                return Optional.empty();
            } else {
                throw new DataException(dataPlace() + " can't be null or empty: " + recordStr);
            }
        }

        return Optional.of(value.toString());
    }

    public static class Key<R extends ConnectRecord<R>> extends ExtractTopic<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.keySchema(), record.key());
        }

        @Override
        protected String dataPlace() {
            return "key";
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ExtractTopic<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.valueSchema(), record.value());
        }

        @Override
        protected String dataPlace() {
            return "value";
        }
    }

    @Override
    public void close() {
    }
}
