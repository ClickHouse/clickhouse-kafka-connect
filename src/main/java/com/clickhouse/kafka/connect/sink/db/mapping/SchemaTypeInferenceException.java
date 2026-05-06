package com.clickhouse.kafka.connect.sink.db.mapping;

public class SchemaTypeInferenceException extends RuntimeException {
    public SchemaTypeInferenceException(String message) {
        super(message);
    }
}
