package com.clickhouse.kafka.connect.exceptions;

public class UnhandledDefaultValuesException extends RuntimeException{
    public UnhandledDefaultValuesException(String name) {
        super(name);
    }
}
