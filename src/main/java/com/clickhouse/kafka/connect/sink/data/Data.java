package com.clickhouse.kafka.connect.sink.data;

import org.apache.kafka.connect.data.Schema;

public class Data {
    private Schema.Type fieldType;
    private Object object;

    public Data(Schema.Type fieldType, Object object) {
        this.fieldType = fieldType;
        this.object = object;
    }

    public Schema.Type getFieldType() {
        return fieldType;
    }

    public Object getObject() {
        return object;
    }

    @Override
    public String toString() {
        if (object == null) {
            return null;
        }
        return object.toString();

    }
}
