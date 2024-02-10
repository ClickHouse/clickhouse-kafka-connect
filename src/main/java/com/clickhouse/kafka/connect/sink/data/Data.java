package com.clickhouse.kafka.connect.sink.data;

import org.apache.kafka.connect.data.Schema;

public class Data {
    private Schema schema;
    private Object object;

    public Data(Schema schema, Object object) {
        this.schema = schema;
        this.object = object;
    }

    public Schema.Type getFieldType() {
        return schema.type();
    }

    public Schema getMapKeySchema() {
        return schema.keySchema();
    }

    public Schema getNestedValueSchema() {
        return schema.valueSchema();
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
