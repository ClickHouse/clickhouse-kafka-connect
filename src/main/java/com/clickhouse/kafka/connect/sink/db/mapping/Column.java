package com.clickhouse.kafka.connect.sink.db.mapping;

public class Column {
    private String name;
    private Type type;
    private boolean isNull;

    private Column(String name, Type type, boolean isNull) {
        this.name = name;
        this.type = type;
        this.isNull = isNull;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public boolean isNull() {
        return isNull;
    }

    public static Column extractColumn(String name, String typeValue, boolean isNull) {
        Type type = Type.NONE;
        switch (typeValue ){
            case "Int8" :
                type = Type.INT8;
                break;
            case "Int16" :
                type = Type.INT16;
                break;
            case "Int32" :
                type = Type.INT32;
                break;
            case "Int64" :
                type = Type.INT64;
                break;
            case "Int128" :
                type = Type.INT128;
                break;
            case "Int256":
                type = Type.INT256;
                break;
            case "String":
                type = Type.STRING;
                break;
            case "Float32":
                type = Type.FLOAT32;
                break;
            case "Float64":
                type = Type.FLOAT64;
                break;
            case "Bool":
                type = Type.BOOLEAN;
                break;
            default:
                if (typeValue.startsWith("Array"))
                    type = Type.NONE;

                break;

        }

        return new Column(name, type, isNull);
    }
}
