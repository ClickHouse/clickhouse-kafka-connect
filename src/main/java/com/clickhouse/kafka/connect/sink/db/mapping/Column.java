package com.clickhouse.kafka.connect.sink.db.mapping;

public class Column {
    private String name;
    private Type type;
    private boolean isNullable;
    private Column subType = null;
    private Type mapKeyType = Type.NONE;
    private Type mapValueType = Type.NONE;

    private Column(String name, Type type, boolean isNullable) {
        this.name = name;
        this.type = type;
        this.isNullable = isNullable;
        this.subType = null;
    }

    private Column(String name, Type type, boolean isNullable, Type mapKeyType, Type mapValueType) {
        this.name = name;
        this.type = type;
        this.isNullable = isNullable;
        this.subType = null;
        this.mapKeyType = mapKeyType;
        this.mapValueType = mapValueType;
    }

    private Column(String name, Type type, boolean isNullable, Column subType) {
        this.name = name;
        this.type = type;
        this.isNullable = isNullable;
        this.subType = subType;
    }


    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public Column getSubType() {
        return subType;
    }
    public boolean isNullable() {
        return isNullable;
    }

    public Type getMapKeyType() {
        return mapKeyType;
    }
    public  Type getMapValueType() {
        return mapValueType;
    }
    private static Type dispatchPrimitive(String valueType) {
        Type type = Type.NONE;
        switch (valueType) {
            case "Int8":
                type = Type.INT8;
                break;
            case "Int16":
                type = Type.INT16;
                break;
            case "Int32":
                type = Type.INT32;
                break;
            case "Int64":
                type = Type.INT64;
                break;
            case "Int128":
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
            case "Date":
                type = Type.Date;
                break;
            case "Date32":
                type = Type.Date32;
                break;
            case "DateTime":
                type = Type.DateTime;
                break;
            default:
                if (valueType.startsWith("DateTime64")) {
                    // Need to understand why DateTime64(3)
                    type = Type.DateTime64;
                }


                break;

        }
        return type;
    }

    public static Column extractColumn(String name, String valueType, boolean isNull) {
        Type type = Type.NONE;
        type = dispatchPrimitive(valueType);
        if (valueType.startsWith("Array")) {
            type = Type.ARRAY;
            Column subType = extractColumn(name, valueType.substring("Array".length() + 1, valueType.length() - 1), false);
            return new Column(name, type, false, subType);
        } else if(valueType.startsWith("Map")) {
            type = Type.MAP;
            String value = valueType.substring("Map".length() + 1, valueType.length() - 1);
            String val[] = value.split(",");
            String mapKey = val[0].trim();
            String mapValue = val[1].trim();
            return new Column(name, type, false, dispatchPrimitive(mapKey), dispatchPrimitive(mapValue));
        } else if (valueType.startsWith("LowCardinality")) {
            return extractColumn(name, valueType.substring("LowCardinality".length() + 1, valueType.length() - 1), isNull);
        } else if (valueType.startsWith("Nullable")) {
            return extractColumn(name, valueType.substring("Nullable".length() + 1, valueType.length() - 1), true);
        }
        return new Column(name, type, isNull);
    }
}
