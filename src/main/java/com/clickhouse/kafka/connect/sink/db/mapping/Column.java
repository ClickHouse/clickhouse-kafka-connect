package com.clickhouse.kafka.connect.sink.db.mapping;

import com.clickhouse.kafka.connect.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Column {
    private static final Logger LOGGER = LoggerFactory.getLogger(Column.class);
    private String name;
    private Type type;
    private boolean isNullable;
    private boolean hasDefaultValue;
    private Column subType = null;
    private Type mapKeyType = Type.NONE;
    private Column mapValueType = null;
    private int precision;
    private int scale;

    private Column(String name, Type type, boolean isNullable,  boolean hasDefaultValue) {
        this.name = name;
        this.type = type;
        this.isNullable = isNullable;
        this.hasDefaultValue = hasDefaultValue;
        this.subType = null;
        this.precision = 0;
    }

    private Column(String name, Type type, boolean isNullable, boolean hasDefaultValue, Type mapKeyType, Column mapValueType) {
        this.name = name;
        this.type = type;
        this.isNullable = isNullable;
        this.hasDefaultValue = hasDefaultValue;
        this.subType = null;
        this.mapKeyType = mapKeyType;
        this.mapValueType = mapValueType;
        this.precision = 0;
    }

    private Column(String name, Type type, boolean isNullable, boolean hasDefaultValue, Column subType) {
        this.name = name;
        this.type = type;
        this.isNullable = isNullable;
        this.hasDefaultValue = hasDefaultValue;
        this.subType = subType;
        this.precision = 0;
    }

    private Column(String name, Type type, boolean isNullable, boolean hasDefaultValue, int precision, int scale) {
        this.name = name;
        this.type = type;
        this.isNullable = isNullable;
        this.hasDefaultValue = hasDefaultValue;
        this.precision = precision;
        this.scale = scale;
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
    public boolean hasDefault() {
        return hasDefaultValue;
    }

    public int getPrecision() {
        return precision;
    }
    public int getScale() {
        return scale;
    }
    public Type getMapKeyType() {
        return mapKeyType;
    }
    public  Column getMapValueType() {
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
            case "UInt8":
                type = Type.UINT8;
                break;
            case "UInt16":
                type = Type.UINT16;
                break;
            case "UInt32":
                type = Type.UINT32;
                break;
            case "UInt64":
                type = Type.UINT64;
                break;
            case "UInt128":
                type = Type.UINT128;
                break;
            case "UInt256":
                type = Type.UINT256;
                break;
            case "UUID":
                type = Type.UUID;
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
                } else if (valueType.startsWith("Decimal")) {
                    type = Type.Decimal;
                } else if (valueType.startsWith("FixedString")) {
                    type = Type.FIXED_STRING;
                }

                break;
        }
        return type;
    }

    public static Column extractColumn(String name, String valueType, boolean isNull, boolean hasDefaultValue) {
        LOGGER.trace("Extracting column {} with type {}", name, valueType);

        Type type = dispatchPrimitive(valueType);
        if (valueType.startsWith("Array")) {
            type = Type.ARRAY;
            Column subType = extractColumn(name, valueType.substring("Array".length() + 1, valueType.length() - 1), false, hasDefaultValue);
            return new Column(name, type, false, hasDefaultValue, subType);
        } else if(valueType.startsWith("Map")) {
            type = Type.MAP;
            String value = valueType.substring("Map".length() + 1, valueType.length() - 1);
            String[] val = value.split(",", 2);
            String mapKey = val[0].trim();
            String mapValue = val[1].trim();
            Column mapValueType = extractColumn(name, mapValue, false, hasDefaultValue);
            return new Column(name, type, false, hasDefaultValue, dispatchPrimitive(mapKey), mapValueType);
        } else if (valueType.startsWith("LowCardinality")) {
            return extractColumn(name, valueType.substring("LowCardinality".length() + 1, valueType.length() - 1), isNull, hasDefaultValue);
        } else if (valueType.startsWith("Nullable")) {
            return extractColumn(name, valueType.substring("Nullable".length() + 1, valueType.length() - 1), true, hasDefaultValue);
        } else if (type == Type.FIXED_STRING) {
            int length = Integer.parseInt(valueType.substring("FixedString".length() + 1, valueType.length() - 1).trim());
            return new Column(name, type, isNull, hasDefaultValue, length, 0);
        } else if (type == Type.DateTime64) {
            String[] scaleAndTimezone = valueType.substring("DateTime64".length() + 1, valueType.length() - 1).split(",");
            int precision = Integer.parseInt(scaleAndTimezone[0].trim());
            LOGGER.trace("Precision is {}", precision);
            return new Column(name, type, isNull, hasDefaultValue, precision, 0);
        } else if (type == Type.Decimal) {
            final Pattern patter = Pattern.compile("Decimal(?<size>\\d{2,3})?\\s*(\\((?<a1>\\d{1,}\\s*)?,*\\s*(?<a2>\\d{1,})?\\))?");
            Matcher match = patter.matcher(valueType);

            if (!match.matches()) {
                throw new RuntimeException("type doesn't match");
            }

            Optional<Integer> size = Optional.ofNullable(match.group("size")).map(Integer::parseInt);
            Optional<Integer> arg1 = Optional.ofNullable(match.group("a1")).map(Integer::parseInt);
            Optional<Integer> arg2 = Optional.ofNullable(match.group("a2")).map(Integer::parseInt);

            if (size.isPresent()) {
                int precision;
                switch (size.get()) {
                    case 32: precision = 9; break;
                    case 64: precision = 18; break;
                    case 128: precision = 38; break;
                    case 256: precision = 76; break;
                    default: throw new RuntimeException("Not supported precision");
                }

                return new Column(name, type, isNull, hasDefaultValue, precision, arg1.orElseThrow());
            } else if (arg2.isPresent()) {
                return new Column(name, type, isNull, hasDefaultValue, arg1.orElseThrow(), arg2.orElseThrow());
            } else if (arg1.isPresent()) {
                return new Column(name, type, isNull, hasDefaultValue, arg1.orElseThrow(), 0);
            } else {
                return new Column(name, type, isNull, hasDefaultValue, 10, 0);
            }
        }

        return new Column(name, type, isNull, hasDefaultValue);
    }

    public String toString() {
        return String.format("Column{name=%s, type=%s, isNullable=%s, hasDefaultValue=%s, subType=%s, mapKeyType=%s, mapValueType=%s, precision=%s, scale=%s}",
                name, type, isNullable, hasDefaultValue, subType, mapKeyType, mapValueType, precision, scale);
    }
}
