package com.clickhouse.kafka.connect.sink.db.mapping;

import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseFieldDescriptor;
import com.clickhouse.kafka.connect.util.Utils;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.clickhouse.kafka.connect.util.reactor.function.Tuple2;
import com.clickhouse.kafka.connect.util.reactor.function.Tuple3;
import com.clickhouse.kafka.connect.util.reactor.function.Tuples;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Builder
@Getter
public class Column {
    private static final Logger LOGGER = LoggerFactory.getLogger(Column.class);

    private String name;
    private Type type;

    @Accessors(fluent = true)
    private boolean hasDefault;
    private boolean isNullable;
    private boolean isSubColumn;


    private int precision;
    private int scale;
    private Map<String, Integer> enumValues;

    private Type mapKeyType;
    @Setter private Column mapValueType;
    @Setter private Column arrayType;
    private List<Column> tupleFields;
    private List<Tuple2<Column, String>> variantTypes;

    @Setter private int mapDepth;
    private int arrayDepth;

    @Setter
    private Column parent;

    /**
     * The Variant Global Discriminators are used to mark which variant is serialized on the wire.
     * See <a href="https://github.com/ClickHouse/ClickHouse/blob/658a8e9a9b1658cd12c78365f9829b35d016f1b2/src/Columns/ColumnVariant.h#L10-L56">Columns/ColumnVariant.h</a>
     */
    @Getter(lazy = true)
    private final List<Tuple2<Column, String>> variantGlobalDiscriminators = variantTypes.stream()
            .sorted(Comparator.comparing(Tuple2::getT2))
            .collect(Collectors.toList());

    /**
     * We need to map Kafka Connect type to ClickHouse type. This is tricky and might not work as expected for
     * parametrized types, such as Decimal(x, y). But this is a problem only when the Variant holds multiple types of
     * the same type, but with different parameters.
     */
    public Optional<Integer> getVariantGlobalDiscriminator(String clickHouseType) {
        int index = this.getVariantGlobalDiscriminators().stream()
                .map(Tuple2::getT2)
                .map(String::toUpperCase)
                .collect(Collectors.toList())
                .indexOf(clickHouseType.toUpperCase());

        if (index < 0) return Optional.empty();
        else return Optional.of(index);
    }

    private static final Map<String, Type> TYPE_MAP = Map.ofEntries(
            Map.entry("Int8", Type.INT8),
            Map.entry("Int16", Type.INT16),
            Map.entry("Int32", Type.INT32),
            Map.entry("Int64", Type.INT64),
            Map.entry("Int128", Type.INT128),
            Map.entry("Int256", Type.INT256),
            Map.entry("UInt8", Type.UINT8),
            Map.entry("UInt16", Type.UINT16),
            Map.entry("UInt32", Type.UINT32),
            Map.entry("UInt64", Type.UINT64),
            Map.entry("UInt128", Type.UINT128),
            Map.entry("UInt256", Type.UINT256),
            Map.entry("UUID", Type.UUID),
            Map.entry("String", Type.STRING),
            Map.entry("Float32", Type.FLOAT32),
            Map.entry("Float64", Type.FLOAT64),
            Map.entry("Bool", Type.BOOLEAN),
            Map.entry("Date", Type.Date),
            Map.entry("Date32", Type.Date32),
            Map.entry("DateTime", Type.DateTime),
            Map.entry("IntervalQuarter", Type.IntervalQuarter),
            Map.entry("IntervalYear", Type.IntervalYear),
            Map.entry("IntervalMonth", Type.IntervalMonth),
            Map.entry("IntervalWeek", Type.IntervalWeek),
            Map.entry("IntervalDay", Type.IntervalDay),
            Map.entry("IntervalHour", Type.IntervalHour),
            Map.entry("IntervalMinute", Type.IntervalMinute),
            Map.entry("IntervalSecond", Type.IntervalSecond),
            Map.entry("IntervalMillisecond", Type.IntervalMillisecond),
            Map.entry("IntervalNanosecond", Type.IntervalNanosecond),
            Map.entry("IntervalMicrosecond", Type.IntervalMicrosecond),
            Map.entry("IPv4", Type.IPv4),
            Map.entry("IPv6", Type.IPv6),
            Map.entry("Point", Type.Point),
            Map.entry("Polygon", Type.Polygon),
            Map.entry("MultiPolygon", Type.MultiPolygon),
            Map.entry("Ring", Type.Ring),
            Map.entry("JSON", Type.JSON),
            Map.entry("Object", Type.Object)
    );

    private static Type dispatchPrimitive(String valueType) {
        Type type = TYPE_MAP.getOrDefault(valueType, Type.UNKNOWN);

        if (valueType.startsWith("DateTime64")) {
            type = Type.DateTime64;
        } else if (valueType.startsWith("Decimal256")) {
            type = Type.Decimal256;
        } else if (valueType.startsWith("Decimal128")) {
            type = Type.Decimal128;
        } else if (valueType.startsWith("Decimal64")) {
            type = Type.Decimal64;
        } else if (valueType.startsWith("Decimal32")) {
            type = Type.Decimal32;
        } else if (valueType.startsWith("Decimal")) {
            type = Type.Decimal;
        } else if (valueType.startsWith("FixedString")) {
            type = Type.FIXED_STRING;
        }
        return type;
    }

    public static Column extractColumn(ClickHouseFieldDescriptor fieldDescriptor) {
        return Column.extractColumn(fieldDescriptor.getName(), fieldDescriptor.getType(), false, fieldDescriptor.hasDefault(), fieldDescriptor.isSubcolumn());
    }

    public static Column extractColumn(String name, String valueType, boolean isNull, boolean hasDefaultValue, boolean isSubColumn) {
        return extractColumn(name, valueType, isNull, hasDefaultValue, isSubColumn, 0);
    }

    public static Column extractColumn(String name, String valueType, boolean isNull, boolean hasDefaultValue, boolean isSubColumn, int arrayDepth) {
        LOGGER.trace("Extracting column {} with type {}", name, valueType);

        ColumnBuilder builder = Column.builder()
                .name(name)
                .arrayDepth(arrayDepth)
                .hasDefault(hasDefaultValue)
                .isSubColumn(isSubColumn);

        if (valueType.startsWith("Enum")) {
            Type type;
            if (valueType.startsWith("Enum16")) {
                type = Type.Enum16;
            } else {
                type = Type.Enum8;
            }
            return builder.type(type)
                    .enumValues(extractEnumValues(valueType))
                    .build();
        } else if (valueType.startsWith("Array")) {
            Column arrayType = extractColumn(name, valueType.substring("Array".length() + 1, valueType.length() - 1), false, isSubColumn, hasDefaultValue, arrayDepth + 1);
            if (arrayType == null) {
                return null;
            }

            Column array = builder.type(Type.ARRAY)
                    .arrayType(arrayType)
                    .build();

            arrayType.setParent(array);
            return array;
        } else if (valueType.startsWith("Map")) {
            String mapDefinition = valueType.substring("Map".length() + 1, valueType.length() - 1);
            String mapKey = mapDefinition.split(",", 2)[0].trim();

            // We will fill the map value type later (since the describe_include_subcolumns option prints the details later).
            return builder.type(Type.MAP)
                    .mapKeyType(dispatchPrimitive(mapKey))
                    .build();
        } else if (valueType.startsWith("Tuple")) {
            // We will fill the columns inside the tuple later (since the describe_include_subcolumns option prints the details later).
            return builder.type(Type.TUPLE)
                    .tupleFields(new ArrayList<>())
                    .build();
        } else if (valueType.startsWith("Nested")) {
            LOGGER.warn("DESCRIBE TABLE is never supposed to return Nested type - it should always yield its Array fields directly. " +
                    "This is likely caused by a different table in the same database using 'flatten_nested=0', so we'll ignore that table...");
            return null;//This is a special case where we don't want to create a column
        } else if (valueType.startsWith("Variant")) {
            String rawVariantTypes = valueType.substring("Variant".length() + 1, valueType.length() - 1);
            List<Tuple2<Column, String>> variantTypes = splitUnlessInBrackets(rawVariantTypes, ',').stream().map(
                    t -> {
                        String definition = t.trim();

                        // Variants support parametrized types, such as Decimal(x, y), which has to be described
                        // including their parameters for proper serialization. We use Column just as a container
                        // for those parameters. Variant types doesn't hold any names, just types.
                        Tuple3<Type, Integer, Integer> typePrecisionAndScale = dispatchPrimitiveWithPrecisionAndScale(definition);

                        ColumnBuilder variantTypeBuilder = Column.builder().type(typePrecisionAndScale.getT1());

                        if (Pattern.compile(".+\\(.+\\)").asMatchPredicate().test(definition)) {
                            variantTypeBuilder = variantTypeBuilder
                                    .precision(typePrecisionAndScale.getT2())
                                    .scale(typePrecisionAndScale.getT3());
                        }

                        if (definition.equalsIgnoreCase("bool")) {
                            // So that we can match it from the Kafka Connect type
                            definition = "Boolean";
                        }

                        return Tuples.of(variantTypeBuilder.build(), definition);
                    }
            ).collect(Collectors.toList());

            return builder.type(Type.VARIANT)
                    .variantTypes(variantTypes)
                    .build();
        } else if (valueType.startsWith("LowCardinality")) {
            return extractColumn(name, valueType.substring("LowCardinality".length() + 1, valueType.length() - 1), isNull, hasDefaultValue, isSubColumn);
        } else if (valueType.startsWith("Nullable")) {
            return extractColumn(name, valueType.substring("Nullable".length() + 1, valueType.length() - 1), true, hasDefaultValue, isSubColumn);
        }

        // We're dealing with a primitive type here
        Tuple3<Type, Integer, Integer> typePrecisionAndScale = dispatchPrimitiveWithPrecisionAndScale(valueType);

        return builder
                .type(typePrecisionAndScale.getT1())
                .isNullable(isNull)
                .precision(typePrecisionAndScale.getT2())
                .scale(typePrecisionAndScale.getT3())
                .build();
    }

    private static Tuple3<Type, Integer, Integer> dispatchPrimitiveWithPrecisionAndScale(String valueType) {
        Type type = dispatchPrimitive(valueType);

        int precision = 0;
        int scale = 0;

        if (type == Type.FIXED_STRING) {
            precision = Integer.parseInt(valueType.substring("FixedString".length() + 1, valueType.length() - 1).trim());
        } else if (type == Type.DateTime64) {
            String[] scaleAndTimezone = valueType.substring("DateTime64".length() + 1, valueType.length() - 1).split(",");
            precision = Integer.parseInt(scaleAndTimezone[0].trim());
            LOGGER.trace("Parsed precision of DateTime64 is {}", precision);
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
                switch (size.get()) {
                    case 32: precision = 9; break;
                    case 64: precision = 18; break;
                    case 128: precision = 38; break;
                    case 256: precision = 76; break;
                    default: throw new RuntimeException("Not supported precision");
                }

                scale = arg1.orElseThrow();
            } else if (arg2.isPresent()) {
                precision = arg1.orElseThrow();
                scale = arg2.orElseThrow();
            } else if (arg1.isPresent()) {
                precision = arg1.orElseThrow();
            } else {
                precision = 10;
            }
        }

        return Tuples.of(type, precision, scale);
    }

    public static List<String> splitUnlessInBrackets(String input, char delimiter) {
        List<String> parts = new ArrayList<>();
        int bracketCounter = 0; // To keep track of whether we are inside brackets
        StringBuilder part = new StringBuilder();

        for (char ch : input.toCharArray()) {
            if (ch == '(') {
                bracketCounter++;
            } else if (ch == ')') {
                bracketCounter--;
            }

            if (ch == delimiter && bracketCounter == 0) {
                // We've reached a comma outside of brackets, add the part to the list and reset the part builder
                parts.add(part.toString());
                part = new StringBuilder();
            } else {
                part.append(ch); // Add the character to the current part
            }
        }

        // Add the last part after the final comma, or the full string if no comma was found
        parts.add(part.toString());

        return parts;
    }

    private static Map<String, Integer> extractEnumValues(String valueType) {
        Map <String, Integer> data = new HashMap<>();
        List<String> values = Utils.splitIgnoringQuotes(valueType.substring(valueType.indexOf("(") + 1, valueType.indexOf(")")), ',');
        for (String value : values) {
            String[] val = value.split("=", 2);
            String key = val[0].trim();
            data.put(key.substring(1, key.length() - 1), Integer.parseInt(val[1].trim()));
        }
        return data;
    }

    public Integer convertEnumValues(String value) {
        if ( this.enumValues != null ) {
            return enumValues.get(value);
        }
        throw new RuntimeException(String.format("No content in enum %s", value));
    }

    public String toString() {
        return String.format(
                "%s{name=%s",
                type, name
        ) + (!isNullable ? "" :
                String.format(", isNullable=%s", isNullable)
        ) + (!hasDefault ? "" :
                String.format(", hasDefault=%s", hasDefault)
        ) + (precision == 0 && type != Type.Decimal ? "" :
                String.format(", precision=%s", precision)
        ) + (scale == 0 && type != Type.Decimal ? "" :
                String.format(", scale=%s", scale)
        ) + (enumValues == null ? "" :
                String.format(", enumValues=%s", enumValues)
        ) + (arrayType == null ? "" :
                String.format(", arrayType=%s", arrayType)
        ) + (mapKeyType == null ? "" :
                String.format(", mapKeyType=%s", mapKeyType)
        ) + (mapValueType == null ? "" :
                String.format(", mapValueType=%s", mapValueType)
        ) + (tupleFields == null ? "" :
                String.format(", tupleFields=%s", tupleFields.stream().map(Column::toString).collect(Collectors.joining(", ", "[", "]")))
        ) + (variantTypes == null ? "" :
                String.format(", variantTypes=%s", variantTypes.stream().map(Tuple2::getT2).collect(Collectors.joining(", ", "[", "]")))
        ) + "}";
    }
}
