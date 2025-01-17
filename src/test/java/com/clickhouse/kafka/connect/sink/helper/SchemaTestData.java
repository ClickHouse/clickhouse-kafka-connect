package com.clickhouse.kafka.connect.sink.helper;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

public class SchemaTestData {
    public static final int DEFAULT_TOTAL_RECORDS = 1000;

    public static Collection<SinkRecord> createWithEmptyDataRecords(String topic, int partition) {
        return createWithEmptyDataRecords(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createWithEmptyDataRecords(String topic, int partition, int totalRecords) {
        List<SinkRecord> array = new ArrayList<>();

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("p_int64", Schema.INT64_SCHEMA)
                .build();

        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("p_int64", n);

            SinkRecord sr = null;
            if (n % 2 == 0) {
                sr = new SinkRecord(
                        topic,
                        partition,
                        null,
                        null, NESTED_SCHEMA,
                        value_struct,
                        n,
                        System.currentTimeMillis(),
                        TimestampType.CREATE_TIME
                );
            } else {
                sr = new SinkRecord(
                        topic,
                        partition,
                        null,
                        null, NESTED_SCHEMA,
                        null,
                        n,
                        System.currentTimeMillis(),
                        TimestampType.CREATE_TIME
                );
            }
            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createWithLowCardinality(String topic, int partition) {
        return createWithLowCardinality(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createWithLowCardinality(String topic, int partition, int totalRecords) {
        List<SinkRecord> array = new ArrayList<>();

        Schema SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("p_int64", Schema.INT64_SCHEMA)
                .field("lc_string", Schema.STRING_SCHEMA)
                .field("nullable_lc_string", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Struct value_struct = new Struct(SCHEMA)
                    .put("off16", (short)n)
                    .put("p_int64", n)
                    .put("lc_string", "abc")
                    .put("nullable_lc_string", n % 2 == 0 ? "def" : null);

            array.add(new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            ));
        });
        return array;
    }

    public static Collection<SinkRecord> createWithUUID(String topic, int partition) {
        return createWithUUID(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createWithUUID(String topic, int partition, int totalRecords) {
        List<SinkRecord> array = new ArrayList<>();

        Schema SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("uuid", Schema.STRING_SCHEMA)
                .build();

        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Struct value_struct = new Struct(SCHEMA)
                    .put("off16", (short)n)
                    .put("uuid", UUID.randomUUID().toString());

            array.add(new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            ));
        });
        return array;
    }

    public static Collection<SinkRecord> createNullableArrayType(String topic, int partition) {
        return createNullableArrayType(topic, partition, DEFAULT_TOTAL_RECORDS);
    }

    public static Collection<SinkRecord> createNullableArrayType(String topic, int partition, int totalRecords) {
        Schema ARRAY_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build();

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("arr", ARRAY_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {

            List<String> arrayTmp = Arrays.asList("1","2");

            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("arr", n % 10 == 0 ? null : arrayTmp)
                    ;

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createArrayType(String topic, int partition) {
        return createArrayType(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createArrayType(String topic, int partition, int totalRecords) {

        Schema ARRAY_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        Schema ARRAY_INT8_SCHEMA = SchemaBuilder.array(Schema.INT8_SCHEMA).build();
        Schema ARRAY_INT16_SCHEMA = SchemaBuilder.array(Schema.INT16_SCHEMA).build();
        Schema ARRAY_INT32_SCHEMA = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
        Schema ARRAY_INT64_SCHEMA = SchemaBuilder.array(Schema.INT64_SCHEMA).build();
        Schema ARRAY_FLOAT32_SCHEMA = SchemaBuilder.array(Schema.FLOAT32_SCHEMA).build();
        Schema ARRAY_FLOAT64_SCHEMA = SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build();
        Schema ARRAY_BOOLEAN_SCHEMA = SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build();
        Schema ARRAY_STRING_ARRAY_SCHEMA = SchemaBuilder.array(ARRAY_SCHEMA).build();
        Schema ARRAY_ARRAY_STRING_ARRAY_SCHEMA = SchemaBuilder.array(SchemaBuilder.array(ARRAY_SCHEMA)).build();
        Schema ARRAY_MAP_SCHEMA = SchemaBuilder.array(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA));


        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("arr", ARRAY_SCHEMA)
                .field("arr_empty", ARRAY_SCHEMA)
                .field("arr_int8", ARRAY_INT8_SCHEMA)
                .field("arr_int16", ARRAY_INT16_SCHEMA)
                .field("arr_int32", ARRAY_INT32_SCHEMA)
                .field("arr_int64", ARRAY_INT64_SCHEMA)
                .field("arr_float32", ARRAY_FLOAT32_SCHEMA)
                .field("arr_float64", ARRAY_FLOAT64_SCHEMA)
                .field("arr_bool", ARRAY_BOOLEAN_SCHEMA)
                .field("arr_str_arr", ARRAY_STRING_ARRAY_SCHEMA)
                .field("arr_arr_str_arr", ARRAY_ARRAY_STRING_ARRAY_SCHEMA)
                .field("arr_map", ARRAY_MAP_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {

            List<String> arrayTmp = Arrays.asList("1","2");
            List<String> arrayEmpty = new ArrayList<>();
            List<Byte> arrayInt8Tmp = Arrays.asList((byte)1,(byte)2);
            List<Short> arrayInt16Tmp = Arrays.asList((short)1,(short)2);
            List<Integer> arrayInt32Tmp = Arrays.asList((int)1,(int)2);
            List<Long> arrayInt64Tmp = Arrays.asList((long)1,(long)2);
            List<Float> arrayFloat32Tmp = Arrays.asList((float)1,(float)2);
            List<Double> arrayFloat64Tmp = Arrays.asList((double)1,(double)2);
            List<Boolean> arrayBoolTmp = Arrays.asList(true,false);
            List<List<String>> arrayStrArray = Arrays.asList(arrayTmp, arrayTmp);
            List<List<List<String>>> arrayArrayStrArray = Arrays.asList(Arrays.asList(arrayTmp, arrayTmp));
            List<Map<String, String>> arrayMap = Arrays.asList(Map.of("k1", "v1", "k2", "v2"));


            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("arr", arrayTmp)
                    .put("arr_empty", arrayEmpty)
                    .put("arr_int8", arrayInt8Tmp)
                    .put("arr_int16", arrayInt16Tmp)
                    .put("arr_int32", arrayInt32Tmp)
                    .put("arr_int64", arrayInt64Tmp)
                    .put("arr_float32", arrayFloat32Tmp)
                    .put("arr_float64", arrayFloat64Tmp)
                    .put("arr_bool", arrayBoolTmp)
                    .put("arr_str_arr", arrayStrArray)
                    .put("arr_arr_str_arr", arrayArrayStrArray)
                    .put("arr_map", arrayMap)
                    ;


            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createArrayNullableSubtypes(String topic, int partition) {
        Schema ARRAY_OPTIONAL_STRING_SCHEMA = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build();
        Schema ARRAY_OPTIONAL_INT8_SCHEMA = SchemaBuilder.array(Schema.OPTIONAL_INT8_SCHEMA).build();
        Schema ARRAY_OPTIONAL_INT16_SCHEMA = SchemaBuilder.array(Schema.OPTIONAL_INT16_SCHEMA).build();
        Schema ARRAY_OPTIONAL_INT32_SCHEMA = SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build();
        Schema ARRAY_OPTIONAL_INT64_SCHEMA = SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).build();
        Schema ARRAY_OPTIONAL_FLOAT32_SCHEMA = SchemaBuilder.array(Schema.OPTIONAL_FLOAT32_SCHEMA).build();
        Schema ARRAY_OPTIONAL_FLOAT64_SCHEMA = SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).build();
        Schema ARRAY_OPTIONAL_BOOLEAN_SCHEMA = SchemaBuilder.array(Schema.OPTIONAL_BOOLEAN_SCHEMA).build();



        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("arr_nullable_str", ARRAY_OPTIONAL_STRING_SCHEMA)
                .field("arr_empty_nullable_str", ARRAY_OPTIONAL_STRING_SCHEMA)
                .field("arr_nullable_int8", ARRAY_OPTIONAL_INT8_SCHEMA)
                .field("arr_nullable_int16", ARRAY_OPTIONAL_INT16_SCHEMA)
                .field("arr_nullable_int32", ARRAY_OPTIONAL_INT32_SCHEMA)
                .field("arr_nullable_int64", ARRAY_OPTIONAL_INT64_SCHEMA)
                .field("arr_nullable_float32", ARRAY_OPTIONAL_FLOAT32_SCHEMA)
                .field("arr_nullable_float64", ARRAY_OPTIONAL_FLOAT64_SCHEMA)
                .field("arr_nullable_bool", ARRAY_OPTIONAL_BOOLEAN_SCHEMA)
                .build();

        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {

            List<String> arrayNullableStrTmp = Arrays.asList("1","2");
            List<String> arrayNullableStrEmpty = new ArrayList<>();
            List<Byte> arrayNullableInt8Tmp = Arrays.asList((byte)1,null);
            List<Short> arrayNullableInt16Tmp = Arrays.asList((short)1, null);
            List<Integer> arrayNullableInt32Tmp = Arrays.asList((int)1,null);
            List<Long> arrayNullableInt64Tmp = Arrays.asList((long)1,null);
            List<Float> arrayNullableFloat32Tmp = Arrays.asList((float)1,null);
            List<Double> arrayNullableFloat64Tmp = Arrays.asList((double)1,null);
            List<Boolean> arrayNullableBoolTmp = Arrays.asList(true,null);


            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("arr_nullable_str", arrayNullableStrTmp)
                    .put("arr_empty_nullable_str", arrayNullableStrEmpty)
                    .put("arr_nullable_int8", arrayNullableInt8Tmp)
                    .put("arr_nullable_int16", arrayNullableInt16Tmp)
                    .put("arr_nullable_int32", arrayNullableInt32Tmp)
                    .put("arr_nullable_int64", arrayNullableInt64Tmp)
                    .put("arr_nullable_float32", arrayNullableFloat32Tmp)
                    .put("arr_nullable_float64", arrayNullableFloat64Tmp)
                    .put("arr_nullable_bool", arrayNullableBoolTmp)
                    ;


            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createMapType(String topic, int partition) {
        return createMapType(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createMapType(String topic, int partition, int totalRecords) {

        Schema MAP_SCHEMA_STRING_STRING = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);
        Schema MAP_SCHEMA_STRING_INT64 = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA);
        Schema MAP_SCHEMA_INT64_STRING = SchemaBuilder.map(Schema.INT64_SCHEMA, Schema.STRING_SCHEMA);
        Schema MAP_SCHEMA_STRING_MAP = SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA));
        Schema MAP_SCHEMA_STRING_ARRAY = SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA));
        Schema MAP_MAP_MAP_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)));

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("map_string_string", MAP_SCHEMA_STRING_STRING)
                .field("map_string_int64", MAP_SCHEMA_STRING_INT64)
                .field("map_int64_string", MAP_SCHEMA_INT64_STRING)
                .field("map_string_map", MAP_SCHEMA_STRING_MAP)
                .field("map_string_array", MAP_SCHEMA_STRING_ARRAY)
                .field("map_map_map", MAP_MAP_MAP_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {

            Map<String,String> mapStringString = Map.of(
                    "k1", "v1",
                    "k2", "v1"
            );

            Map<String,Long> mapStringLong = Map.of(
                    "k1", (long)1,
                    "k2", (long)2
            );

            Map<Long,String> mapLongString = Map.of(
                    (long)1, "v1",
                    (long)2, "v2"
            );

            Map<String,Map<String, Long>> mapStringMap = Map.of(
                    "k1", Map.of("nk1", (long)1, "nk2", (long)2),
                    "k2", Map.of("nk1", (long)3, "nk2", (long)4)
            );

            Map<String, List<String>> mapStringArray = Map.of(
                    "k1", Arrays.asList("v1", "v2"),
                    "k2", Arrays.asList("v3", "v4")
            );
            Map<String, Map<String, Map<String, String>>> mapMapMap = Map.of(
                    "k1", Map.of("nk1", Map.of("nk2", "v1")),
                    "k2", Map.of("nk1", Map.of("nk2", "v2"))
            );


            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("map_string_string", mapStringString)
                    .put("map_string_int64", mapStringLong)
                    .put("map_int64_string", mapLongString)
                    .put("map_string_map", mapStringMap)
                    .put("map_string_array", mapStringArray)
                    .put("map_map_map", mapMapMap)
                    ;


            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }
    public static Collection<SinkRecord> createTupleType(String topic, int partition) {
        return createTupleType(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createTupleType(String topic, int partition, int totalRecords) {

        Schema ARRAY_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA);
        Schema MAP_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);

        Schema ARRAY_ARRAY_SCHEMA = SchemaBuilder.array(SchemaBuilder.array(Schema.STRING_SCHEMA));
        Schema MAP_MAP_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA));

        Schema ARRAY_MAP_SCHEMA = SchemaBuilder.array(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA));
        Schema MAP_ARRAY_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA));

        Schema ARRAY_ARRAY_ARRAY_SCHEMA = SchemaBuilder.array(SchemaBuilder.array(SchemaBuilder.array(Schema.STRING_SCHEMA)));
        Schema MAP_MAP_MAP_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)));

        Schema VARIANT_SCHEMA = SchemaBuilder.struct()
                .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema VARIANT_TUPLE_SCHEMA = SchemaBuilder.struct()
                .field("variant_with_string", VARIANT_SCHEMA)
                .field("variant_with_double", VARIANT_SCHEMA)
                .field("variant_array", SchemaBuilder.array(VARIANT_SCHEMA).build())
                .field("variant_map", SchemaBuilder.map(Schema.STRING_SCHEMA, VARIANT_SCHEMA).build())
                .build();

        Schema TUPLE_SCHEMA = SchemaBuilder.struct()
                .field("array", ARRAY_SCHEMA)
                .field("map", MAP_SCHEMA)
                .field("array_array", ARRAY_ARRAY_SCHEMA)
                .field("map_map", MAP_MAP_SCHEMA)
                .field("array_map", ARRAY_MAP_SCHEMA)
                .field("map_array", MAP_ARRAY_SCHEMA)
                .field("array_array_array", ARRAY_ARRAY_ARRAY_SCHEMA)
                .field("map_map_map", MAP_MAP_MAP_SCHEMA)
                .field("tuple", VARIANT_TUPLE_SCHEMA)
                .field("array_tuple", SchemaBuilder.array(VARIANT_TUPLE_SCHEMA).build())
                .field("map_tuple", SchemaBuilder.map(Schema.STRING_SCHEMA, VARIANT_TUPLE_SCHEMA).build())
                .field("array_array_tuple", SchemaBuilder.array(SchemaBuilder.array(VARIANT_TUPLE_SCHEMA)).build())
                .field("map_map_tuple", SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.map(Schema.STRING_SCHEMA, VARIANT_TUPLE_SCHEMA)).build())
                .field("array_map_tuple", SchemaBuilder.array(SchemaBuilder.map(Schema.STRING_SCHEMA, VARIANT_TUPLE_SCHEMA)).build())
                .field("map_array_tuple", SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(VARIANT_TUPLE_SCHEMA)).build())
                .build();

        Schema ROOT_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("tuple", TUPLE_SCHEMA)
                .build();

        List<SinkRecord> result = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {

            List<String> array = Arrays.asList("1", "2");
            List<List<String>> arrayArray = Arrays.asList(array, array);
            List<List<List<String>>> arrayArrayArray = Arrays.asList(arrayArray, arrayArray);

            Map<String, String> map = Map.of("k1", "v1", "k2", "v2");
            Map<String, Map<String, Long>> mapMap = Map.of(
                    "k1", Map.of("nk1", (long) 1),
                    "k2", Map.of("nk1", (long) 2)
            );
            Map<String, Map<String, Map<String, String>>> mapMapMap = Map.of(
                    "k1", Map.of("nk1", Map.of("nk2", "v1")),
                    "k2", Map.of("nk1", Map.of("nk2", "v2"))
            );

            List<Map<String, String>> arrayMap = Arrays.asList(map, map);
            Map<String, List<String>> mapArray = Map.of(
                    "k1", array,
                    "k2", array
            );

            Struct stringVariant = new Struct(VARIANT_SCHEMA).put("string", "v1");
            Struct doubleVariant = new Struct(VARIANT_SCHEMA).put("double", (double) 1 / 3);

            List<Struct> variantArray = Arrays.asList(stringVariant, doubleVariant);

            Struct nestedTuple = new Struct(VARIANT_TUPLE_SCHEMA)
                    .put("variant_with_string", stringVariant)
                    .put("variant_with_double", doubleVariant)
                    .put("variant_array", variantArray)
                    .put("variant_map", Map.of("s1", stringVariant, "d1", doubleVariant));

            List<Struct> arrayTuple = Arrays.asList(nestedTuple, nestedTuple);
            Map<String, Struct> mapTuple = Map.of("k1", nestedTuple, "k2", nestedTuple);

            Struct tupleStruct = new Struct(TUPLE_SCHEMA)
                    .put("array", array)
                    .put("map", map)
                    .put("array_array", arrayArray)
                    .put("map_map", mapMap)
                    .put("array_map", arrayMap)
                    .put("map_array", mapArray)
                    .put("array_array_array", arrayArrayArray)
                    .put("map_map_map", mapMapMap)
                    .put("tuple", nestedTuple)
                    .put("array_tuple", arrayTuple)
                    .put("map_tuple", mapTuple)
                    .put("array_array_tuple", Arrays.asList(arrayTuple, arrayTuple))
                    .put("map_map_tuple", Map.of("r1", mapTuple, "r2", mapTuple))
                    .put("array_map_tuple", Arrays.asList(mapTuple, mapTuple))
                    .put("map_array_tuple", Map.of("r1", arrayTuple, "r2", arrayTuple));


            Struct rootStruct = new Struct(ROOT_SCHEMA)
                    .put("off16", (short) n)
                    .put("tuple", tupleStruct);

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, ROOT_SCHEMA,
                    rootStruct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            result.add(sr);
        });
        return result;
    }

    public static Collection<SinkRecord> createNullValueData(String topic, int partition) {
        return createNullValueData(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createNullValueData(String topic, int partition, int totalRecords) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("null_value_data", Schema.OPTIONAL_INT64_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {

            Long null_value_data = null;

            if ( n % 2 == 0) {
                null_value_data = Long.valueOf((short)n);
            }
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("null_value_data", null_value_data)
                    ;


            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createBytesValueData(String topic, int partition) {
        return createBytesValueData(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createBytesValueData(String topic, int partition, int totalRecords) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("string", Schema.BYTES_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Struct value_struct = new Struct(NESTED_SCHEMA).put("string", Long.toBinaryString(n).getBytes());

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createTupleLikeInfluxValueData(String topic, int partition) {
        return createTupleLikeInfluxValueData(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createTupleLikeInfluxValueData(String topic, int partition, int totalRecords) {

        Schema variantSchema = SchemaBuilder.struct()
                .field("float64", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("int64", Schema.OPTIONAL_INT64_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema payloadSchema = SchemaBuilder.struct()
                .field("fields", SchemaBuilder.map(Schema.STRING_SCHEMA, variantSchema))
                .field("tags", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
                .build();

        Schema nestedSchema = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("payload", payloadSchema)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Struct payload = new Struct(payloadSchema)
                    .put("fields", Map.of(
                            "field1", new Struct(variantSchema).put("float64", 1 / (float) (n + 1)),
                            "field2", new Struct(variantSchema).put("int64", n),
                            "field3", new Struct(variantSchema).put("string", String.format("Value '%d'", n))
                    ))
                    .put("tags", Map.of(
                            "tag1", "tag1",
                            "tag2", "tag2"
                    ));

            Struct valueStruct = new Struct(nestedSchema)
                    .put("off16", (short) n)
                    .put("payload", payload);

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, nestedSchema,
                    valueStruct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createDateType(String topic, int partition) {
        return createDateType(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createDateType(String topic, int partition, int totalRecords) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("date_number", Schema.OPTIONAL_INT32_SCHEMA)
                .field("date32_number", Schema.OPTIONAL_INT32_SCHEMA)
                .field("datetime_int", Schema.INT32_SCHEMA)
                .field("datetime_number", Schema.INT64_SCHEMA)
                .field("datetime64_number", Schema.INT64_SCHEMA)
                .field("timestamp_int64",  Timestamp.SCHEMA)
                .field("timestamp_date", Timestamp.SCHEMA)
                .field("time_int32" , Time.SCHEMA)
                .field("time_date32" , Time.SCHEMA)
                .field("date_date", Time.SCHEMA)
                .field("datetime_date", Timestamp.SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            long currentTime = System.currentTimeMillis();
            LocalDate localDate = LocalDate.now();
            Integer localDateInt = (int)localDate.toEpochDay();
            if(n%3 == 0) {
                localDateInt = null;
            }

            LocalDateTime localDateTime = LocalDateTime.now();
            long localDateTimeLong = localDateTime.toEpochSecond(ZoneOffset.UTC);
            int localDateTimeInt = (int)localDateTime.toEpochSecond(ZoneOffset.UTC);

            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("date_number", localDateInt)
                    .put("date32_number", localDateInt)
                    .put("datetime_int", localDateTimeInt)
                    .put("datetime_number", localDateTimeLong)
                    .put("datetime64_number", currentTime)
                    .put("timestamp_int64", new Date(System.currentTimeMillis()))
                    .put("timestamp_date",  new Date(System.currentTimeMillis()))
                    .put("time_int32", new Date(System.currentTimeMillis()))
                    .put("time_date32", new Date(System.currentTimeMillis()))
                    .put("date_date", new Date(System.currentTimeMillis()))
                    .put("datetime_date", new Date(System.currentTimeMillis()))
                    ;


            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createArrayDateTime64Type(String topic, int partition) {

            Schema NESTED_SCHEMA = SchemaBuilder.struct()
                    .field("off16", Schema.INT16_SCHEMA)
                    .field("arr_datetime64_number", SchemaBuilder.array(Schema.INT64_SCHEMA).build())
                    .field("arr_timestamp_date", SchemaBuilder.array(Timestamp.SCHEMA).build())
                    .build();


            List<SinkRecord> array = new ArrayList<>();
            LongStream.range(0, 1000).forEachOrdered(n -> {
                long currentTime1 = System.currentTimeMillis();
                long currentTime2 = System.currentTimeMillis();
                Date date1 = new Date(System.currentTimeMillis());
                Date date2 = new Date(System.currentTimeMillis());
                List<Long> arrayDateTime64Number = Arrays.asList(currentTime1, currentTime2);
                List<Date> arrayTimestamps = Arrays.asList(date1, date2);

                Struct value_struct = new Struct(NESTED_SCHEMA)
                        .put("off16", (short)n)
                        .put("arr_datetime64_number", arrayDateTime64Number)
                        .put("arr_timestamp_date", arrayTimestamps)
                        ;


                SinkRecord sr = new SinkRecord(
                        topic,
                        partition,
                        null,
                        null, NESTED_SCHEMA,
                        value_struct,
                        n,
                        System.currentTimeMillis(),
                        TimestampType.CREATE_TIME
                );

                array.add(sr);
            });
            return array;
        }

    public static Collection<SinkRecord> createUnsupportedDataConversions(String topic, int partition) {
        return createUnsupportedDataConversions(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createUnsupportedDataConversions(String topic, int partition, int totalRecords) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("date_number", Schema.INT64_SCHEMA)
                .field("date32_number", Schema.INT64_SCHEMA)
                .field("datetime_number", Schema.INT32_SCHEMA)
                .field("datetime64_number", Schema.INT32_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            long currentTime = System.currentTimeMillis();
            LocalDate localDate = LocalDate.now();
            int localDateInt = (int)localDate.toEpochDay();

            LocalDateTime localDateTime = LocalDateTime.now();
            long localDateTimeLong = localDateTime.toEpochSecond(ZoneOffset.UTC);

            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("date_number", localDateTimeLong)
                    .put("date32_number", currentTime)
                    .put("datetime_number", localDateInt)
                    .put("datetime64_number", localDateInt)
                    ;


            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createDecimalValueData(String topic, int partition) {
        return createDecimalValueData(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createDecimalValueData(String topic, int partition, int totalRecords) {
        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("decimal_14_2", Decimal.schema(2))
                .build();

        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("decimal_14_2", new BigDecimal(String.format("%d.%d", n, 2)));

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });

        return array;
    }

    public static Collection<SinkRecord> createDecimalValueDataWithNulls(String topic, int partition) {
        return createDecimalValueDataWithNulls(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createDecimalValueDataWithNulls(String topic, int partition, int totalRecords) {
        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("decimal_14_2", Decimal.builder(2).optional().build())
                .build();

        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("decimal_14_2", n % 10 == 0 ? null : new BigDecimal(String.format("%d.%d", n, 2)));

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });

        return array;
    }


    public static Collection<SinkRecord> createZonedTimestampConversions(String topic, int partition) {
        return createZonedTimestampConversions(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createZonedTimestampConversions(String topic, int partition, int totalRecords) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("zoned_date", Schema.STRING_SCHEMA)
                .field("offset_date", Schema.STRING_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("zoned_date", ZonedDateTime.now().toString())
                    .put("offset_date", OffsetDateTime.now().toString());


            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createFormattedTimestampConversions(String topic, int partition) {
        return createFormattedTimestampConversions(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createFormattedTimestampConversions(String topic, int partition, int totalRecords) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("format_date", Schema.STRING_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("format_date", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")));


            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }



    public static Collection<SinkRecord> createFixedStringData(String topic, int partition, int fixedSize) {
        return createFixedStringData(topic, partition, DEFAULT_TOTAL_RECORDS, fixedSize);
    }
    public static Collection<SinkRecord> createFixedStringData(String topic, int partition, int totalRecords, int fixedSize) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("fixed_string_string", Schema.STRING_SCHEMA)
                .field("fixed_string_bytes", Schema.BYTES_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("fixed_string_string", RandomStringUtils.random(fixedSize, true, true))
                    .put("fixed_string_bytes", RandomStringUtils.random(fixedSize, true, true).getBytes());


            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createEnumValueData(String topic, int i) {
        return createEnumValueData(topic, i, DEFAULT_TOTAL_RECORDS);
    }

    public static Collection<SinkRecord> createEnumValueData(String topic, int partition, int totalRecords) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("enum8_type", Schema.STRING_SCHEMA)
                .field("enum16_type", Schema.STRING_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("enum8_type", "A")
                    .put("enum16_type", "D");

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createNestedType(String topic, int partition) {
        return createNestedType(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createNestedType(String topic, int partition, int totalRecords) {

        Schema VARIANT_SCHEMA = SchemaBuilder.struct()
                .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema TUPLE_SCHEMA = SchemaBuilder.struct()
                .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
                .field("variant", VARIANT_SCHEMA)
                .build();

        Schema ROOT_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("nested.string", SchemaBuilder.array(Schema.STRING_SCHEMA))
                .field("nested.decimal", SchemaBuilder.array(Decimal.schema(2)))
                .field("nested.tuple", SchemaBuilder.array(TUPLE_SCHEMA))
                .build();

        List<SinkRecord> result = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {

            String currentStringKey = String.format("k%d", n);
            String currentStringValue = String.format("v%d", n);

            Struct booleanVariant = new Struct(VARIANT_SCHEMA).put("boolean", n % 8 >= 4);
            Struct stringVariant = new Struct(VARIANT_SCHEMA).put("string", currentStringValue);

            Struct tupleStruct = new Struct(TUPLE_SCHEMA)
                    .put("map", Map.of(currentStringKey, currentStringValue))
                    .put("variant", n % 2 == 0 ? booleanVariant : stringVariant);

            int nestedSize = (int) n % 4;

            Struct rootStruct = new Struct(ROOT_SCHEMA)
                    .put("off16", (short) n)
                    .put("nested.string", new ArrayList<>(Collections.nCopies(nestedSize, currentStringValue)))
                    .put("nested.decimal", new ArrayList<>(Collections.nCopies(nestedSize, new BigDecimal(String.format("%d.%d", n, 2)))))
                    .put("nested.tuple", new ArrayList<>(Collections.nCopies(nestedSize, tupleStruct)));

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, ROOT_SCHEMA,
                    rootStruct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            result.add(sr);
        });
        return result;
    }

    public static Collection<SinkRecord> createUnsignedIntegers(String topic, int partition) {
        return createUnsignedIntegers(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createUnsignedIntegers(String topic, int partition, int totalRecords) {
        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("uint8", Schema.OPTIONAL_INT8_SCHEMA)
                .field("uint16", Schema.OPTIONAL_INT16_SCHEMA)
                .field("uint32", Schema.OPTIONAL_INT32_SCHEMA)
                .field("uint64", Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short) n)
                    .put("uint8", (byte) ThreadLocalRandom.current().nextInt(0, 127))
                    .put("uint16", (short) ThreadLocalRandom.current().nextInt(0, 32767))
                    .put("uint32", ThreadLocalRandom.current().nextInt(0, 2147483647))
                    .put("uint64", ThreadLocalRandom.current().nextLong(0, 2147483647));

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });

        return array;
    }
    public static Collection<SinkRecord> createSimpleData(String topic, int partition) {
        return createSimpleData(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createSimpleData(String topic, int partition, int totalRecords) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .build();

        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {

            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("string", "test string");

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }
    public static Collection<SinkRecord> createTupleSimpleData(String topic, int partition) {
        return createTupleSimpleData(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createTupleSimpleData(String topic, int partition, int totalRecords) {

        Schema TUPLE_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.OPTIONAL_INT16_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("t", TUPLE_SCHEMA)
                .build();

        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {

            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("string", "test string")
                    .put( "t", new Struct(TUPLE_SCHEMA)
                            .put("off16", (short)n)
                            .put("string", "test string")
                    );

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createNestedTupleSimpleData(String topic, int partition) {
        return createNestedTupleSimpleData(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createNestedTupleSimpleData(String topic, int partition, int totalRecords) {

        Schema NESTED_TUPLE_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.OPTIONAL_INT16_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema TUPLE_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.OPTIONAL_INT16_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("n", NESTED_TUPLE_SCHEMA)
                .build();

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("t", TUPLE_SCHEMA)
                .build();

        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {

            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("string", "test string")
                    .put( "t", new Struct(TUPLE_SCHEMA)
                            .put("off16", (short)n)
                            .put("string", "test string")
                            .put("n" , new Struct(NESTED_TUPLE_SCHEMA)
                                    .put("off16", (short)n)
                                    .put("string", "test string"))
                    );

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }
    public static Collection<SinkRecord> createCoolSchemaWithRandomFields(String topic, int partition) {
        return createCoolSchemaWithRandomFields(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createCoolSchemaWithRandomFields(String topic, int partition, int totalRecords) {

//        "`processing_time` DateTime," +
//                "`insert_time` DateTime DEFAULT now()," +
//                "`type` String," +
//                "`player` Tuple(id Nullable(Int64), key Nullable(String), ip Nullable(String), label Nullable(String), device_id Nullable(Int64), player_tracker_id Nullable(Int64), is_new_player Nullable(Bool), player_root Nullable(String), target Nullable(String), `type` Nullable(String), name Nullable(String), processing_time Nullable(Int64), tags Map(String, String), session_id Nullable(String), machine_fingerprint Nullable(String), player_fingerprint Nullable(String))," +
//                "`sensor` Tuple(sensor_id String, origin_device String, session_id String, machine_id Int64, machine_timestamp String)," +
//                "`data` String, " +
//                "`id` String, " +
//                "`desc` Nullable(String), " +
//                "`tag` Nullable(String), " +
//                "`va` Nullable(Float64) " +


        Schema PLAYER_SCHEMA = SchemaBuilder.struct()
                .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                .field("key", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ip", Schema.OPTIONAL_STRING_SCHEMA)
                .field("label", Schema.OPTIONAL_STRING_SCHEMA)
                .field("device_id", Schema.OPTIONAL_INT64_SCHEMA)
                .field("player_tracker_id", Schema.OPTIONAL_INT64_SCHEMA)
                .field("is_new_player", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("player_root", Schema.OPTIONAL_STRING_SCHEMA)
                .field("target", Schema.OPTIONAL_STRING_SCHEMA)
                .field("type", Schema.OPTIONAL_STRING_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("processing_time", Schema.OPTIONAL_INT64_SCHEMA)
                .field("tags", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
                .field("session_id", Schema.OPTIONAL_STRING_SCHEMA)
                .field("machine_fingerprint", Schema.OPTIONAL_STRING_SCHEMA)
                .field("player_fingerprint", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema SENSOR_SCHEMA = SchemaBuilder.struct()
                .field("sensor_id", Schema.STRING_SCHEMA)
                .field("origin_device", Schema.STRING_SCHEMA)
                .field("session_id", Schema.STRING_SCHEMA)
                .field("machine_id", Schema.INT64_SCHEMA)
                .field("machine_timestamp", Schema.STRING_SCHEMA)
                .build();

        Schema COOL_SCHEMA = SchemaBuilder.struct()
                .field("processing_time", Schema.OPTIONAL_INT32_SCHEMA)
                .field("type", Schema.STRING_SCHEMA)
                .field("player", PLAYER_SCHEMA)
                .field("sensor", SENSOR_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("id", Schema.STRING_SCHEMA)
                .field("desc", Schema.OPTIONAL_STRING_SCHEMA)
                .field("tag", Schema.OPTIONAL_STRING_SCHEMA)
                .field("va", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .build();

        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {

            Map<String,String> tags = Map.of(
                    "k1", "v1",
                    "k2", "v1"
            );

            Struct player_struct = new Struct(PLAYER_SCHEMA)
                    .put("id", n)
                    .put("key", "Cool key")
                    .put("ip", "0.0.0.0")
                    .put("label", "Cool label")
                    .put("device_id", n)
                    .put("player_tracker_id", n)
                    .put("is_new_player", false)
                    .put("player_root", UUID.randomUUID().toString())
                    .put("target", "cool target")
                    .put("type", "cool type")
                    .put("name", "cool name")
                    .put("processing_time", System.currentTimeMillis())
                    .put("tags", tags)
                    .put("session_id", UUID.randomUUID().toString())
                    .put("machine_fingerprint", UUID.randomUUID().toString())
                    .put("player_fingerprint", UUID.randomUUID().toString());

            Struct sensor_struct = new Struct(SENSOR_SCHEMA)
                    .put("sensor_id", "461ca7c5-5632-4f08-be15-8a0aceb5c874")
                    .put("origin_device", "cool device")
                    .put("session_id", "7b45b380-77fe-4cda-a390-00603cdd0c8e")
                    .put("machine_id", n)
                    .put("machine_timestamp", "cool device");

            Struct value_struct = new Struct(COOL_SCHEMA)
                    .put("processing_time", (int)n)
                    .put("type", "Cool type")
                    .put("player", player_struct)
                    .put("sensor", sensor_struct)
                    .put("data", "Cool data")
                    .put("id", "0ce8ca62-237a-422f-af1f-a8ec73549fe8 ")
                    .put("desc", "Cool app")
                    .put("tag", "Cool")
                    .put("va", (double)n);

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, COOL_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }
    public static Collection<SinkRecord> createSimpleExtendWithNullableData(String topic, int partition) {
        return createSimpleExtendWithNullableData(topic, partition, 0, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createSimpleExtendWithNullableData(String topic, int partition, int startOffset, int totalRecords) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("num32", Schema.OPTIONAL_INT32_SCHEMA)
                .build();

        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(startOffset, startOffset + totalRecords).forEachOrdered(n -> {

            Integer null_value_data = null;

            if ( n % 2 == 0) {
                null_value_data = Integer.valueOf((int)n);
            }
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("string", "test string")
                    .put("num32", null_value_data);

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createSimpleExtendWithDefaultData(String topic, int partition, int startOffset, int totalRecords) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("num32", Schema.OPTIONAL_INT32_SCHEMA)
                .field("num32_default", Schema.INT32_SCHEMA)
                .build();

        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(startOffset, startOffset + totalRecords).forEachOrdered(n -> {

            Integer null_value_data = null;

            if ( n % 2 == 0) {
                null_value_data = Integer.valueOf((int)n);
            }

            Integer default_value_data = 0;
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("string", "test string")
                    .put("num32", null_value_data);
            if ( n % 2 == 0) {
                value_struct.put("num32_default", (int)n);
            }

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }
}
