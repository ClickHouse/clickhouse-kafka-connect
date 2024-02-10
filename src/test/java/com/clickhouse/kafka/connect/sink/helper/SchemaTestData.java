package com.clickhouse.kafka.connect.sink.helper;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.sink.SinkRecord;

import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import java.util.Date;
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
        Schema ARRAY_MAP_ARRAY_SCHEMA = SchemaBuilder.array(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA));


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
                .field("arr_map_arr", ARRAY_MAP_ARRAY_SCHEMA)
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
            List<Map<String, String>> arrayMapArray = Arrays.asList(Map.of("k1", "v1", "k2", "v2"));


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
                    .put("arr_map_arr", arrayMapArray)
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

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("map_string_string", MAP_SCHEMA_STRING_STRING)
                .field("map_string_int64", MAP_SCHEMA_STRING_INT64)
                .field("map_int64_string", MAP_SCHEMA_INT64_STRING)
                .field("map_string_map", MAP_SCHEMA_STRING_MAP)
                .field("map_string_array", MAP_SCHEMA_STRING_ARRAY)
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


            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("map_string_string", mapStringString)
                    .put("map_string_int64", mapStringLong)
                    .put("map_int64_string", mapLongString)
                    .put("map_string_map", mapStringMap)
                    .put("map_string_array", mapStringArray)
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

    public static Collection<SinkRecord> createDateType(String topic, int partition) {
        return createDateType(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createDateType(String topic, int partition, int totalRecords) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("date_number", Schema.OPTIONAL_INT32_SCHEMA)
                .field("date32_number", Schema.OPTIONAL_INT32_SCHEMA)
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

            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("date_number", localDateInt)
                    .put("date32_number", localDateInt)
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
}
