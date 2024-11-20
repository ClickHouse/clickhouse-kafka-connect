package com.clickhouse.kafka.connect.sink.helper;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("map_string_string", MAP_SCHEMA_STRING_STRING)
                .field("map_string_int64", MAP_SCHEMA_STRING_INT64)
                .field("map_int64_string", MAP_SCHEMA_INT64_STRING)
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


            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("map_string_string", mapStringString)
                    .put("map_string_int64", mapStringLong)
                    .put("map_int64_string", mapLongString)
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
}
