package com.clickhouse.kafka.connect.sink.helper;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

public class SchemalessTestData {
    public static final int DEFAULT_TOTAL_RECORDS = 1000;

    public static Collection<SinkRecord> createPrimitiveTypes(String topic, int partition) {
        return createPrimitiveTypes(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createPrimitiveTypes(String topic, int partition, int totalRecords) {
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Map<String, Object> value_struct = new HashMap<>();
            value_struct.put("str", "num" + n);
            value_struct.put("indexCount", n);
            value_struct.put("off16", (short)n);
            value_struct.put("p_int8", (byte)n);
            value_struct.put("p_int16", (short)n);
            value_struct.put("p_int32", (int)n);
            value_struct.put("p_int64", (long)n);
            value_struct.put("p_float32", (float)n*1.1);
            value_struct.put("p_float64", (double)n*1.111111);
            value_struct.put("p_bool", (boolean)true);

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createDataWithEmojis(String topic, int partition) {
        return createDataWithEmojis(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createDataWithEmojis(String topic, int partition, int totalRecords) {
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Map<String, Object> value_struct = new HashMap<>();
            value_struct.put("off16", (short)n);
            if ( n % 2 == 0) value_struct.put("str", "num \uD83D\uDE00 :" + n);
            else value_struct.put("str", "num \uD83D\uDE02 :" + n);
            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createPrimitiveTypesWithNulls(String topic, int partition) {
        return createPrimitiveTypesWithNulls(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createPrimitiveTypesWithNulls(String topic, int partition, int totalRecords) {
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Map<String, Object> value_struct = new HashMap<>();
            value_struct.put("str", "num" + n);
            value_struct.put("null_str", null);  // the column that should be inserted as it is.
            value_struct.put("off16", (short)n);
            value_struct.put("p_int8", (byte)n);
            value_struct.put("p_int16", (short)n);
            value_struct.put("p_int32", (int)n);
            value_struct.put("p_int64", (long)n);
            value_struct.put("p_float32", (float)n*1.1);
            value_struct.put("p_float64", (double)n*1.111111);
            value_struct.put("p_bool", (boolean)true);

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
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
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {

            List<String> arrayTmp = Arrays.asList("1","2");
            List<String> arrayEmpty = new ArrayList<>();
            List<Byte> arrayInt8Tmp = Arrays.asList((byte)1,(byte)2);
            List<Short> arrayInt16Tmp = Arrays.asList((short)1,(short)2);
            List<Integer> arrayInt32Tmp = Arrays.asList((int)1,(int)2);
            List<Long> arrayInt64Tmp = Arrays.asList((long)1,(long)2);
            List<Float> arrayFloat32Tmp = Arrays.asList((float)1.0,(float)2.0);
            List<Double> arrayFloat64Tmp = Arrays.asList((double)1.1,(double)2.1);
            List<Boolean> arrayBoolTmp = Arrays.asList(true,false);

            Map<String, Object> value_struct = new HashMap<>();
            value_struct.put("arr", arrayTmp);
            value_struct.put("off16", (short)n);
            value_struct.put("arr_empty", arrayEmpty);
            value_struct.put("arr_int8", arrayInt8Tmp);
            value_struct.put("arr_int16", arrayInt16Tmp);
            value_struct.put("arr_int32", arrayInt32Tmp);
            value_struct.put("arr_int64", arrayInt64Tmp);
            value_struct.put("arr_float32", arrayFloat32Tmp);
            value_struct.put("arr_float64", arrayFloat64Tmp);
            value_struct.put("arr_bool", arrayBoolTmp);

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
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



            Map<String, Object> value_struct = new HashMap<>();
            value_struct.put("off16", (short)n);
            value_struct.put("map_string_string", mapStringString);
            value_struct.put("map_string_int64", mapStringLong);
            value_struct.put("map_int64_string", mapLongString);

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public static Collection<SinkRecord> createWithEmptyDataRecords(String topic, int partition) {
        return createWithEmptyDataRecords(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createWithEmptyDataRecords(String topic, int partition, int totalRecords) {
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Map<String, Object> value_struct = new HashMap<>();
            value_struct.put("str", "num" + n);
            value_struct.put("off16", (short)n);
            value_struct.put("p_int8", (byte)n);
            value_struct.put("p_int16", (short)n);
            value_struct.put("p_int32", (int)n);
            value_struct.put("p_int64", (long)n);
            value_struct.put("p_float32", (float)n*1.1);
            value_struct.put("p_float64", (double)n*1.111111);
            value_struct.put("p_bool", (boolean)true);

            SinkRecord sr = null;
            if (n % 2 == 0) {
                sr = new SinkRecord(
                        topic,
                        partition,
                        null,
                        null, null,
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
                        null, null,
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

    public static Collection<SinkRecord> createDecimalTypes(String topic, int partition) {
        return createDecimalTypes(topic, partition, DEFAULT_TOTAL_RECORDS);
    }
    public static Collection<SinkRecord> createDecimalTypes(String topic, int partition, int totalRecords) {
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Map<String, Object> value_struct = new HashMap<>();
            value_struct.put("str", "num" + n);
            value_struct.put("decimal_14_2", new BigDecimal(String.format("%d.%d", n, 2)));

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
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
