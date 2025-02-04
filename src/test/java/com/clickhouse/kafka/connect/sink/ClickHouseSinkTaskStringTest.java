package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.query.Records;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.dlq.InMemoryDLQ;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClickHouseSinkTaskStringTest extends ClickHouseBase {
    private int countRowsWithEmojis(ClickHouseHelperClient chc, String topic) {
        String queryCount = "select count(*) from " + topic + " where str LIKE '%\uD83D\uDE00%' SETTINGS select_sequential_consistency = 1";
        try {
            Records records = chc.getClient().queryRecords(queryCount).get();
            String value = records.iterator().next().getString(1);
            return Integer.parseInt(value);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    private int countRows(ClickHouseHelperClient chc, String topic) {
        String queryCount = String.format("select count(*) from `%s` SETTINGS select_sequential_consistency = 1", topic);
        try {
            Records records = chc.getClient().queryRecords(queryCount).get();
            String value = records.iterator().next().getString(1);
            return Integer.parseInt(value);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Collection<SinkRecord> createPrimitiveTypes(String topic, int partition) {
        Gson gson = new Gson();
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {
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

            java.lang.reflect.Type gsonType = new TypeToken<HashMap>() {
            }.getType();
            String gsonString = gson.toJson(value_struct, gsonType);

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
                    gsonString,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public Collection<SinkRecord> createCSV(String topic, int partition) {
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {
            String dataAsCSV = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s",  (short)n, "num" + n, (byte)n, (short)n, (int)n, (long)n, (float)n*1.1, (double)n*1.111111, (boolean)true);
            if (n % 2 == 0) {
                dataAsCSV += "\n";
            }
            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
                    dataAsCSV,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public Collection<SinkRecord> createTSV(String topic, int partition) {
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {
            String dataAsTSV = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",  (short)n, "num" + n, (byte)n, (short)n, (int)n, (long)n, (float)n*1.1, (double)n*1.111111, (boolean)true);
            if (n % 2 == 0) {
                dataAsTSV += "\n";
            }
            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
                    dataAsTSV,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public Collection<SinkRecord> createCode25(String topic, int partition) {
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {
            String dataAsJSON = "{\"str\":\"\\\\\\\\ \\ud83d\",\"off16\":0}";
            dataAsJSON += "\n";
            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
                    dataAsJSON,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public Collection<SinkRecord> createDataWithEmojis(String topic, int partition) {
        Gson gson = new Gson();
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {
            Map<String, Object> value_struct = new HashMap<>();
            value_struct.put("off16", (short)n);
            if ( n % 2 == 0) value_struct.put("str", "num \uD83D\uDE00 :" + n);
            else value_struct.put("str", "num \uD83D\uDE02 :" + n);

            java.lang.reflect.Type gsonType = new TypeToken<HashMap>() {
            }.getType();
            String gsonString = gson.toJson(value_struct, gsonType);

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
                    gsonString,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public Collection<SinkRecord> createPrimitiveTypesWithNulls(String topic, int partition) {
        Gson gson = new Gson();
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {
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

            java.lang.reflect.Type gsonType = new TypeToken<HashMap>() {
            }.getType();
            String gsonString = gson.toJson(value_struct, gsonType);

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
                    gsonString,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        return array;
    }

    public Collection<SinkRecord> createArrayType(String topic, int partition) {
        Gson gson = new Gson();
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {

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

            java.lang.reflect.Type gsonType = new TypeToken<HashMap>() {
            }.getType();
            String gsonString = gson.toJson(value_struct, gsonType);

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
                    gsonString,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        Collection<SinkRecord> collection = array;
        return collection;
    }
    public Collection<SinkRecord> createMapType(String topic, int partition) {
        Gson gson = new Gson();
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {

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


            java.lang.reflect.Type gsonType = new TypeToken<HashMap>() {
            }.getType();
            String gsonString = gson.toJson(value_struct, gsonType);

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, null,
                    gsonString,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        Collection<SinkRecord> collection = array;
        return collection;
    }


    public Collection<SinkRecord> createWithEmptyDataRecords(String topic, int partition) {
        Gson gson = new Gson();
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {
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
            java.lang.reflect.Type gsonType = new TypeToken<HashMap>() {
            }.getType();
            String gsonString = gson.toJson(value_struct, gsonType);

            if (n % 2 == 0) {
                sr = new SinkRecord(
                        topic,
                        partition,
                        null,
                        null, null,
                        gsonString,
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
    @Test
    public void primitiveTypesTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = createTopicName("schemaless_primitive_types_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = createPrimitiveTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    public void withEmptyDataRecordsTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = createTopicName("schemaless_empty_records_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = createWithEmptyDataRecords(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, countRows(chc, topic));
    }

    @Test
    public void NullableValuesTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = createTopicName("schemaless_nullable_values_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `null_str` Nullable(String), `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = createPrimitiveTypesWithNulls(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    public void arrayTypesTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schemaless_array_string_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = createArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    public void mapTypesTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schemaless_map_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, map_string_string Map(String, String), map_string_int64 Map(String, Int64), map_int64_string Map(Int64, String)  ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/38
    public void specialCharTableNameTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("special-char-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, map_string_string Map(String, String), map_string_int64 Map(String, Int64), map_int64_string Map(Int64, String)  ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    public void emojisCharsDataTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("emojis_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = createDataWithEmojis(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, countRowsWithEmojis(chc, topic));
    }


    @Test
    public void tableMappingTest() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "mapping_table_test=table_mapping_test");

        ClickHouseHelperClient chc = createClient(props);
        String topic = "mapping_table_test";
        String tableName = "table_mapping_test";
        ClickHouseTestHelpers.dropTable(chc, tableName);
        createTable(chc, tableName, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = createPrimitiveTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), countRows(chc, tableName));
    }

    @Test
    public void csvTest() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.INSERT_FORMAT, "csv");

        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("csv_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = createCSV(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    public void tsvTest() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.INSERT_FORMAT, "tsv");

        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("tsv_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = createTSV(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), countRows(chc, topic));
    }
    @Test
    public void clickHouseErrorCode25() {
        InMemoryDLQ er = new InMemoryDLQ();
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.INSERT_FORMAT, "json");
        props.put("errors.tolerance", "all");

        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("code_25_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = createCode25(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.setErrorReporter(er);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), er.size());
    }

}
