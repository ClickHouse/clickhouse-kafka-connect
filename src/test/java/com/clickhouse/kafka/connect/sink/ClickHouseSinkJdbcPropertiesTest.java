package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
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
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClickHouseSinkJdbcPropertiesTest extends ClickHouseBase {
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
        props.put(ClickHouseSinkConfig.JDBC_CONNECTION_PROPERTIES, "?load_balancing_policy=random&health_check_interval=5000&failover=2");

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
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void withEmptyDataRecordsTest() {
        Map<String, String> props = createProps();
        if (isCloud) {
            props.put(ClickHouseSinkConfig.JDBC_CONNECTION_PROPERTIES, "?ssl=true&sslmode=none");
        } else {
            props.put(ClickHouseSinkConfig.JDBC_CONNECTION_PROPERTIES, "?ssl=false&sslmode=none");
        }
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
        assertEquals(sr.size() / 2, ClickHouseTestHelpers.countRows(chc, topic));
    }
}
