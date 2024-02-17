package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;

public class ClickHouseSinkTaskTest extends ClickHouseBase {

    public Collection<SinkRecord> createDBTopicSplit(int dbRange, String topic, int partition, String splitChar) {
        Gson gson = new Gson();
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, dbRange).forEachOrdered(i -> {
            String newTopic = i + splitChar + topic  ;
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
                        newTopic,
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
        });




        return array;
    }
    @Test
    public void testExceptionHandling() {
        ClickHouseSinkTask task = new ClickHouseSinkTask();
        assertThrows(RuntimeException.class, () -> task.put(null));
        try {
            task.put(null);
        } catch (Exception e) {
            assertEquals(e.getClass(), RuntimeException.class);
            assertTrue(e.getCause() instanceof NullPointerException);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            assertTrue(sw.toString().contains("com.clickhouse.kafka.connect.util.Utils.handleException"));
        }
    }
    @Test
    public void testDBTopicSplit() {
        Map<String, String> props =  createProps();
        props.put(ClickHouseSinkConfig.ENABLE_DB_TOPIC_SPLIT, "true");
        props.put(ClickHouseSinkConfig.DB_TOPIC_SPLIT_CHAR, ".");

        createClient(props);
        String tableName = "splitTopic";
        int dbRange = 10;
        LongStream.range(0, dbRange).forEachOrdered(i -> {
            String tmpTableName = String.format("%d.%s", i, tableName);
            System.out.println(tmpTableName);
            createDatabase(String.valueOf(i));
            createTable(chc, tmpTableName, "CREATE TABLE `%s` ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        });

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        // Generate SinkRecords with different topics and check if they are split correctly
        Collection<SinkRecord> records = createDBTopicSplit(dbRange, "splitTopic", 0, ".");
        try {
            task.start(props);
            task.put(records);
        } catch (Exception e) {
            fail("Exception should not be thrown");
        }
        LongStream.range(0, dbRange).forEachOrdered(i -> {
            int count = countRows(chc, String.valueOf(i), tableName);
            assertEquals(1000, countRows(chc, String.valueOf(i), tableName));
        });
    }
}
