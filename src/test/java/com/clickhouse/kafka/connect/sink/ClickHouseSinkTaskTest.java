package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;

public class ClickHouseSinkTaskTest extends ClickHouseBase {

    public static final int DEFAULT_TOTAL_RECORDS = 1000;
    public Collection<SinkRecord> createDBTopicSplit(int dbRange, long timeStamp, String topic, int partition, String splitChar) {
        Gson gson = new Gson();
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, dbRange).forEachOrdered(i -> {
            String newTopic = i + "_" + timeStamp + splitChar + topic  ;
            LongStream.range(0, DEFAULT_TOTAL_RECORDS).forEachOrdered(n -> {
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

    public ClickHouseResponseSummary dropTable(ClickHouseHelperClient chc, String tableName) {
        String dropTable = String.format("DROP TABLE IF EXISTS %s", tableName);
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer())
                     .query(dropTable)
                     .executeAndWait()) {
            return response.getSummary();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

//    @Test TODO: Fix this test
    public void testDBTopicSplit() {
        Map<String, String> props =  createProps();
        props.put(ClickHouseSinkConfig.ENABLE_DB_TOPIC_SPLIT, "true");
        props.put(ClickHouseSinkConfig.DB_TOPIC_SPLIT_CHAR, ".");
        long timeStamp = System.currentTimeMillis();
        createClient(props, false);
        String tableName = createTopicName("splitTopic");
        int dbRange = 10;
        LongStream.range(0, dbRange).forEachOrdered(i -> {
            String databaseName = String.format("%d_%d" , i, timeStamp);
            String tmpTableName = String.format("`%s`.`%s`", databaseName, tableName);
            dropTable(chc, tmpTableName);
            createDatabase(databaseName);
            createTable(chc, tmpTableName, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        });

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        // Generate SinkRecords with different topics and check if they are split correctly
        Collection<SinkRecord> records = createDBTopicSplit(dbRange, timeStamp, tableName, 0, ".");
        try {
            task.start(props);
            task.put(records);
        } catch (Exception e) {
            fail("Exception should not be thrown");
        }
        LongStream.range(0, dbRange).forEachOrdered(i -> {
            int count = countRows(chc, String.valueOf(i), tableName);
            assertEquals(DEFAULT_TOTAL_RECORDS, count);
        });
    }


    @Test
    public void simplifiedBatchingSchemaless() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.IGNORE_PARTITIONS_WHEN_BATCHING, "true");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("schemaless_simple_batch_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);
        sr.addAll(SchemalessTestData.createPrimitiveTypes(topic, 2));
        sr.addAll(SchemalessTestData.createPrimitiveTypes(topic, 3));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        assertTrue(ClickHouseTestHelpers.validateRows(chc, topic, sr));
        //assertEquals(1, com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers.countInsertQueries(chc, topic));
    }

    @Test
    public void clientNameTest() {

        List<String> versions = Arrays.asList("V1", "V2");

        for (String version : versions) {
            Map<String, String> props = createProps();
            props.put(ClickHouseSinkConfig.IGNORE_PARTITIONS_WHEN_BATCHING, "true");
            props.put(ClickHouseSinkConnector.CLIENT_VERSION, version);
            ClickHouseHelperClient chc = createClient(props);
            String topic = createTopicName("schemaless_simple_batch_test");
            ClickHouseTestHelpers.dropTable(chc, topic);
            ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                    "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
            Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);
            sr.addAll(SchemalessTestData.createPrimitiveTypes(topic, 2));
            sr.addAll(SchemalessTestData.createPrimitiveTypes(topic, 3));

            ClickHouseSinkTask chst = new ClickHouseSinkTask();
            chst.start(props);
            chst.put(sr);
            chst.stop();
            assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
            assertTrue(ClickHouseTestHelpers.validateRows(chc, topic, sr));

            chc.queryV2("SYSTEM FLUSH LOGS "+ (isCloud ? "ON CLUSTER 'default'" : ""));

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                // ignore
            }

            for (int i = 0; i < 3; i++) {
                String getLogRecords = String.format("SELECT http_user_agent, query FROM clusterAllReplicas('default', system.query_log) " +
                                "   WHERE query_kind = 'Insert' " +
                                "   AND type = 'QueryStart'" +
                                "   AND has(databases,'%1$s') " +
                                "   AND position(http_user_agent, '%3$s') > 0 LIMIT 100",
                        chc.getDatabase(), topic, ClickHouseHelperClient.CONNECT_CLIENT_NAME);


                List<GenericRecord> records = chc.getClient().queryAll(getLogRecords);
                if (records.isEmpty() && i < 2) {
                    continue;
                }
                assertFalse(records.isEmpty());
                for (GenericRecord record : records) {
                    assertTrue(record.getString(1).startsWith(ClickHouseHelperClient.CONNECT_CLIENT_NAME));
                }
                break;
            }
        }
    }
}
