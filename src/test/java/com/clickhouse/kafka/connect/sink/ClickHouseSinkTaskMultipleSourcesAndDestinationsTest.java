package com.clickhouse.kafka.connect.sink;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkTaskSchemalessTest.*;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ClickHouseContainer;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClickHouseSinkTaskMultipleSourcesAndDestinationsTest {
    private static int num_dbs = 5;
    private static ClickHouseContainer[] dbs = new ClickHouseContainer[num_dbs];
    private static ClickHouseHelperClient chc = null;
    ClickHouseSinkTaskSchemalessTest cstt = new ClickHouseSinkTaskSchemalessTest();

    @BeforeAll
    private static void setup() {
        for (int i=0; i<num_dbs; i++) {
            dbs[i] = new ClickHouseContainer("clickhouse/clickhouse-server:22.5");
            dbs[i].start();
        }
    }

    private ClickHouseHelperClient createClient(Map<String,String> props) {
        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(props);

        String hostname = csc.getHostname();
        int port = csc.getPort();
        String database = csc.getDatabase();
        String username = csc.getUsername();
        String password = csc.getPassword();
        boolean sslEnabled = csc.isSslEnabled();
        int timeout = csc.getTimeout();


        chc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port)
                .setDatabase(database)
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .build();
        return chc;
    }


    private void dropTable(ClickHouseHelperClient chc, String tableName) {
        String dropTable = String.format("DROP TABLE IF EXISTS `%s`", tableName);
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(chc.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format


                     .query(dropTable)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();

        } catch (ClickHouseException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void createTable(ClickHouseHelperClient chc, String topic, String createTableQuery) {
        String createTableQueryTmp = String.format(createTableQuery, topic);

        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(chc.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format


                     .query(createTableQueryTmp)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();

        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }

    }

    private int countRows(ClickHouseHelperClient chc, String topic) {
        String queryCount = String.format("select count(*) from `%s`", topic);
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(chc.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format


                     .query(queryCount)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();
            return response.firstRecord().getValue(0).asInteger();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void SingleTopicTest() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, dbs[0].getHost());
        props.put(ClickHouseSinkConnector.PORT, dbs[0].getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, dbs[0].getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, dbs[0].getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = "single_topic_test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = cstt.createPrimitiveTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    public void MultipleTopicsSingleConnectorTest() {
        int num_topics = 3;
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, dbs[0].getHost());
        props.put(ClickHouseSinkConnector.PORT, dbs[0].getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, dbs[0].getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, dbs[0].getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        ClickHouseHelperClient chc = createClient(props);

        for (int i = 0; i < num_topics; i++) {
            String topic = "multiple_topics_test_"+i;
            dropTable(chc, topic);
            createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
            Collection<SinkRecord> sr = cstt.createPrimitiveTypes(topic, 1);
            chst.start(props);
            chst.put(sr);
            chst.stop();
            assertEquals(sr.size(), countRows(chc, topic));
        }
    }

    @Test
    public void MultipleConnectorsSingleTopicTest() {
        int num_topics = 3;
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, dbs[0].getHost());
        props.put(ClickHouseSinkConnector.PORT, dbs[0].getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, dbs[0].getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, dbs[0].getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);
        String topic = "single_topic_test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        int current_total = 0;
        for (int i = 0; i < num_topics; i++) {
            ClickHouseSinkTask chst = new ClickHouseSinkTask();
            Collection<SinkRecord> sr = cstt.createPrimitiveTypes(topic, 1);
            chst.start(props);
            chst.put(sr);
            chst.stop();
            current_total+=sr.size();
            assertEquals(current_total, countRows(chc, topic));
        }
    }

    @Test
    public void SingleConnectorMultipleDestinationsTest() {
        Map<String, String> props = new HashMap<>();
        String topic = "single_topic_test";
        Collection<SinkRecord> sr = cstt.createPrimitiveTypes(topic, 1);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        for (int i = 0; i < num_dbs; i++) {
            props.put(ClickHouseSinkConnector.HOSTNAME, dbs[i].getHost());
            props.put(ClickHouseSinkConnector.PORT, dbs[i].getFirstMappedPort().toString());
            props.put(ClickHouseSinkConnector.DATABASE, "default");
            props.put(ClickHouseSinkConnector.USERNAME, dbs[i].getUsername());
            props.put(ClickHouseSinkConnector.PASSWORD, dbs[i].getPassword());
            props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
            ClickHouseHelperClient chc = createClient(props);
            dropTable(chc, topic);
            createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
            chst.start(props);
            chst.put(sr);
            chst.stop();
            assertEquals(sr.size(), countRows(chc, topic));
        }
    }

    @Test
    public void SingleConnectorShardMultipleDestinationsTest() {
        Map<String, String> props = new HashMap<>();
        String topic = "single_topic_test";
        Collection<SinkRecord> sr = cstt.createPrimitiveTypes(topic, 1);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, dbs[0].getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, dbs[0].getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        for (int i = 0; i < num_dbs; i++) {
            props.put(ClickHouseSinkConnector.HOSTNAME, dbs[i].getHost());
            props.put(ClickHouseSinkConnector.PORT, dbs[i].getFirstMappedPort().toString());
            ClickHouseHelperClient chc = createClient(props);
            dropTable(chc, topic);
            createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        }
        String endpoints = "";
        for (ClickHouseContainer db : dbs) {endpoints += String.format("%s:%s,",db.getHost(),db.getFirstMappedPort().toString());}
        props.put(ClickHouseSinkConnector.ENDPOINTS, endpoints);
//        System.out.println(props);
        chst.start(props);
        chst.put(sr);
        chst.stop();
        int total_rows = 0;
        for (int i = 0; i < num_dbs; i++) {
            props.put(ClickHouseSinkConnector.HOSTNAME, dbs[i].getHost());
            props.put(ClickHouseSinkConnector.PORT, dbs[i].getFirstMappedPort().toString());
            ClickHouseHelperClient chc = createClient(props);
            total_rows +=countRows(chc, topic);
//            System.out.println("Shard "+i+" count: " +countRows(chc, topic));
        }
        assertEquals(sr.size(),total_rows);
    }

    @Test
    public void SingleConnectorMultipleTopicsShardMultipleDestinationsTest() {
        int num_topics = 2;
        List<String> topics = new ArrayList<>();
        Map<String, String> props = new HashMap<>();
        for (int j = 0; j < num_topics; j++) { topics.add("topic_"+j); }

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, dbs[0].getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, dbs[0].getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        for (int i = 0; i < num_dbs; i++) {
            props.put(ClickHouseSinkConnector.HOSTNAME, dbs[i].getHost());
            props.put(ClickHouseSinkConnector.PORT, dbs[i].getFirstMappedPort().toString());
            ClickHouseHelperClient chc = createClient(props);
            for (String topic : topics) {
                dropTable(chc, topic);
                createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
            }
        }
        String endpoints = "";
        for (ClickHouseContainer db : dbs) {endpoints += String.format("%s:%s,",db.getHost(),db.getFirstMappedPort().toString());}
        props.put(ClickHouseSinkConnector.ENDPOINTS, endpoints);
        props.put(ClickHouseSinkConnector.HASH_FUNCTION_NAME, "SHA-512");
//        System.out.println(props);
        chst.start(props);
        int sr_rows = 0;
        for (String topic : topics) {
            Collection<SinkRecord> sr = cstt.createPrimitiveTypes(topic, 1);
            chst.put(sr);
            sr_rows+=sr.size();
        }
        chst.stop();
        int total_rows = 0;
        for (String topic : topics) {
            for (int i = 0; i < num_dbs; i++) {
                props.put(ClickHouseSinkConnector.HOSTNAME, dbs[i].getHost());
                props.put(ClickHouseSinkConnector.PORT, dbs[i].getFirstMappedPort().toString());
                ClickHouseHelperClient chc = createClient(props);
                total_rows += countRows(chc, topic);
//            System.out.println(topic+": Shard "+i+" count: " +countRows(chc, topic));
            }
        }
        assertEquals(sr_rows,total_rows);
//        TimeUnit.MINUTES.sleep(5);
    }
}
