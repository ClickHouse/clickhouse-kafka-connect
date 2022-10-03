package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.data.SchemaType;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.util.Mask;
import jdk.jfr.Description;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ClickHouseContainer;

import java.util.*;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClickHouseDbWriterTest {

    private static ClickHouseContainer db = null;
    private static ClickHouseWriter chw = null;

    private static ClickHouseHelperClient chc = null;

    private static int count = 3;

    private List<Record> createRecords(String topic, int partition) {
        // create records
        List<Record> records = new ArrayList<>(count);
        Field fieldOff8 = new Field("off8", 0, null);
        Field fieldOff16 = new Field("off16", 0, null);
        Field fieldOff32 = new Field("off32", 0, null);
        Field fieldOff64 = new Field("off64", 0, null);
        Field fieldOfff32 = new Field("offf32", 0, null);
        Field fieldOfff64 = new Field("offf64", 0, null);
        Field fieldBool = new Field("bool", 0, null);
        Field fieldString = new Field("str", 0, null);
//        Field fieldDouble = new Field("double", 0, null);
//        Field fieldArray = new Field("arr", 0, null);
//        Field fieldBool = new Field("bool", 0, null);


        List<Field> fList = new ArrayList<>();
        fList.add(fieldOff8);
        fList.add(fieldOff16);
        fList.add(fieldOff32);
        fList.add(fieldOff64);
        fList.add(fieldOfff32);
        fList.add(fieldOfff64);
        fList.add(fieldBool);
        fList.add(fieldString);
//        fList.add(fieldDouble);
//        fList.add(fieldArray);
//        fList.add(fieldBool);

        LongStream.range(0, count).forEachOrdered(n -> {
            SinkRecord sr = new SinkRecord(topic,
                    partition,
                    null,
                    null,
                    null,
                    null,
                    0);

            List<Integer> iList = new ArrayList<>();

            iList.add(Integer.valueOf((int)n));
            iList.add(Integer.valueOf((int)n+1));
            iList.add(Integer.valueOf((int)n+2));
            iList.add(Integer.valueOf((int)n+3));

            Map<String, Data> dataMap  = new HashMap<String, Data>() {{
                put("off8", new Data(Schema.Type.INT8, (byte)n));
                put("off16", new Data(Schema.Type.INT16, (short)n));
                put("off32", new Data(Schema.Type.INT32, (int)n));
                put("off64", new Data(Schema.Type.INT64, n));
                put("offf32", new Data(Schema.Type.FLOAT32, (float)(n*1.1)));
                put("offf64", new Data(Schema.Type.FLOAT64, (n*1.1)));
                put("bool", new Data(Schema.Type.BOOLEAN, true));
                put("str", new Data(Schema.Type.STRING, "" + n));
//                put("double", new Data(Schema.Type.FLOAT64,n * 1.1));
//                put("arr", new Data(Schema.Type.ARRAY, iList));
//                put("bool", new Data(Schema.Type.BOOLEAN,true));
            }};
            Record record = Record.newRecord(SchemaType.SCHEMA, topic, partition, n, fList, dataMap, sr);
            records.add(record);
        });
        return records;
    }

    private void createTable(String topic) {
//        String createTable = String.format("CREATE TABLE %s ( `off` Int8 , `str` String , `double` DOUBLE, `arr` Array(Int8),  `bool` BOOLEAN)  Engine = MergeTree ORDER BY off", topic);
        String createTable = String.format("CREATE TABLE %s ( `off8` Int8, `off16` Int16, `off32` Int32, `off64` Int64, `offf32` Float32, `offf64` Float64, `bool` BOOLEAN, `str` String)  Engine = MergeTree ORDER BY off8", topic);

        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(chc.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format


                     .query(createTable)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();

        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }

    }

    private void dropTable(String tableName) {
        String dropTable = String.format("DROP TABLE IF EXISTS %s", tableName);
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

    private int countRows(String topic) {
        String queryCount = String.format("select count(*) from %s", topic);
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


    @BeforeAll
    private static void setup() {
        db = new ClickHouseContainer("clickhouse/clickhouse-server:22.5");
        db.start();

    }


    @Test
    @Description("write to table according topic name to local ClickHouse")
    public void WriteTableLocalAccordingTopicNameTest() {
        chw = new ClickHouseWriter();
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

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

        dropTable("table_name_test");
        createTable("table_name_test");


        chw.start(csc);

        List<Record> records = createRecords("table_name_test", 1);
        // Let's create a table;
        chw.doInsert(records);

        // Let's check that we inserted all records
        assertEquals(records.size(), countRows("table_name_test"));
    }

    @Test
    @Description("write to table according topic name to Cloud ClickHouse")
    public void WriteTableCloudAccordingTopicNameTest() throws InterruptedException {

        String hostname = System.getenv("HOST");
        String port = System.getenv("PORT");
        String password = System.getenv("PASSWORD");
        // TODO: we need to ignore the test if there is not ENV variables
        if (hostname == null || port == null || password == null)
            throw new RuntimeException("Can not continue missing env variables.");

        chw = new ClickHouseWriter();
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, hostname);
        props.put(ClickHouseSinkConnector.PORT, port);
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, "default");
        props.put(ClickHouseSinkConnector.PASSWORD, password);
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "true");
        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(props);

        chc = new ClickHouseHelperClient.ClickHouseClientBuilder(csc.getHostname(), csc.getPort())
                .setDatabase(csc.getDatabase())
                .setUsername(csc.getUsername())
                .setPassword(csc.getPassword())
                .sslEnable(csc.isSslEnabled())
                .setTimeout(csc.getTimeout())
                .setRetry(csc.getRetry())
                .build();



        dropTable("table_name_test");
        createTable("table_name_test");

        chw.start(csc);

        List<Record> records = createRecords("table_name_test", 1);
        // Let's create a table;
        chw.doInsert(records);
        //Thread.sleep(5 * 1000);
        // Let's check that we inserted all records
        assertEquals(records.size(), countRows("table_name_test"));
    }

    @AfterAll
    protected static void tearDown() {
        db.stop();
    }
}
