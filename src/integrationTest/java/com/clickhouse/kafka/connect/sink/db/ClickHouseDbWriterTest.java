package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.data.Record;
import jdk.jfr.Description;
import org.apache.kafka.connect.data.Field;
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

    private List<Record> createRecords(String topic, int partition) {
        // create records
        List<Record> records = new ArrayList<>(1000);
        Field fieldOff = new Field("off", 0, null);
        Field fieldString = new Field("str", 0, null);
        Field fieldDouble = new Field("double", 0, null);
        Field fieldArray = new Field("arr", 0, null);
        Field fieldBool = new Field("bool", 0, null);


        List<Field> fList = new ArrayList<>();
        fList.add(fieldOff);
        fList.add(fieldString);
        fList.add(fieldDouble);
        fList.add(fieldArray);
        fList.add(fieldBool);

        LongStream.range(0, 1000).forEachOrdered(n -> {
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

            Map<String, Object> dataMap  = new HashMap<String, Object>() {{
                put("off", n);
                put("str", "" + n);
                put("double",  n * 1.1);
                put("arr", iList);
                put("bool", true);
            }};
            Record record = Record.newRecord(topic, partition, n, fList, dataMap, sr);
            records.add(record);
        });
        return records;
    }

    private void createTable(String topic) {
        String createTable = String.format("CREATE TABLE %s ( `off` Int8 , `str` String , `double` DOUBLE, `arr` Array(Int8),  `bool` BOOLEAN)  Engine = MergeTree ORDER BY off", topic);

        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(chw.getServer()) // or client.connect(endpoints)
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
             ClickHouseResponse response = client.connect(chw.getServer()) // or client.connect(endpoints)
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
             ClickHouseResponse response = client.connect(chw.getServer()) // or client.connect(endpoints)
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
        chw.start(props);

        List<Record> records = createRecords("table_name_test", 1);
        // Let's create a table;
        dropTable("table_name_test");
        createTable("table_name_test");
        chw.doInsert(records);

        // Let's check that we inserted all records
        assertEquals(records.size(), countRows("table_name_test"));
    }

    @Test
    @Description("write to table according topic name to Cloud ClickHouse")
    public void WriteTableCloudAccordingTopicNameTest() {

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
        chw.start(props);

        List<Record> records = createRecords("table_name_test", 1);
        // Let's create a table;
        dropTable("table_name_test");
        createTable("table_name_test");
        chw.doInsert(records);

        // Let's check that we inserted all records
        assertEquals(records.size(), countRows("table_name_test"));
    }

    @AfterAll
    protected static void tearDown() {
        db.stop();
    }
}
