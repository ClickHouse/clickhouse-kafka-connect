package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.ClickHouseWriter;
import jdk.jfr.Description;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.ConfluentPlatform;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClickHouseSinkConnectorIntegrationTest {

    /*


    {
  "side": "SELL",
  "quantity": 3737,
  "symbol": "ZVV",
  "price": 710,
  "account": "LMN456",
  "userid": "User_9"
}

{
  "connect.name": "ksql.StockTrade",
  "connect.parameters": {
    "io.confluent.connect.avro.field.doc.account": "Simulated accounts assigned to the trade",
    "io.confluent.connect.avro.field.doc.price": "A simulated random trade price in pennies",
    "io.confluent.connect.avro.field.doc.quantity": "A simulated random quantity of the trade",
    "io.confluent.connect.avro.field.doc.side": "A simulated trade side (buy or sell or short)",
    "io.confluent.connect.avro.field.doc.symbol": "Simulated stock symbols",
    "io.confluent.connect.avro.field.doc.userid": "The simulated user who executed the trade",
    "io.confluent.connect.avro.record.doc": "Defines a hypothetical stock trade using some known test stock symbols."
  },
  "doc": "Defines a hypothetical stock trade using some known test stock symbols.",
  "fields": [
    {
      "doc": "A simulated trade side (buy or sell or short)",
      "name": "side",
      "type": "string"
    },
    {
      "doc": "A simulated random quantity of the trade",
      "name": "quantity",
      "type": "int"
    },
    {
      "doc": "Simulated stock symbols",
      "name": "symbol",
      "type": "string"
    },
    {
      "doc": "A simulated random trade price in pennies",
      "name": "price",
      "type": "int"
    },
    {
      "doc": "Simulated accounts assigned to the trade",
      "name": "account",
      "type": "string"
    },
    {
      "doc": "The simulated user who executed the trade",
      "name": "userid",
      "type": "string"
    }
  ],
  "name": "StockTrade",
  "namespace": "ksql",
  "type": "record"
}

     */

    private static String hostname = null;
    private static String port = null;
    private static String password = null;

    public static ConfluentPlatform confluentPlatform = null;
    private static ClickHouseContainer db = null;
    private static ClickHouseWriter chw = null;
    private static ClickHouseHelperClient chc = null;

    @BeforeAll
    private static void setup() {
        hostname = System.getenv("HOST");
        port = System.getenv("PORT");
        password = System.getenv("PASSWORD");
        // TODO: we need to ignore the test if there is not ENV variables
        if (hostname == null || port == null || password == null)
            throw new RuntimeException("Can not continue missing env variables.");

        db = new ClickHouseContainer("clickhouse/clickhouse-server:22.5");
        db.start();

        chc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, Integer.valueOf(port))
                .setUsername("default")
                .setPassword(password)
                .sslEnable(true)
                .build();

        System.out.println("ping");
        System.out.println(chc.ping());

        List<String> connectorPath = new LinkedList<>();
        String confluentArchive = new File(Paths.get("build/confluentArchive").toString()).getAbsolutePath();
        connectorPath.add(confluentArchive);
        confluentPlatform = new ConfluentPlatform(connectorPath);
    }


    private void dropTable(String tableName) {
        String dropTable = String.format("DROP TABLE IF EXISTS %s", tableName);
        chc.query(dropTable);
    }
    private void createTable(String tableName) {
        String createTable = String.format("CREATE TABLE %s ( `side` String, `quantity` Int32, `symbol` String, `price` Int32, `account` String, `userid` String )  Engine = MergeTree ORDER BY symbol", tableName);
        chc.query(createTable);
    }

    private int countRows(String topic) {
        String queryCount = String.format("select count(*) from %s", topic);
//        ClickHouseResponse response = chc.query(queryCount);
//        return response.firstRecord().getValue(0).asInteger();
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


    private void sleep(long l) {
        try {
            Thread.sleep(l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    @Description("stockGenSingleTask")
    public void stockGenSingleTaskTest() throws IOException {

        String topicName = "stock_gen_topic_single_task";
        int parCount = 1;
        String payloadDataGen = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/stock_gen.json")));

        confluentPlatform.createTopic(topicName, 1);
        confluentPlatform.createConnect(String.format(payloadDataGen, "DatagenConnectorConnector_Single", "DatagenConnectorConnector_Single", parCount, topicName));

        // Now let's create the correct table & configure Sink to insert data to ClickHouse
        dropTable(topicName);
        createTable(topicName);
        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink.json")));

        sleep(5 * 1000);

        confluentPlatform.createConnect(String.format(payloadClickHouseSink, "ClickHouseSinkConnectorConnector_Single", "ClickHouseSinkConnectorConnector_Single", parCount, topicName, hostname, port, password));

        long count = 0;
        while (count < 10000) {
            sleep(5*1000);
            long endOffset = confluentPlatform.getOffset(topicName, 0 );
            if (endOffset % 100 == 0)
                System.out.println(endOffset);
            if (endOffset == 10000) {
                break;
            }
            count+=1;
        }
        // TODO : see the progress of the offset currently it is 1 min
        sleep(30 * 1000);


        count = countRows(topicName);
        System.out.println(count);
        while (count < 10000) {
            long tmpCount = countRows(topicName);
            System.out.println(tmpCount);
            sleep(2 * 1000);
            if (tmpCount > count)
                count = tmpCount;
        }
        assertEquals(10000, countRows(topicName));

    }

    @Test
    @Description("stockMultiTask")
    public void stockGenMultiTaskTest() throws IOException {

        String topicName = "stock_gen_topic_multi_task";
        int parCount = 3;
        String payloadDataGen = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/stock_gen.json")));

        confluentPlatform.createTopic(topicName, parCount);
        confluentPlatform.createConnect(String.format(payloadDataGen, "DatagenConnectorConnector_Multi", "DatagenConnectorConnector_Multi", parCount, topicName));

        // Now let's create the correct table & configure Sink to insert data to ClickHouse
        dropTable(topicName);
        createTable(topicName);
        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink.json")));

        sleep(5 * 1000);

        confluentPlatform.createConnect(String.format(payloadClickHouseSink, "ClickHouseSinkConnectorConnector_Multi", "ClickHouseSinkConnectorConnector_Multi", parCount, topicName, hostname, port, password));

        long count = 0;
        count = countRows(topicName);
        System.out.println(count);
        while (count < 10000 * parCount) {
            long tmpCount = countRows(topicName);
            System.out.println(tmpCount);
            sleep(2 * 1000);
            if (tmpCount > count)
                count = tmpCount;
        }
        assertEquals(10000 * parCount, countRows(topicName));

    }

    @Test
    @Description("stockMultiTaskTopic")
    public void stockGenMultiTaskTopicTest() throws IOException {

        String topicName01 = "stock_gen_topic_multi_task_01";
        String topicName02 = "stock_gen_topic_multi_task_02";
        int parCount = 3;
        String payloadDataGen = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/stock_gen.json")));

        confluentPlatform.createTopic(topicName01, parCount);
        confluentPlatform.createTopic(topicName02, parCount);

        confluentPlatform.createConnect(String.format(payloadDataGen, "DatagenConnectorConnector_Multi_01", "DatagenConnectorConnector_Multi_01", parCount, topicName01));
        confluentPlatform.createConnect(String.format(payloadDataGen, "DatagenConnectorConnector_Multi_02", "DatagenConnectorConnector_Multi_02", parCount, topicName02));

        // Now let's create the correct table & configure Sink to insert data to ClickHouse
        dropTable(topicName01);
        dropTable(topicName02);
        createTable(topicName01);
        createTable(topicName02);
        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink.json")));

        sleep(5 * 1000);

        confluentPlatform.createConnect(String.format(payloadClickHouseSink, "ClickHouseSinkConnectorConnector_Multi_01", "ClickHouseSinkConnectorConnector_Multi_01", parCount, topicName01, hostname, port, password));
        confluentPlatform.createConnect(String.format(payloadClickHouseSink, "ClickHouseSinkConnectorConnector_Multi_02", "ClickHouseSinkConnectorConnector_Multi_02", parCount, topicName02, hostname, port, password));

        long count01 = 0;
        long count02 = 0;
        count01 = countRows(topicName01);
        System.out.println(count01);
        while (count01 < 10000 * parCount) {
            long tmpCount = countRows(topicName01);
            System.out.println(tmpCount);
            sleep(2 * 1000);
            if (tmpCount > count01)
                count01 = tmpCount;
        }

        count02 = countRows(topicName01);
        System.out.println(count02);
        while (count02 < 10000 * parCount) {
            long tmpCount = countRows(topicName01);
            System.out.println(tmpCount);
            sleep(2 * 1000);
            if (tmpCount > count02)
                count02 = tmpCount;
        }

        assertEquals(10000 * parCount, countRows(topicName01));
        assertEquals(10000 * parCount, countRows(topicName02));

    }

    @AfterAll
    protected static void tearDown() {
        db.stop();
    }

}
