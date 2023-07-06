package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.utility.MountableFile;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class ClickHouseConnectShardsTest {
    private static int num_shards = 2;
    private static int num_replicas = 3;
    private static ClickHouseContainer db = null;
    private static ClickHouseHelperClient chc = null;
    ClickHouseSinkTaskSchemalessTest cstt = new ClickHouseSinkTaskSchemalessTest();

    @BeforeAll
    private static void setup() {
        db = new ClickHouseContainer("clickhouse/clickhouse-server:22.5");
        db.withCopyToContainer(MountableFile.forHostPath("src/test/resources/clickhouse_tester_config.xml"),"/etc/clickhouse-server/config.xml");
        db.start();
    }

    private ClickHouseHelperClient createClient(Map<String, String> props) {
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
    public void ClickHouseReplicaAndShardsTest() throws InterruptedException {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        ClickHouseHelperClient chc = createClient(props);
//        TimeUnit.HOURS.sleep(1);
        createTable(chc, "spe", "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");


        for (var t : chc.showTables()){
            System.out.println("Table:"+t);
        }


    }
}
