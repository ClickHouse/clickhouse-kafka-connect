package com.clickhouse.kafka.connect.sink.clickhouse;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ClickhouseShardedTestContainersTest {
    private static ClickHouseHelperClient chc = null;
    private void Query(ClickHouseHelperClient chc, String query) {
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(chc.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format


                     .query(query)
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

    @Test
    public void CreateOneShardWithReplicasTest() throws IOException, InterruptedException {
        String table_name="test";
        int maxNumRep = 5;
        int numMsg=10;
        for (int i = 1; i <= maxNumRep; i++) {
            ClickhouseShardedTestContainers csc = new ClickhouseShardedTestContainers();
            csc.setClickhouseImageName("clickhouse/clickhouse-server:22.5"); // used in opensource env
            csc.addShard(i);
            csc.start();
            Map<String, String> props = new HashMap<>();
            props.put(ClickHouseSinkConnector.DATABASE, "default");
            props.put(ClickHouseSinkConnector.USERNAME, "default");
            props.put(ClickHouseSinkConnector.PASSWORD, "");
            props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

            for (var db : csc.getClickhouseInstances()) {
                props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
                props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
                ClickHouseHelperClient chc = createClient(props);
                Query(chc, String.format("CREATE TABLE %s ( id UInt16,name String ) Engine = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/test', '{replica}') PARTITION BY id %% 2 ORDER BY id;", table_name));

            }

            for (int j = 0; j < numMsg; j++) {
                Query(chc, String.format("insert into %s values(%s,'%s');", table_name, j, j));
            }
            TimeUnit.SECONDS.sleep(5);
            for (var db : csc.getClickhouseInstances()) {
                props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
                props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
                ClickHouseHelperClient chc = createClient(props);
                assert countRows(chc, table_name) == numMsg;
            }
            csc.tearDown();
        }
    }

    @Test
    public void CreateTwoShardWithReplicasTest() throws IOException, InterruptedException {
        String table_name = "test";
        int S1PRep = 5;
        int S2PRep = 3;
        int numMsgS1 = 10;
        int numMsgS2 = 20;
        ClickhouseShardedTestContainers csc = new ClickhouseShardedTestContainers();
        csc.setClickhouseImageName("clickhouse/clickhouse-server:22.5"); // used in opensource env
        csc.addShard(S1PRep);
        csc.addShard(S2PRep);
        csc.start();


        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, "default");
        props.put(ClickHouseSinkConnector.PASSWORD, "");
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        for (var db : csc.getClickhouseInstances()) {
            props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
            props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
            ClickHouseHelperClient chc = createClient(props);
            Query(chc, String.format("CREATE TABLE %s ( id UInt16,name String ) Engine = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/test', '{replica}') PARTITION BY id %% 2 ORDER BY id;", table_name));
        }

        //insert to shard 1
        props.put(ClickHouseSinkConnector.HOSTNAME, csc.getClickhouseInstances().get(0).getHost());
        props.put(ClickHouseSinkConnector.PORT, csc.getClickhouseInstances().get(0).getFirstMappedPort().toString());
        ClickHouseHelperClient chc = createClient(props);
        for (int j = 0; j < numMsgS1; j++) {
            Query(chc, String.format("insert into %s values(%s,'%s');", table_name, j, j));
        }

        //insert to shard 2
        props.put(ClickHouseSinkConnector.HOSTNAME, csc.getClickhouseInstances().get(S1PRep).getHost());
        props.put(ClickHouseSinkConnector.PORT, csc.getClickhouseInstances().get(S1PRep).getFirstMappedPort().toString());
        chc = createClient(props);
        for (int j = 0; j < numMsgS2; j++) {
            Query(chc, String.format("insert into %s values(%s,'%s');", table_name, j, j));
        }

        TimeUnit.SECONDS.sleep(5);
        for (int i = 0; i < S1PRep + S2PRep; i++) {
            props.put(ClickHouseSinkConnector.HOSTNAME, csc.getClickhouseInstances().get(i).getHost());
            props.put(ClickHouseSinkConnector.PORT, csc.getClickhouseInstances().get(i).getFirstMappedPort().toString());
            chc = createClient(props);
            if (i < S1PRep) {
                assert countRows(chc, table_name) == numMsgS1;
            } else {
                assert countRows(chc, table_name) == numMsgS2;
            }
        }
        csc.tearDown();
    }

}
