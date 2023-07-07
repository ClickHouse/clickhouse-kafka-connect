package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.clickhouse.ClickhouseShardedTestContainers;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

public class ClickHouseConnectShardsTest {
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

    public Collection<SinkRecord> createRecords(String topic, int numMsgs) {
        List<SinkRecord> array = new ArrayList<>();
        if (numMsgs<1){return array;}
        LongStream.range(0, numMsgs).forEachOrdered(n -> {
            Map<String, Object> value_struct = new HashMap<>();
            value_struct.put("id", n);
            value_struct.put("name", "data" + n);
            SinkRecord sr = new SinkRecord(
                    topic,
                    1,
                    null,
                    null, null,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );
            array.add(sr);
        });
        return array;
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
                     .query(dropTable)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();

        } catch (ClickHouseException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void CreateTwoShardWithReplicasTest() throws IOException, InterruptedException {
        String table_name = "test";
        int S1PRep = 5;
        int S2PRep = 3;
        int numMsg = 1000;
        ClickhouseShardedTestContainers csc = new ClickhouseShardedTestContainers();
        csc.setClickhouseImageName("clickhouse/clickhouse-server:22.5"); // used in opensource env
        csc.addShard(S1PRep);
        csc.addShard(S2PRep);
        csc.start();

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        Collection<SinkRecord> sr = createRecords(table_name,numMsg);

        List<String> shard_list= new ArrayList<>();
        for (var inst : csc.getClickhouseInstances()) {
            shard_list.add(inst.getHost() + ":" + inst.getFirstMappedPort().toString());
        }
        String shard_string = String.join(",",shard_list.subList(0,S1PRep))+";"
                +String.join(",",shard_list.subList(S1PRep,S1PRep+S2PRep));

        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, "default");
        props.put(ClickHouseSinkConnector.PASSWORD, "");
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        props.put(ClickHouseSinkConnector.HASH_FUNCTION_NAME,"SHA-512");

        for (var db : csc.getClickhouseInstances()) {
            props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
            props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
            ClickHouseHelperClient chc = createClient(props);
            Query(chc, String.format("CREATE TABLE %s ( id UInt16,name String ) Engine = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/test', '{replica}') PARTITION BY id %% 2 ORDER BY id;", table_name));
        }

        //insert data
        props.put(ClickHouseSinkConnector.SHARDS, shard_string);
        chst.start(props);
        chst.put(sr);
        chst.stop();

        TimeUnit.SECONDS.sleep(5);
        int numMsgS1 = -1;
        int numMsgS2 = -1;
        for (int i = 0; i < S1PRep + S2PRep; i++) {
            props.put(ClickHouseSinkConnector.HOSTNAME, csc.getClickhouseInstances().get(i).getHost());
            props.put(ClickHouseSinkConnector.PORT, csc.getClickhouseInstances().get(i).getFirstMappedPort().toString());
            ClickHouseHelperClient chc = createClient(props);
            if (i < S1PRep) {
                if (numMsgS1==-1){numMsgS1=countRows(chc, table_name);}
                assert countRows(chc, table_name) == numMsgS1;
            } else {
                if (numMsgS2==-1){numMsgS2=countRows(chc, table_name);}
                assert countRows(chc, table_name) == numMsgS2;
            }
        }
        assert sr.size()==numMsgS2+numMsgS1;
        csc.tearDown();
    }

}
