package com.clickhouse.kafka.connect.sink.provider.state;

import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateRecord;
import com.clickhouse.kafka.connect.sink.state.provider.KeeperStateProvider;
import com.clickhouse.kafka.connect.sink.state.provider.RedisStateProvider;
import jdk.jfr.Description;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KeeperProviderIntegrationTest {


    private static String hostname = null;
    private static String port = null;
    private static String password = null;

    public static KeeperStateProvider ksp = null;
    public static ClickHouseHelperClient chc = null;
    @BeforeAll
    private static void setup() {
        hostname = System.getenv("HOST");
        port = System.getenv("PORT");
        password = System.getenv("PASSWORD");
        if (hostname == null || port == null || password == null)
            throw new RuntimeException("Can not continue missing env variables.");

        chc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, Integer.valueOf(port))
                .setUsername("default")
                .setDatabase("default")
                .setPassword(password)
                .sslEnable(true)
                .build();
    }

    @Test
    @Description("Keeper set get test")
    public void setGetTest() throws InterruptedException {
        // Drop keepermap table
        String dropTable = String.format("DROP TABLE IF EXISTS %s SYNC", "connect_state");
        chc.query(dropTable);
        // Create KeeperMap table
        String createTable = String.format("create table connect_state (`key` String, minOffset BIGINT, maxOffset BIGINT, state String) ENGINE=KeeperMap('/kafka-coonect') PRIMARY KEY `key`;" );
        chc.query(createTable);
        ksp = new KeeperStateProvider(chc);

        int partition = 11;

        StateRecord st = new StateRecord("keeper_test", partition, 10,0, State.BEFORE_PROCESSING);
        ksp.setStateRecord(st);

        StateRecord stGet =  ksp.getStateRecord("keeper_test", partition);
        assertEquals(stGet.getTopic(), st.getTopic(), "Check topic");
        assertEquals(stGet.getPartition(), st.getPartition(), "Check partition");
        assertEquals(stGet.getMaxOffset(), st.getMaxOffset(), "Check max offset");
        assertEquals(stGet.getMinOffset(), st.getMinOffset(), "Check min offset");
        assertEquals(stGet.getState(), st.getState(),"Check state");

    }

    @Test
    @Description("Keeper set update get test")
    public void setUpdateGetTest() {

        // Drop keepermap table
        String dropTable = String.format("DROP TABLE IF EXISTS %s SYNC", "connect_state");
        chc.query(dropTable);
        // Create KeeperMap table
        String createTable = String.format("create table connect_state (`key` String, minOffset BIGINT, maxOffset BIGINT, state String) ENGINE=KeeperMap('/kafka-coonect') PRIMARY KEY `key`;" );
        chc.query(createTable);
        ksp = new KeeperStateProvider(chc);

        int partition = 12;

        StateRecord st = new StateRecord("keeper_test", partition, 10,0, State.AFTER_PROCESSING);
        ksp.setStateRecord(st);

        StateRecord stGet =  ksp.getStateRecord("keeper_test", partition);
        assertEquals(stGet.getTopic(), st.getTopic(), "Check topic");
        assertEquals(stGet.getPartition(), st.getPartition(), "Check partition");
        assertEquals(stGet.getMaxOffset(), st.getMaxOffset(), "Check max offset");
        assertEquals(stGet.getMinOffset(), st.getMinOffset(), "Check min offset");
        assertEquals(stGet.getState(), st.getState(),"Check state");

        StateRecord stUpdate = new StateRecord("keeper_test", partition, 20,11, State.AFTER_PROCESSING);

        ksp.setStateRecord(stUpdate);

        stGet =  ksp.getStateRecord("keeper_test", partition);
        assertEquals(stGet.getTopic(), stUpdate.getTopic(), "Check topic");
        assertEquals(stGet.getPartition(), stUpdate.getPartition(), "Check partition");
        assertEquals(stGet.getMaxOffset(), stUpdate.getMaxOffset(), "Check max offset");
        assertEquals(stGet.getMinOffset(), stUpdate.getMinOffset(), "Check min offset");
        assertEquals(stGet.getState(), stUpdate.getState(),"Check state");


    }

    @AfterAll
    protected static void tearDown() {

    }

}
