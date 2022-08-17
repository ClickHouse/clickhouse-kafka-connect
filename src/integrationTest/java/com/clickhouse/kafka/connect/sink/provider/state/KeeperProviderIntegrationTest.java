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


    public static KeeperStateProvider ksp = null;
    public static ClickHouseHelperClient chc = null;
    @BeforeAll
    private static void setup() {
        chc = new ClickHouseHelperClient.ClickHouseClientBuilder("localhost", 8123)
                .setUsername("default")
                .build();
    }


    //@Test
    @Description("Keeper set get test")
    public void setGetTest() {

        String dropTable = String.format("DROP TABLE IF EXISTS %s", "connect_state");
        chc.query(dropTable);
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

    //@Test
    @Description("Keeper set update get test")
    public void setUpdateGetTest() {

        String dropTable = String.format("DROP TABLE IF EXISTS %s", "connect_state");
        chc.query(dropTable);
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
