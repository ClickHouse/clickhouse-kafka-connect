package com.clickhouse.kafka.connect.sink.provider.state;

import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateRecord;
import com.clickhouse.kafka.connect.sink.state.provider.RedisStateProvider;
import jdk.jfr.Description;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisProviderIntegrationTest {


    public static GenericContainer redis = null;

    @BeforeAll
    private static void setup() {
        redis = new GenericContainer("redis:7.0.4-bullseye")
                .withExposedPorts(6379);
        redis.start();
    }


    @Test
    @Description("Redis get set test")
    public void getSetTest() {
        String host = redis.getHost();
        int port = redis.getMappedPort(6379);

        RedisStateProvider rsp = new RedisStateProvider(host, port, Optional.empty());

        StateRecord st = new StateRecord("redis_test", 2, 10,0, State.BEFORE_PROCESSING);
        rsp.setStateRecord(st);

        StateRecord stGet =  rsp.getStateRecord("redis_test", 2);
        assertEquals(stGet.getTopic(), st.getTopic(), "Check topic");
        assertEquals(stGet.getPartition(), st.getPartition(), "Check partition");
        assertEquals(stGet.getMaxOffset(), st.getMaxOffset(), "Check max offset");
        assertEquals(stGet.getMinOffset(), st.getMinOffset(), "Check min offset");
        assertEquals(stGet.getState(), st.getState(),"Check state");

    }

    @AfterAll
    protected static void tearDown() {
        redis.close();
    }

}
