package com.clickhouse.kafka.connect.sink.state.provider;

import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.StateRecord;

public class RedisStateProvider implements StateProvider {

    public RedisStateProvider() {
    }

    @Override
    public StateRecord getStateRecord(String topic, int partition) {
        return new StateRecord(topic, partition, 10, 0, State.BEFORE_PROCESSING);
    }
    @Override
    public void setStateRecord(StateRecord stateRecord) {

    }

}
