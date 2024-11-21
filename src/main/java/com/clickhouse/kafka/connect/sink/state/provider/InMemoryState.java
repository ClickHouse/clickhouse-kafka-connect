package com.clickhouse.kafka.connect.sink.state.provider;

import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.StateRecord;

import java.util.HashMap;
import java.util.Map;

public class InMemoryState implements StateProvider {

    private Map<String, StateRecord> stateDB = null;
    public InMemoryState() {
        this.stateDB = new HashMap<>(10);
    }

    private String genKey(String topic, int partition) {
        return String.format("%s-%d", topic, partition);
    }
    @Override
    public StateRecord getStateRecord(String topic, int partition) {
        String key = genKey(topic, partition);
        if ( !stateDB.containsKey(key))
            return new StateRecord(topic, partition, -1 , -1, State.NONE);
        return stateDB.get(key);
    }

    @Override
    public void setStateRecord(StateRecord stateRecord) {
        String key = genKey(stateRecord.getTopic(), stateRecord.getPartition());
        stateDB.put(key, stateRecord);
    }
}
