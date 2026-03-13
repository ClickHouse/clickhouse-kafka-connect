package com.clickhouse.kafka.connect.sink.state;

import java.util.function.Consumer;

public interface StateProvider {

    public StateRecord getStateRecord(String topic, int partition );


    public void setStateRecord(StateRecord stateRecord);

    void setStateUpdateListener(Consumer<StateRecord> callback);
}
