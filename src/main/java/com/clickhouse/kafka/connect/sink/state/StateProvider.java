package com.clickhouse.kafka.connect.sink.state;

public interface StateProvider {

    public StateRecord getStateRecord(String topic, int partition );


    public void setStateRecord(StateRecord stateRecord);

}
