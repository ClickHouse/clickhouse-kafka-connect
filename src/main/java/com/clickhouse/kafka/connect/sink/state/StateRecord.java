package com.clickhouse.kafka.connect.sink.state;

import com.clickhouse.kafka.connect.sink.kafka.RangeContainer;

public class StateRecord extends RangeContainer {
    private State state;

    public StateRecord(String topic, int partition , long maxOffset, long minOffset, State state) {
        super(topic, partition, maxOffset, minOffset);
        this.state = state;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }
}
