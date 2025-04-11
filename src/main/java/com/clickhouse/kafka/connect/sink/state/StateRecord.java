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

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StateRecord)) return false;
        //if (!super.equals(o)) return false; //If we overrode it there

        StateRecord that = (StateRecord) o;

        return this.topic.equals(that.topic)
                && this.partition == that.partition
                && this.state == that.state
                && this.getMinOffset() == that.getMinOffset()
                && this.getMaxOffset() == that.getMaxOffset();
    }
}
