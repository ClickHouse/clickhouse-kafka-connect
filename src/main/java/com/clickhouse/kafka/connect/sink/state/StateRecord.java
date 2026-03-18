package com.clickhouse.kafka.connect.sink.state;

import com.clickhouse.kafka.connect.sink.kafka.RangeContainer;

import java.util.Objects;

public class StateRecord extends RangeContainer {
    private State state;
    private String originalTopic;

    public StateRecord(String topic, int partition , long maxOffset, long minOffset, State state, String originalTopic) {
        super(topic, partition, maxOffset, minOffset);
        this.state = state;
        this.originalTopic = originalTopic;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public String getOriginalTopic() {
        return originalTopic;
    }

    public void setOriginalTopic(String topic) {
        this.originalTopic = topic;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StateRecord)) return false;
        //if (!super.equals(o)) return false; //If we overrode it there

        StateRecord that = (StateRecord) o;

        return Objects.equals(this.topic, that.topic)
                && this.partition == that.partition
                && this.state == that.state
                && this.getMinOffset() == that.getMinOffset()
                && this.getMaxOffset() == that.getMaxOffset();
    }

    public String toString() {
        return "StateRecord{" +
                "topic='" + topic + "'" +
                ", partition=" + partition +
                ", state='" + state + "'" +
                ", minOffset=" + getMinOffset() +
                ", maxOffset=" + getMaxOffset() +
                '}';
    }
}
