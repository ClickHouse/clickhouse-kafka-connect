package com.clickhouse.kafka.connect.sink.state;

public enum State {
    NONE(1),
    BEFORE_PROCESSING(2),
    IN_PROCESSING(3),
    AFTER_PROCESSING(4);


    private int state;

    State(int state) {
        this.state = state;
    }
}
