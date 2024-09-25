package com.clickhouse.kafka.connect.sink.kafka;

public enum RangeState {
    ZERO(0), //This is for when it seems like the topic has been deleted/recreated
    SAME(1),
    PREFIX(2),
    SUFFIX(3),
    CONTAINS(4),
    OVER_LAPPING(5),
    NEW(6),
    ERROR(7),
    PREVIOUS(8);


    private int rangeState;

    RangeState(int rangeState) {
        this.rangeState = rangeState;
    }
}
