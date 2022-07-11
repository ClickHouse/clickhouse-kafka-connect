package com.clickhouse.kafka.connect.sink.dedup;

public enum DeDupStrategy {

    OFF,
    PRIMARY_KEY,
    ALL_DATA,
}
