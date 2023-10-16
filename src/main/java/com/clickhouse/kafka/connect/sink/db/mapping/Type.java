package com.clickhouse.kafka.connect.sink.db.mapping;

public enum Type {
    NONE,
    INT8,
    INT16,
    INT32,
    INT64,
    INT128,
    INT256,
    STRING,
    FLOAT32,
    FLOAT64,
    BOOLEAN,
    ARRAY,
    MAP,
    Date,
    Date32,
    DateTime,
    DateTime64,
    UUID,
    UINT8,
    UINT16,
    UINT32,
    UINT64,
    UINT128,
    UINT256,
    Decimal
}
