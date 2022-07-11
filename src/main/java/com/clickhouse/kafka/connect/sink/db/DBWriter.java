package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.kafka.connect.sink.data.Record;

import java.util.List;

public interface DBWriter {

    public void start();
    public void stop();
    public void doInsert(List<Record> records);
    public long recordsInserted();
}
