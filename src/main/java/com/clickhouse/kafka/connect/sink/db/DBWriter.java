package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;

import java.util.List;
import java.util.Map;

public interface DBWriter {

    public boolean start(ClickHouseSinkConfig csc);
    public void stop();
    public void doInsert(List<Record> records);
    public void doInsert(List<Record> records, ErrorReporter errorReporter);
    public long recordsInserted();
}
