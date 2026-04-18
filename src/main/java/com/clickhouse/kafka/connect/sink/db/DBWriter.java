package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import com.clickhouse.kafka.connect.util.QueryIdentifier;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public interface DBWriter {

    boolean start(ClickHouseSinkConfig csc);
    void stop();
    void doInsert(List<Record> records, QueryIdentifier queryId) throws IOException, ExecutionException, InterruptedException;
    void doInsert(List<Record> records, QueryIdentifier queryId, ErrorReporter errorReporter) throws IOException, ExecutionException, InterruptedException;
    long recordsInserted();
}
