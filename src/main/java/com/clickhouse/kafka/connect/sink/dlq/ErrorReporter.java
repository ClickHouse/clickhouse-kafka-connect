package com.clickhouse.kafka.connect.sink.dlq;

import org.apache.kafka.connect.sink.SinkRecord;

public interface ErrorReporter {
    void report(SinkRecord record, Exception e);
}
