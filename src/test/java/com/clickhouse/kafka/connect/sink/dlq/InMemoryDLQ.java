package com.clickhouse.kafka.connect.sink.dlq;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;

public class InMemoryDLQ implements ErrorReporter {


    private class DLQRecord {
        private SinkRecord record = null;
        private Throwable t = null;

        public DLQRecord(SinkRecord record, Throwable t) {
            this.record = record;
            this.t = t;
        }
    }
    private List<DLQRecord> dlq = null;

    public InMemoryDLQ() {
        this.dlq = new ArrayList<DLQRecord>();

    }

    @Override
    public void report(SinkRecord record, Exception e) {
        dlq.add(new DLQRecord(record, e));
    }

    public int size() {
        return dlq.size();
    }
}
