package com.clickhouse.kafka.connect.util.jmx;

import java.util.concurrent.atomic.AtomicLong;

public class SinkTaskStatistics implements SinkTaskStatisticsMBean {
    private AtomicLong receivedRecords;
    private AtomicLong recordProcessingTime;
    private AtomicLong taskProcessingTime;

    public SinkTaskStatistics() {
        this.receivedRecords = new AtomicLong(0);
        this.recordProcessingTime = new AtomicLong(0);
        this.taskProcessingTime = new AtomicLong(0);
    }

    @Override
    public long getReceivedRecords() {
        return receivedRecords.get();
    }

    @Override
    public long getRecordProcessingTime() {
        return recordProcessingTime.get();
    }

    @Override
    public long getTaskProcessingTime() {
        return taskProcessingTime.get();
    }

    public void receivedRecords(final int n) {
        this.receivedRecords.addAndGet(n);
    }

    public void recordProcessingTime(ExecutionTimer timer) {
        this.recordProcessingTime.addAndGet(timer.nanosElapsed());
    }

    public void taskProcessingTime(ExecutionTimer timer) {
        this.taskProcessingTime.addAndGet(timer.nanosElapsed());
    }

}
