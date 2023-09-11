package com.clickhouse.kafka.connect.util.jmx;

public class SinkTaskStatistics implements SinkTaskStatisticsMBean {
    private volatile long receivedRecords;
    private volatile long recordProcessingTime;
    private volatile long taskProcessingTime;
    @Override
    public long getReceivedRecords() {
        return receivedRecords;
    }

    @Override
    public long getRecordProcessingTime() {
        return recordProcessingTime;
    }

    @Override
    public long getTaskProcessingTime() {
        return taskProcessingTime;
    }

    public void receivedRecords(final int n ) {
        this.receivedRecords += n;
    }

    public void recordProcessingTime(ExecutionTimer timer) {
        this.recordProcessingTime += timer.nanosElapsed();
    }

    public void taskProcessingTime(ExecutionTimer timer) {
        this.taskProcessingTime += timer.nanosElapsed();
    }

}
