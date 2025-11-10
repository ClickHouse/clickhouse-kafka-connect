package com.clickhouse.kafka.connect.util.jmx;

import com.codahale.metrics.ExponentialMovingAverages;

import java.util.concurrent.atomic.AtomicLong;

public class TopicStatistics implements TopicStatisticsMBean {

    private final AtomicLong totalInsertedRecords;
    private final AtomicLong totalNumberOfBatches;
    private final AtomicLong totalFailedBatches;
    private final AtomicLong totalFailedRecords;

    private final ExponentialMovingAverages insertTime;

    public TopicStatistics() {
        totalInsertedRecords = new AtomicLong(0);
        totalNumberOfBatches = new AtomicLong(0);
        totalFailedBatches = new AtomicLong(0);
        totalFailedRecords = new AtomicLong(0);
        insertTime = new ExponentialMovingAverages();
    }


    @Override
    public long getTotalSuccessfulRecords() {
        return totalInsertedRecords.get();
    }

    @Override
    public long getTotalSuccessfulBatches() {
        return totalNumberOfBatches.get();
    }


    @Override
    public long getMeanInsertTime() {
        return Double.valueOf(insertTime.getM1Rate()).longValue();
    }

    @Override
    public long getTotalFailedBatches() {
        return totalFailedBatches.get();
    }

    @Override
    public long getTotalFailedRecords() {
        return totalFailedRecords.get();
    }

    public void recordsInserted(long n) {
        totalInsertedRecords.addAndGet(n);
    }

    public void batchInserted(long n) {
        totalNumberOfBatches.addAndGet(n);
    }

    public void batchesFailed(long n) {
        totalFailedBatches.addAndGet(n);
    }

    public void recordsFailed(long n) {
        totalFailedRecords.addAndGet(n);
    }

    public void insertTime(long insertTime) {
        this.insertTime.update(insertTime);
        this.insertTime.tickIfNecessary();
    }
}
