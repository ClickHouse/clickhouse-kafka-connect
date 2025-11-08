package com.clickhouse.kafka.connect.util.jmx;

public interface TopicStatisticsMBean {

    long getTotalSuccessfulRecords();

    long getTotalSuccessfulBatches();

    long getTotalFailedBatches();

    long getTotalFailedRecords();

    long getMeanReceiveLag();

    long getMeanInsertTime();
}
