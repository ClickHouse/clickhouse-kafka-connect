package com.clickhouse.kafka.connect.util.jmx;

public interface SinkTaskStatisticsMBean {

    long getReceivedRecords();

    long getRecordProcessingTime();

    long getTaskProcessingTime();

}
