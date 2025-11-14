package com.clickhouse.kafka.connect.util.jmx;

public interface TopicStatisticsMBean {

    /**
     * Total successful records in the topic.
     * Implemented as counter. Reset on restart.
     *
     * @return counter value
     */
    long getTotalSuccessfulRecords();

    /**
     * Total successful batches in the topic.
     * Implemented as counter. Reset on restart.
     *
     * @return counter value
     */
    long getTotalSuccessfulBatches();

    /**
     * Total failed batches in the topic.
     * Implemented as counter. Reset on restart.
     *
     * @return counter value
     */
    long getTotalFailedBatches();

    /**
     * Total failed records in the topic.
     * Implemented as counter. Reset on restart.
     *
     * @return counter value
     */
    long getTotalFailedRecords();


    /**
     * Mean receive lag what is calculated as receive by task time - record timestamp.
     * Calculated using a first record timestamp in the batch.
     * Implemented as simple moving average. Reset on restart.
     *
     * @return counter value
     */
    long getMeanInsertTime();
}
