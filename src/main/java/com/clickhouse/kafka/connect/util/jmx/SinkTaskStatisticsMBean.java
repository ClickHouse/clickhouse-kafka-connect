package com.clickhouse.kafka.connect.util.jmx;

public interface SinkTaskStatisticsMBean {

    /**
     * Total number of records received by a Sink Task instance.
     * Implemented as counter and is reset on restart.
     *
     * @return counter value
     */
    long getReceivedRecords();

    /**
     * Total time in milliseconds spent in analyzing records and grouping by topic.
     * Implemented as counter and is reset on restart.
     *
     * @return counter value in milliseconds
     */
    long getRecordProcessingTime();

    /**
     * Total time in milliseconds spent in whole include pre-processing and insert.
     * Implemented as counter and is reset on restart.
     *
     * @return counter value in milliseconds
     */
    long getTaskProcessingTime();

    /**
     * Total number of records inserted by Sink Task instance.
     * Implemented as counter and is reset on restart.
     *
     * @return counter value
     */
    long getInsertedRecords();

    /**
     * Total number of inserted bytes. Limited to Long.MAX_VALUE (8192 PB);
     * Implemented as counter and is reset on restart.
     *
     * @return counter value
     */
    long getInsertedBytes();

    /**
     * Total number of failed records.
     * Implemented as counter and is reset on restart.
     *
     * @return counter value
     */
    long getFailedRecords();


    /**
     * Total number of received record batches by a Sink Task instance.
     * Implemented as counter and is reset on restart.
     *
     * @return counter value
     */
    long getReceivedBatches();

    /**
     * Mean receive lag calculated as receive by task time - record timestamp.
     * Calculated using a first record timestamp in the batch.
     * Implemented as exponential moving average. Reset on restart.
     *
     * @return counter value
     */
    long getMeanReceiveLag();
}
