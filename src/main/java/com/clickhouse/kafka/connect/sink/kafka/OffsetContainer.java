package com.clickhouse.kafka.connect.sink.kafka;

public class OffsetContainer extends TopicPartitionContanier {
    private long offset;


    public OffsetContainer(String topic, int partition, long offset) {
        super(topic, partition);
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }
}
