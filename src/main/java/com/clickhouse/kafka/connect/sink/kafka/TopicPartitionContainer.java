package com.clickhouse.kafka.connect.sink.kafka;

public class TopicPartitionContainer {

    protected String topic;
    protected int partition;

    public TopicPartitionContainer(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public String getTopicAndPartitionKey() {
        return String.format("%s-%d", topic, partition);
    }
}
