package com.clickhouse.kafka.connect.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryIdentifier {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryIdentifier.class);
    private final String topic;
    private final int partition;
    private final long minOffset;
    private final long maxOffset;
    private final String queryId;

    public QueryIdentifier(String topic, int partition, long minOffset, long maxOffset, String queryId) {
        this.topic = topic;
        this.partition = partition;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.queryId = queryId;
    }

    public String toString() {
        return String.format("Topic: [%s], Partition: [%s], MinOffset: [%s], MaxOffset: [%s], (QueryId: [%s])",
                topic, partition, minOffset, maxOffset, queryId);
    }

    public String getQueryId() {
        return queryId;
    }
    public String getTopic() {
        return topic;
    }
    public int getPartition() {
        return partition;
    }
    public long getMinOffset() {
        return minOffset;
    }
    public long getMaxOffset() {
        return maxOffset;
    }

    public String getDeduplicationToken() {
        return String.format("%s-%s-%s-%s", topic, partition, minOffset, maxOffset);
    }
}
