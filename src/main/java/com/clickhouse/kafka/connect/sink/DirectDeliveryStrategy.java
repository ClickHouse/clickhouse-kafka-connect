package com.clickhouse.kafka.connect.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * No buffering: each {@code put()} is inserted immediately. Offset reconciliation
 * is delegated to the Connect framework / {@code ProxySinkTask} insert tracking,
 * matching the connector's original (pre-buffer) behavior.
 */
final class DirectDeliveryStrategy implements DeliveryStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectDeliveryStrategy.class);

    private final ProxySinkTask proxySinkTask;
    private final ClickHouseSinkConfig clickHouseSinkConfig;
    private final ChunkFlusher flusher;

    DirectDeliveryStrategy(ProxySinkTask proxySinkTask, ClickHouseSinkConfig clickHouseSinkConfig, ChunkFlusher flusher) {
        this.proxySinkTask = proxySinkTask;
        this.clickHouseSinkConfig = clickHouseSinkConfig;
        this.flusher = flusher;
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        flusher.putDirect(records);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        LOGGER.debug("preCommit: {}", currentOffsets);
        if (!clickHouseSinkConfig.isExactlyOnce() && clickHouseSinkConfig.isIgnorePartitionsWhenBatching()) {
            // link to com.clickhouse.kafka.connect.sink.processing.Processing.doLogic
            LOGGER.debug("preCommit: returning currentOffsets back");
            return currentOffsets; // there is another way to reconcile data
        }
        if (!clickHouseSinkConfig.isReportInsertedOffsets()) {
            LOGGER.debug("preCommit: reportInsertedOffsets=false, returning currentOffsets");
            return currentOffsets;
        }
        Map<TopicPartition, OffsetAndMetadata> inserted = proxySinkTask.getInsertedOffsetsSnapshot();
        if (inserted.keySet().removeIf(key -> !currentOffsets.containsKey(key))) {
            LOGGER.debug("preCommit: inserted offsets doesn't match currentOffsets. This is ok - seems result of rebalance.");
        }
        LOGGER.debug("preCommit: returned {}", inserted);
        return inserted;
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        // No buffered state to clean up.
    }

    @Override
    public void stop() {
        // No buffered state to discard.
    }
}
