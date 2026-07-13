package com.clickhouse.kafka.connect.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * At-least-once buffering. Single flat buffer across all partitions, flushed as
 * a whole when the total record count reaches {@code bufferCount} or the time
 * trigger fires. Offsets are committed only for flushed data, so buffered
 * records are redelivered on crash/rebalance. Preserves the semantics shipped in
 * PR #658. See the "Internal Buffering" section of docs/DESIGN.md.
 */
final class AtLeastOnceBufferStrategy implements DeliveryStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(AtLeastOnceBufferStrategy.class);

    private final ChunkFlusher flusher;
    private final int bufferCount;
    private final long bufferFlushTime;
    private final List<SinkRecord> buffer;
    private long lastFlushTime;

    AtLeastOnceBufferStrategy(ChunkFlusher flusher, int bufferCount, long bufferFlushTime) {
        this.flusher = flusher;
        this.bufferCount = bufferCount;
        this.bufferFlushTime = bufferFlushTime;
        this.buffer = new ArrayList<>(bufferCount);
        this.lastFlushTime = System.currentTimeMillis();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records != null && !records.isEmpty()) {
            buffer.addAll(records);
            LOGGER.debug("Buffered {} records, total buffer size: {}", records.size(), buffer.size());
        }

        boolean sizeThreshold = buffer.size() >= bufferCount;
        boolean timeThreshold = bufferFlushTime > 0
                && (System.currentTimeMillis() - lastFlushTime) >= bufferFlushTime
                && !buffer.isEmpty();

        if (sizeThreshold || timeThreshold) {
            LOGGER.debug("Buffer flush triggered: size={}, sizeThreshold={}, timeThreshold={}, lastFlushTime={}",
                    buffer.size(), sizeThreshold, timeThreshold, lastFlushTime);
            flushBuffer();
        }
    }

    private void flushBuffer() {
        if (buffer.isEmpty()) {
            return;
        }
        flusher.flush(buffer);
        buffer.clear();
        lastFlushTime = System.currentTimeMillis();
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        return flusher.drainFlushedOffsets();
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        // Remove buffered records for revoked partitions to prevent duplicates.
        // Their offsets were never committed, so the new owner redelivers them.
        if (!buffer.isEmpty()) {
            int before = buffer.size();
            buffer.removeIf(record -> partitions.contains(new TopicPartition(record.topic(), record.kafkaPartition())));
            int dropped = before - buffer.size();
            if (dropped > 0) {
                LOGGER.info("Rebalance: removed {} buffered records for revoked partitions {}", dropped, partitions);
            }
        }
        flusher.forgetPartitions(partitions);
    }

    @Override
    public void stop() {
        int remaining = buffer.size();
        buffer.clear();
        if (remaining > 0) {
            LOGGER.warn("Stop called with {} buffered records still in buffer — " +
                    "these will be redelivered on restart since offsets were not committed", remaining);
        }
    }
}
