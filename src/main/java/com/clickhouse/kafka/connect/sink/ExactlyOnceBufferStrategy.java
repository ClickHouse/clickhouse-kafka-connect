package com.clickhouse.kafka.connect.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Exactly-once buffering ("strict chunking"). Records are bucketed per partition
 * and drained in fixed {@code bufferCount}-sized chunks. A bucket below the
 * threshold stays buffered across {@code put()} calls until a later call pushes
 * it over {@code bufferCount}. Each flush has a fixed record count, so
 * {@code (minOffset, maxOffset)} is reproducible across retries — required for
 * ClickHouse {@code insert_deduplication_token} reuse and the state-machine
 * range comparison. See the "Internal Buffering" section of docs/DESIGN.md.
 */
final class ExactlyOnceBufferStrategy implements DeliveryStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExactlyOnceBufferStrategy.class);

    private final ChunkFlusher flusher;
    private final int bufferCount;
    private final Map<TopicPartition, List<SinkRecord>> perPartitionBuffer = new LinkedHashMap<>();

    ExactlyOnceBufferStrategy(ChunkFlusher flusher, int bufferCount) {
        this.flusher = flusher;
        this.bufferCount = bufferCount;
        LOGGER.info("Buffering in strict-chunking mode (exactlyOnce=true): per-partition flush " +
                "in fixed bufferCount={} chunks, time trigger disabled.", bufferCount);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records != null && !records.isEmpty()) {
            for (SinkRecord r : records) {
                TopicPartition tp = new TopicPartition(r.topic(), r.kafkaPartition());
                perPartitionBuffer.computeIfAbsent(tp, k -> new ArrayList<>()).add(r);
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Buffered {} records across {} partitions (strict)",
                        records.size(), perPartitionBuffer.size());
            }
        }

        // Flush only full bufferCount-sized chunks. The trailing tail
        // (bucket.size() % bufferCount records) stays buffered for a later put().
        for (List<SinkRecord> bucket : perPartitionBuffer.values()) {
            while (bucket.size() >= bufferCount) {
                List<SinkRecord> chunk = bucket.subList(0, bufferCount);
                flusher.flush(chunk);
                chunk.clear(); // truncates the flushed head from the backing bucket
            }
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        return flusher.drainFlushedOffsets();
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        // Remove buffered records for revoked partitions. Their offsets were never
        // committed, so the new owner redelivers them — no data loss.
        int dropped = 0;
        for (TopicPartition tp : partitions) {
            List<SinkRecord> bucket = perPartitionBuffer.remove(tp);
            if (bucket != null) {
                dropped += bucket.size();
            }
        }
        if (dropped > 0) {
            LOGGER.info("Rebalance: removed {} buffered records for revoked partitions {}", dropped, partitions);
        }
        flusher.forgetPartitions(partitions);
    }

    @Override
    public void stop() {
        int remaining = 0;
        for (List<SinkRecord> bucket : perPartitionBuffer.values()) {
            remaining += bucket.size();
        }
        perPartitionBuffer.clear();
        if (remaining > 0) {
            LOGGER.warn("Stop called with {} buffered records still in buffer — " +
                    "these will be redelivered on restart since offsets were not committed", remaining);
        }
    }
}
