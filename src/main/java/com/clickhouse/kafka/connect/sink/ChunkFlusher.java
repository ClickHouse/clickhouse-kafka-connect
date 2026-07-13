package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import com.clickhouse.kafka.connect.util.Utils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared insert path for the buffered delivery strategies. Pushes a chunk to
 * ClickHouse via {@link ProxySinkTask} and tracks the highest committed offset
 * per partition so {@code preCommit()} only advances offsets for data actually
 * written.
 *
 * <p>Failure semantics: if the insert throws (no error tolerance) the exception
 * propagates and {@code flushedOffsets} is left untouched for that chunk, so the
 * Kafka offset does not advance and the records are redelivered on restart
 * (at-least-once). In exactly-once mode the dedup token plus the state machine
 * absorb the redelivery. If records are routed to the DLQ (error tolerance on),
 * the chunk is considered handled and the offset advances.
 */
final class ChunkFlusher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkFlusher.class);

    private final ProxySinkTask proxySinkTask;
    private final ClickHouseSinkConfig clickHouseSinkConfig;
    private final ErrorReporter errorReporter;
    private final Map<TopicPartition, OffsetAndMetadata> flushedOffsets = new HashMap<>();

    ChunkFlusher(ProxySinkTask proxySinkTask, ClickHouseSinkConfig clickHouseSinkConfig, ErrorReporter errorReporter) {
        this.proxySinkTask = proxySinkTask;
        this.clickHouseSinkConfig = clickHouseSinkConfig;
        this.errorReporter = errorReporter;
    }

    /** Inserts a chunk and records the next offset to consume per partition on success. */
    void flush(List<SinkRecord> chunk) {
        if (chunk.isEmpty()) {
            return;
        }
        putDirect(chunk);
        for (SinkRecord record : chunk) {
            TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            long offset = record.kafkaOffset() + 1; // committed offset = next offset to consume
            OffsetAndMetadata current = flushedOffsets.get(tp);
            if (current == null || offset > current.offset()) {
                flushedOffsets.put(tp, new OffsetAndMetadata(offset));
            }
        }
    }

    void putDirect(Collection<SinkRecord> records) {
        try {
            long putStat = System.currentTimeMillis();
            this.proxySinkTask.put(records);
            long putEnd = System.currentTimeMillis();
            if (!records.isEmpty()) {
                LOGGER.info("Put records: {} in {} ms", records.size(), putEnd - putStat);
            }
        } catch (Exception e) {
            LOGGER.trace("Passing the exception to the exception handler.");
            boolean errorTolerance = clickHouseSinkConfig != null && clickHouseSinkConfig.isErrorsTolerance();
            Utils.handleException(e, errorTolerance, records);
            if (errorTolerance && errorReporter != null) {
                LOGGER.warn("Sending [{}] records to DLQ for exception: {}", records.size(), e.getLocalizedMessage());
                records.forEach(r -> Utils.sendTODlq(errorReporter, r, e));
            }
        }
    }

    /**
     * Returns and clears the offsets for data flushed since the last call. Only
     * offsets for records actually written to ClickHouse are returned, so
     * buffered-but-unwritten records are never committed (at-least-once).
     */
    Map<TopicPartition, OffsetAndMetadata> drainFlushedOffsets() {
        if (flushedOffsets.isEmpty()) {
            LOGGER.info("preCommit: no offsets to commit (all records still buffered)");
            return new HashMap<>();
        }
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>(flushedOffsets);
        flushedOffsets.clear();
        LOGGER.debug("preCommit: committing offsets for flushed data: {}", offsetsToCommit);
        return offsetsToCommit;
    }

    /** Drop flushed-offset tracking for revoked partitions. */
    void forgetPartitions(Collection<TopicPartition> partitions) {
        flushedOffsets.keySet().removeAll(partitions);
    }
}
