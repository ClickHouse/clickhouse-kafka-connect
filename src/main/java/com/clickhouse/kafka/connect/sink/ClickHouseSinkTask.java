package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import com.clickhouse.kafka.connect.util.Utils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ClickHouseSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkTask.class);

    private ProxySinkTask proxySinkTask;
    private ClickHouseSinkConfig clickHouseSinkConfig;
    private ErrorReporter errorReporter;

    // Internal buffering. See "Internal Buffering" section of docs/DESIGN.md.
    private List<SinkRecord> buffer;
    private Map<TopicPartition, List<SinkRecord>> perPartitionBuffer;
    private long lastFlushTime;
    private int bufferCount;
    private long bufferFlushTime;
    private boolean bufferingEnabled;
    private boolean exactlyOnceAndBufferingEnabled;
    private Map<TopicPartition, OffsetAndMetadata> flushedOffsets;

    @Override
    public String version() {
        return "0.0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("Start SinkTask: ");
        try {
            clickHouseSinkConfig = new ClickHouseSinkConfig(props);
            if (errorReporter == null) {
                errorReporter = createErrorReporter();
            }
        } catch (Exception e) {
            throw new ConnectException("Failed to start new task" , e);
        }

        validateBufferConfig(clickHouseSinkConfig);

        this.proxySinkTask = new ProxySinkTask(clickHouseSinkConfig, errorReporter);
        this.proxySinkTask.start();

        this.bufferCount = clickHouseSinkConfig.getBufferCount();
        this.bufferFlushTime = clickHouseSinkConfig.getBufferFlushTime();
        this.bufferingEnabled = isBufferingEnabled(clickHouseSinkConfig);
        this.exactlyOnceAndBufferingEnabled = isExactlyOnceAndBufferingEnabled(clickHouseSinkConfig);
        this.buffer = (this.bufferingEnabled && !this.exactlyOnceAndBufferingEnabled)
                ? new ArrayList<>(this.bufferCount)
                : new ArrayList<>();
        this.perPartitionBuffer = new LinkedHashMap<>();
        this.lastFlushTime = System.currentTimeMillis();
        this.flushedOffsets = new HashMap<>();

        if (this.bufferFlushTime > 0 && this.bufferCount == 0) {
            LOGGER.warn("bufferFlushTime is set but will be ignored because bufferCount is 0");
        }
        if (this.exactlyOnceAndBufferingEnabled) {
            LOGGER.info("Buffering in strict-chunking mode (exactlyOnce=true): per-partition flush " +
                    "in fixed bufferCount={} chunks, time trigger disabled.", this.bufferCount);
        }
    }

    /**
     * @return {@code true} when the connector should use the internal record buffer
     *         to coalesce multiple {@code put()} calls into larger ClickHouse inserts.
     *         Activated by {@code bufferCount > 0}.
     */
    static boolean isBufferingEnabled(ClickHouseSinkConfig cfg) {
        return cfg.getBufferCount() > 0;
    }

    /**
     * @return {@code true} when buffering must enforce per-partition,
     *         fixed {@code bufferCount}-sized flush boundaries because the user
     *         opted into exactly-once delivery alongside buffering.
     *         Combination of {@link #isBufferingEnabled} and
     *         {@link ClickHouseSinkConfig#isExactlyOnce}.
     *
     * <p>If the public flag matrix is later replaced by a single {@code deliveryMode}
     * enum, this predicate collapses into {@code mode == EXACTLY_ONCE_BUFFERED}.
     */
    static boolean isExactlyOnceAndBufferingEnabled(ClickHouseSinkConfig cfg) {
        return isBufferingEnabled(cfg) && cfg.isExactlyOnce();
    }

    /**
     * Validates buffer + exactly-once compatibility. EO with buffering is supported
     * only under strict-chunking constraints that preserve batch-boundary determinism.
     * Throws {@link ConnectException} if the connector is configured in a combination
     * that would silently break dedup-token reuse on retry.
     */
    static void validateBufferConfig(ClickHouseSinkConfig cfg) {
        if (cfg.getBufferCount() <= 0 || !cfg.isExactlyOnce()) {
            return;
        }
        if (cfg.getBufferFlushTime() > 0) {
            throw new ConnectException(
                    "Buffering with exactlyOnce=true requires bufferFlushTime=0. " +
                    "Time-based flush triggers break batch-boundary determinism required for " +
                    "ClickHouse insert_deduplication_token reuse across retries."
            );
        }
        if (cfg.isIgnorePartitionsWhenBatching()) {
            throw new ConnectException(
                    "Buffering with exactlyOnce=true requires ignorePartitionsWhenBatching=false. " +
                    "When partitions are ignored, the dedup token has no partition component (null) " +
                    "and offset-range deduplication cannot fire."
            );
        }
    }


    @Override
    public void put(Collection<SinkRecord> records) {
        if (!bufferingEnabled) {
            // Original behavior - no buffering
            putDirect(records);
            return;
        }

        if (exactlyOnceAndBufferingEnabled) {
            putBufferExactlyOnce(records);
        } else {
            putBufferAtLeastOnce(records);
        }
    }

    /**
     * At-least-once buffering. Single flat buffer, threshold on total size,
     * whole-buffer flush. Preserves the semantics shipped in PR #658.
     */
    private void putBufferAtLeastOnce(Collection<SinkRecord> records) {
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

    /**
     * Exactly-once buffering. Buckets per partition, drains in fixed
     * bufferCount-sized chunks. Tail records below threshold remain buffered
     * across put() calls until a subsequent call pushes the bucket over
     * bufferCount. Each flush has a fixed record count, so (minOffset, maxOffset)
     * is reproducible across retries — required for ClickHouse
     * insert_deduplication_token reuse and StateProvider range comparison.
     */
    private void putBufferExactlyOnce(Collection<SinkRecord> records) {
        if (records != null && !records.isEmpty()) {
            for (SinkRecord r : records) {
                TopicPartition tp = new TopicPartition(r.topic(), r.kafkaPartition());
                perPartitionBuffer.computeIfAbsent(tp, k -> new ArrayList<>(bufferCount)).add(r);
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Buffered {} records across {} partitions (strict)",
                        records.size(), perPartitionBuffer.size());
            }
        }

        for (List<SinkRecord> bucket : perPartitionBuffer.values()) {
            while (bucket.size() >= bufferCount) {
                List<SinkRecord> head = bucket.subList(0, bufferCount);
                flushChunk(head);
                head.clear();
            }
        }
        lastFlushTime = System.currentTimeMillis();
    }

    private void flushBuffer() {
        if (buffer.isEmpty()) {
            return;
        }
        flushChunk(buffer);
        buffer.clear();
        lastFlushTime = System.currentTimeMillis();
    }

    /**
     * Pushes a single chunk to ClickHouse and records the highest offset per
     * partition in flushedOffsets so preCommit() only commits offsets for data
     * actually written.
     *
     * <p>Failure semantics:
     * <ul>
     *   <li>If {@code putDirect()} throws (no error tolerance), the exception
     *       propagates. The chunk records have already been removed from their
     *       per-partition bucket by the caller, but {@code flushedOffsets} is
     *       NOT updated for this chunk because the offset-tracking loop below
     *       runs only on the success path. {@code preCommit()} therefore returns
     *       no offset for these records, Kafka's consumer-group offset does not
     *       advance, and on restart Kafka redelivers the same offsets —
     *       at-least-once is preserved. In exactly-once mode, the dedup token
     *       plus {@code StateProvider} absorb the redelivery: the second
     *       insert either no-ops via the token or hits the
     *       {@code AFTER_PROCESSING} branch of the state machine.</li>
     *   <li>If {@code putDirect()} routes records to the DLQ (error tolerance
     *       enabled), the chunk is considered handled; the offset-tracking
     *       loop runs and the offset advances on the next {@code preCommit()}.</li>
     *   <li>The chunk is removed from the buffer <em>before</em> insert. If the
     *       JVM crashes between {@code clear()} and {@code putDirect()}, the
     *       records are lost from the buffer but never reached ClickHouse, and
     *       the Kafka offset was never committed — so on restart Kafka
     *       redelivers and the buffer is rebuilt from the same offsets.</li>
     * </ul>
     */
    private void flushChunk(List<SinkRecord> chunk) {
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

    private void putDirect(Collection<SinkRecord> records) {
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

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        if (!bufferingEnabled) {
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
        // Only commit offsets for records that have been successfully written to ClickHouse.
        // Records still in the buffer have NOT been written, so their offsets must not be committed.
        // This guarantees at-least-once delivery: on crash/rebalance, Kafka redelivers buffered records.
        // Pattern follows Confluent S3 Sink Connector: flush() is no-op, preCommit() manages offsets.
        if (flushedOffsets.isEmpty()) {
            LOGGER.info("preCommit: no offsets to commit (all records still buffered)");
            return new HashMap<>();
        }
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>(flushedOffsets);
        flushedOffsets.clear();
        LOGGER.debug("preCommit: committing offsets for flushed data: {}", offsetsToCommit);
        return offsetsToCommit;
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        LOGGER.debug("close: {}", partitions);
        if (proxySinkTask != null) {
            proxySinkTask.onPartitionRemoved(partitions);
        }
        if (!bufferingEnabled) {
            return;
        }
        // Remove buffered records for revoked partitions to prevent duplicates.
        // Their offsets were never committed via preCommit(), so the new owner
        // will redeliver them — no data loss.
        int dropped = 0;
        if (exactlyOnceAndBufferingEnabled) {
            for (TopicPartition tp : partitions) {
                List<SinkRecord> bucket = perPartitionBuffer.remove(tp);
                if (bucket != null) {
                    dropped += bucket.size();
                }
            }
        } else if (!buffer.isEmpty()) {
            int before = buffer.size();
            buffer.removeIf(record -> {
                TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
                return partitions.contains(tp);
            });
            dropped = before - buffer.size();
        }
        if (dropped > 0) {
            LOGGER.info("Rebalance: removed {} buffered records for revoked partitions {}",
                    dropped, partitions);
        }
        // Always clean up flushed offsets for revoked partitions so preCommit()
        // doesn't return offsets that this task no longer owns.
        flushedOffsets.keySet().removeAll(partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        close(partitions); // call newer method in case we are running in older runtime
    }

    @Override
    public void stop() {
        // Note: close() is called before stop() by the framework, which already
        // removes buffered records. Unflushed records' offsets were never committed
        // via preCommit(), so Kafka will redeliver them on restart (at-least-once).
        if (bufferingEnabled) {
            int remaining = 0;
            if (exactlyOnceAndBufferingEnabled) {
                for (List<SinkRecord> bucket : perPartitionBuffer.values()) {
                    remaining += bucket.size();
                }
                perPartitionBuffer.clear();
            } else {
                remaining = buffer.size();
                buffer.clear();
            }
            if (remaining > 0) {
                LOGGER.warn("Stop called with {} buffered records still in buffer — " +
                        "these will be redelivered on restart since offsets were not committed", remaining);
            }
        }
        if (this.proxySinkTask != null) {
            this.proxySinkTask.stop();
        }
    }

    /**
     * Should be run before start
     * @param errorReporter
     */
    public void setErrorReporter(ErrorReporter errorReporter) {
        this.errorReporter = errorReporter;
    }


    private ErrorReporter createErrorReporter() {
        ErrorReporter result = devNullErrorReporter();
        if (context != null) {
            try {
                ErrantRecordReporter errantRecordReporter = context.errantRecordReporter();
                if (errantRecordReporter != null) {
                    result = errantRecordReporter::report;
                } else {
                    LOGGER.info("Errant record reporter not configured.");
                }
            } catch (NoClassDefFoundError | NoSuchMethodError e) {
                // Will occur in Connect runtimes earlier than 2.6
                LOGGER.info("Kafka versions prior to 2.6 do not support the errant record reporter.");
            }
        }
        return result;
    }

    static ErrorReporter devNullErrorReporter() {
        return (record, e) -> {
        };
    }

    public int taskId() {
        return this.proxySinkTask == null ? Integer.MAX_VALUE : this.proxySinkTask.getId();
    }
}
