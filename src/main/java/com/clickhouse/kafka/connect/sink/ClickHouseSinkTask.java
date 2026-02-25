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
import java.util.List;
import java.util.Map;

public class ClickHouseSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkTask.class);

    private ProxySinkTask proxySinkTask;
    private ClickHouseSinkConfig clickHouseSinkConfig;
    private ErrorReporter errorReporter;

    // Internal buffering
    private List<SinkRecord> buffer;
    private long lastFlushTime;
    private int bufferCount;
    private long bufferFlushTime;
    private boolean bufferingEnabled;
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

        // Validate config before heavy initialization
        int configBufferCount = clickHouseSinkConfig.getBufferCount();
        if (configBufferCount > 0 && clickHouseSinkConfig.isExactlyOnce()) {
            throw new ConnectException(
                    "Internal buffering (bufferCount > 0) is not compatible with exactly-once mode. " +
                    "Buffering changes batch boundaries, which breaks ClickHouse block deduplication and the offset state machine. " +
                    "To resolve this, either disable exactly-once mode by setting 'exactlyOnce=false' in your connector config, " +
                    "or disable buffering by setting 'bufferCount=0'."
            );
        }

        this.proxySinkTask = new ProxySinkTask(clickHouseSinkConfig, errorReporter);

        // Initialize buffering
        this.bufferCount = configBufferCount;
        this.bufferFlushTime = clickHouseSinkConfig.getBufferFlushTime();
        this.bufferingEnabled = this.bufferCount > 0;
        this.buffer = this.bufferingEnabled ? new ArrayList<>(this.bufferCount) : new ArrayList<>();
        this.lastFlushTime = System.currentTimeMillis();
        this.flushedOffsets = new HashMap<>();

        if (this.bufferFlushTime > 0 && this.bufferCount == 0) {
            LOGGER.warn("bufferFlushTime is set but will be ignored because bufferCount is 0");
        }
    }


    @Override
    public void put(Collection<SinkRecord> records) {
        if (!bufferingEnabled) {
            // Original behavior - no buffering
            putDirect(records);
            return;
        }

        // Buffering mode: accumulate records
        if (!records.isEmpty()) {
            buffer.addAll(records);
            LOGGER.debug("Buffered {} records, total buffer size: {}", records.size(), buffer.size());
        }

        // Check if we should flush
        boolean sizeThreshold = buffer.size() >= bufferCount;
        boolean timeThreshold = bufferFlushTime > 0
                && (System.currentTimeMillis() - lastFlushTime) >= bufferFlushTime
                && !buffer.isEmpty();

        if (sizeThreshold || timeThreshold) {
            LOGGER.info("Buffer flush triggered: size={}, sizeThreshold={}, timeThreshold={}", buffer.size(), sizeThreshold, timeThreshold);
            flushBuffer();
        }
    }

    private void flushBuffer() {
        if (buffer.isEmpty()) {
            return;
        }
        List<SinkRecord> toFlush = new ArrayList<>(buffer);
        putDirect(toFlush);
        // Track the max flushed offset per topic/partition so preCommit() only
        // allows the framework to commit offsets for data written to ClickHouse
        for (SinkRecord record : toFlush) {
            TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            long offset = record.kafkaOffset() + 1; // +1 because committed offset = next offset to consume
            OffsetAndMetadata current = flushedOffsets.get(tp);
            if (current == null || offset > current.offset()) {
                flushedOffsets.put(tp, new OffsetAndMetadata(offset));
            }
        }
        buffer.clear();
        lastFlushTime = System.currentTimeMillis();
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
            return currentOffsets;
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
        LOGGER.info("preCommit: committing offsets for flushed data: {}", offsetsToCommit);
        return offsetsToCommit;
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        if (!bufferingEnabled) {
            return;
        }
        // Remove buffered records for revoked partitions to prevent duplicates.
        // Their offsets were never committed via preCommit(), so the new owner
        // will redeliver them — no data loss.
        if (!buffer.isEmpty()) {
            int before = buffer.size();
            buffer.removeIf(record -> {
                TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
                return partitions.contains(tp);
            });
            if (buffer.size() != before) {
                LOGGER.info("Rebalance: removed {} buffered records for revoked partitions {}",
                        before - buffer.size(), partitions);
            }
        }
        // Always clean up flushed offsets for revoked partitions so preCommit()
        // doesn't return offsets that this task no longer owns.
        flushedOffsets.keySet().removeAll(partitions);
    }

    @Override
    public void stop() {
        // Note: close() is called before stop() by the framework, which already
        // removes buffered records. Unflushed records' offsets were never committed
        // via preCommit(), so Kafka will redeliver them on restart (at-least-once).
        if (bufferingEnabled && buffer != null && !buffer.isEmpty()) {
            LOGGER.warn("Stop called with {} buffered records still in buffer — " +
                    "these will be redelivered on restart since offsets were not committed", buffer.size());
            buffer.clear();
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
