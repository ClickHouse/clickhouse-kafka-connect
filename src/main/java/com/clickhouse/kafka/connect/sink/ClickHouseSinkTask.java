package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class ClickHouseSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkTask.class);

    private ProxySinkTask proxySinkTask;
    private ClickHouseSinkConfig clickHouseSinkConfig;
    private ErrorReporter errorReporter;

    // One of the three delivery semantics, chosen at start() from the config.
    // See "Internal Buffering" section of docs/DESIGN.md.
    private DeliveryStrategy deliveryStrategy;

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

        this.deliveryStrategy = createDeliveryStrategy(clickHouseSinkConfig, proxySinkTask, errorReporter);
    }

    /**
     * Picks the delivery strategy from the config flag matrix: direct (no
     * buffer), at-least-once buffered, or exactly-once buffered (strict
     * chunking).
     */
    private static DeliveryStrategy createDeliveryStrategy(ClickHouseSinkConfig cfg,
                                                           ProxySinkTask proxySinkTask,
                                                           ErrorReporter errorReporter) {
        if (cfg.getBufferFlushTime() > 0 && cfg.getBufferCount() == 0) {
            LOGGER.warn("bufferFlushTime is set but will be ignored because bufferCount is 0");
        }
        ChunkFlusher flusher = new ChunkFlusher(proxySinkTask, cfg, errorReporter);
        if (!isBufferingEnabled(cfg)) {
            return new DirectDeliveryStrategy(proxySinkTask, cfg, flusher);
        }
        if (isExactlyOnceAndBufferingEnabled(cfg)) {
            return new ExactlyOnceBufferStrategy(flusher, cfg.getBufferCount());
        }
        return new AtLeastOnceBufferStrategy(flusher, cfg.getBufferCount(), cfg.getBufferFlushTime());
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
        deliveryStrategy.put(records);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // Buffered strategies commit offsets only for data actually written to
        // ClickHouse — records still buffered are redelivered on crash/rebalance
        // (at-least-once). Pattern follows the Confluent S3 Sink Connector:
        // flush() is a no-op, preCommit() manages offsets.
        return deliveryStrategy.preCommit(currentOffsets);
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        LOGGER.debug("close: {}", partitions);
        if (proxySinkTask != null) {
            proxySinkTask.onPartitionRemoved(partitions);
        }
        if (deliveryStrategy != null) {
            deliveryStrategy.close(partitions);
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        close(partitions); // call newer method in case we are running in older runtime
    }

    @Override
    public void stop() {
        // close() is called before stop() by the framework, which already removes
        // buffered records for revoked partitions. Any remaining buffered records'
        // offsets were never committed via preCommit(), so Kafka redelivers them on
        // restart (at-least-once).
        if (deliveryStrategy != null) {
            deliveryStrategy.stop();
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
