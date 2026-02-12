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

        this.proxySinkTask = new ProxySinkTask(clickHouseSinkConfig, errorReporter);

        // Initialize buffering
        this.bufferCount = clickHouseSinkConfig.getBufferCount();
        this.bufferFlushTime = clickHouseSinkConfig.getBufferFlushTime();
        this.bufferingEnabled = this.bufferCount > 0;
        this.buffer = new ArrayList<>();
        this.lastFlushTime = System.currentTimeMillis();

        if (bufferingEnabled) {
            LOGGER.info("Internal buffering enabled: bufferCount={}, bufferFlushTime={}ms",
                    bufferCount, bufferFlushTime);
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
        // Clear buffer only after successful flush to prevent data loss on failure
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

    // TODO: can be removed ss
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        LOGGER.trace("Test");
    }

    @Override
    public void stop() {
        // Flush remaining buffered records before stopping
        if (bufferingEnabled && buffer != null && !buffer.isEmpty()) {
            LOGGER.info("Stop called with {} buffered records - flushing", buffer.size());
            flushBuffer();
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
