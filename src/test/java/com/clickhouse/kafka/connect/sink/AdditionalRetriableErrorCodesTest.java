package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.ServerException;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.InMemoryDBWriter;
import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import com.clickhouse.kafka.connect.util.QueryIdentifier;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdditionalRetriableErrorCodesTest {

    private static final String TOPIC = "additional_retriable_error_codes";

    /** Fails the first insert with the given server error code, then behaves like {@link InMemoryDBWriter}. */
    private static final class FailOnceDBWriter extends InMemoryDBWriter {
        private final int errorCode;
        private boolean failed;

        FailOnceDBWriter(int errorCode) {
            this.errorCode = errorCode;
        }

        @Override
        public void doInsert(List<Record> records, QueryIdentifier queryId, ErrorReporter errorReporter) {
            if (!failed) {
                failed = true;
                throw new ServerException(errorCode, "Query with id = " + queryId.getQueryId() + " is already running", 500);
            }
            super.doInsert(records, queryId, errorReporter);
        }
    }

    private static ClickHouseSinkConfig config(String additionalRetriableErrorCodes) {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConfig.ADDITIONAL_RETRIABLE_ERROR_CODES, additionalRetriableErrorCodes);
        return new ClickHouseSinkConfig(props);
    }

    @Test
    @DisplayName("Configured code escapes ProxySinkTask and ChunkFlusher as RetriableException and succeeds on redelivery")
    public void configuredCodeIsRedelivered() {
        ClickHouseSinkConfig config = config("216");
        FailOnceDBWriter writer = new FailOnceDBWriter(216);
        ProxySinkTask proxySinkTask = new ProxySinkTask(config, null, writer);
        ChunkFlusher flusher = new ChunkFlusher(proxySinkTask, config, null);
        List<SinkRecord> records = SchemalessTestData.createPrimitiveTypes(TOPIC, 0, 10);

        assertThrows(RetriableException.class, () -> flusher.putDirect(records));
        assertEquals(0, writer.recordsInserted());

        flusher.flush(records);
        assertEquals(records.size(), writer.recordsInserted());
        Map<TopicPartition, OffsetAndMetadata> offsets = flusher.drainFlushedOffsets();
        assertEquals(records.size(), offsets.get(new TopicPartition(TOPIC, 0)).offset());
    }

    @Test
    @DisplayName("Buffered flush after a configured code leaves no offsets to commit")
    public void configuredCodeLeavesOffsetsUncommitted() {
        ClickHouseSinkConfig config = config("216");
        ProxySinkTask proxySinkTask = new ProxySinkTask(config, null, new FailOnceDBWriter(216));
        ChunkFlusher flusher = new ChunkFlusher(proxySinkTask, config, null);
        List<SinkRecord> records = SchemalessTestData.createPrimitiveTypes(TOPIC, 0, 10);

        assertThrows(RetriableException.class, () -> flusher.flush(records));
        assertTrue(flusher.drainFlushedOffsets().isEmpty());
    }

    @Test
    @DisplayName("Unconfigured code still fails the task")
    public void unconfiguredCodeFailsTask() {
        ClickHouseSinkConfig config = config("");
        ProxySinkTask proxySinkTask = new ProxySinkTask(config, null, new FailOnceDBWriter(216));
        ChunkFlusher flusher = new ChunkFlusher(proxySinkTask, config, null);
        List<SinkRecord> records = SchemalessTestData.createPrimitiveTypes(TOPIC, 0, 10);

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> flusher.putDirect(records));
        assertFalse(thrown instanceof RetriableException);
    }
}
