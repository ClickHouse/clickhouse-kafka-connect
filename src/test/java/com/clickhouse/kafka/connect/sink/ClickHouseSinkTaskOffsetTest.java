package com.clickhouse.kafka.connect.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class ClickHouseSinkTaskOffsetTest {

    @Test
    void preCommitUsesOriginalMetadataAfterTopicTransform() throws Exception {
        ClickHouseSinkTask task = bufferingTask(1);
        SinkRecord record = transformedRecord("destination-table", 4, 900, "source-topic", 2, 41);

        task.put(List.of(record));
        Map<TopicPartition, OffsetAndMetadata> offsets = task.preCommit(Map.of());

        assertEquals(1, offsets.size());
        assertEquals(42, offsets.get(new TopicPartition("source-topic", 2)).offset());
    }

    @Test
    void closeRemovesTransformedRecordsByOriginalPartition() throws Exception {
        ClickHouseSinkTask task = bufferingTask(10);
        SinkRecord record = transformedRecord("destination-table", 4, 900, "source-topic", 2, 41);
        task.put(List.of(record));

        task.close(List.of(new TopicPartition("source-topic", 2)));

        assertTrue(buffer(task).isEmpty());
    }

    @Test
    void preCommitFallsBackToCurrentMetadataOnOlderKafkaRuntimes() throws Exception {
        ClickHouseSinkTask task = bufferingTask(1);
        SinkRecord record = new SinkRecord("source-topic", 2, null, null, null, null, 41);

        task.put(List.of(record));
        Map<TopicPartition, OffsetAndMetadata> offsets = task.preCommit(Map.of());

        assertEquals(42, offsets.get(new TopicPartition("source-topic", 2)).offset());
    }

    private static ClickHouseSinkTask bufferingTask(int bufferCount) throws Exception {
        ClickHouseSinkTask task = new ClickHouseSinkTask();
        setField(task, "proxySinkTask", mock(ProxySinkTask.class));
        setField(task, "bufferingEnabled", true);
        setField(task, "bufferCount", bufferCount);
        setField(task, "bufferFlushTime", 0L);
        setField(task, "buffer", new ArrayList<SinkRecord>());
        setField(task, "flushedOffsets", new HashMap<TopicPartition, OffsetAndMetadata>());
        setField(task, "lastFlushTime", System.currentTimeMillis());
        return task;
    }

    private static SinkRecord transformedRecord(
            String topic,
            int partition,
            long offset,
            String originalTopic,
            int originalPartition,
            long originalOffset) {
        return new SinkRecordWithOriginalMetadata(
                topic, partition, offset, originalTopic, originalPartition, originalOffset);
    }

    @SuppressWarnings("unchecked")
    private static Collection<SinkRecord> buffer(ClickHouseSinkTask task) throws Exception {
        Field field = ClickHouseSinkTask.class.getDeclaredField("buffer");
        field.setAccessible(true);
        return (Collection<SinkRecord>) field.get(task);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final class SinkRecordWithOriginalMetadata extends SinkRecord {
        private final String originalTopic;
        private final Integer originalPartition;
        private final long originalOffset;

        private SinkRecordWithOriginalMetadata(
                String topic,
                int partition,
                long offset,
                String originalTopic,
                int originalPartition,
                long originalOffset) {
            super(topic, partition, null, null, null, null, offset);
            this.originalTopic = originalTopic;
            this.originalPartition = originalPartition;
            this.originalOffset = originalOffset;
        }

        public String originalTopic() {
            return originalTopic;
        }

        public Integer originalKafkaPartition() {
            return originalPartition;
        }

        public long originalKafkaOffset() {
            return originalOffset;
        }
    }
}
