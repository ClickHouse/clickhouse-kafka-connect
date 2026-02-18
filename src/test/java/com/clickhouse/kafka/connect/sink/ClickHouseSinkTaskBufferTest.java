package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClickHouseSinkTaskBufferTest extends ClickHouseBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkTaskBufferTest.class);

    private static final String TABLE_CREATE_SQL = "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, " +
            "`p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) " +
            "Engine = MergeTree ORDER BY off16";

    @Test
    public void bufferingDisabledByDefault() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("buffer_disabled_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);
        task.put(sr);
        task.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void bufferFlushOnSizeThreshold() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("buffer_size_flush_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        // Send 300 records (below threshold of 500) - should NOT be flushed yet
        List<SinkRecord> batch1 = SchemalessTestData.createPrimitiveTypes(topic, 1, 300);
        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);
        task.put(batch1);

        assertEquals(0, ClickHouseTestHelpers.countRows(chc, topic),
                "Records should be buffered, not flushed yet");

        // Send 300 more records (total 600 > threshold 500) - should trigger flush
        List<SinkRecord> batch2 = SchemalessTestData.createPrimitiveTypes(topic, 1, 300);
        task.put(batch2);

        assertEquals(600, ClickHouseTestHelpers.countRows(chc, topic),
                "Buffer should have been flushed after reaching threshold");

        task.stop();
    }

    @Test
    public void bufferGracefulShutdownRedelivers() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "5000");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("buffer_shutdown_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        List<SinkRecord> records = SchemalessTestData.createPrimitiveTypes(topic, 1, 100);
        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);
        task.put(records);

        assertEquals(0, ClickHouseTestHelpers.countRows(chc, topic),
                "Records should be buffered, not flushed yet");

        // Simulate real framework shutdown: close(all) then stop()
        // Buffered records are discarded — their offsets were never committed,
        // so Kafka redelivers them on restart (at-least-once guarantee).
        TopicPartition tp = new TopicPartition(topic, 1);
        task.close(Collections.singletonList(tp));
        task.stop();

        assertEquals(0, ClickHouseTestHelpers.countRows(chc, topic),
                "Unflushed records should NOT be written — they'll be redelivered on restart");
    }

    @Test
    public void bufferRebalanceRemovesRevokedPartitions() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "5000");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("buffer_rebalance_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        // Buffer records from P0, P1, P2
        List<SinkRecord> allRecords = new ArrayList<>();
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic, 0, 100));
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic, 1, 100));
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic, 2, 100));
        task.put(allRecords);

        assertEquals(0, ClickHouseTestHelpers.countRows(chc, topic),
                "All 300 records should be buffered");

        // Simulate cooperative rebalance: P2 revoked
        TopicPartition revokedTp = new TopicPartition(topic, 2);
        task.close(Collections.singletonList(revokedTp));

        // Now add more records for P0 and P1 to push over threshold (200 remaining + 4900 new > 5000)
        List<SinkRecord> moreRecords = new ArrayList<>();
        moreRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic, 0, 2500));
        moreRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic, 1, 2500));
        task.put(moreRecords);

        // Only P0 and P1 records should have been written — P2 records were removed by close()
        int totalRows = ClickHouseTestHelpers.countRows(chc, topic);
        assertEquals(5200, totalRows,
                "Only P0 (2600) + P1 (2600) records should be written, P2 records removed by rebalance");

        task.stop();
    }

    @Test
    public void bufferFlushOnTimeThreshold() throws InterruptedException {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "50000");
        props.put(ClickHouseSinkConfig.BUFFER_FLUSH_TIME, "2000");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("buffer_time_flush_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        List<SinkRecord> records = SchemalessTestData.createPrimitiveTypes(topic, 1, 100);
        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);
        task.put(records);

        assertEquals(0, ClickHouseTestHelpers.countRows(chc, topic),
                "Records should be buffered, not flushed yet");

        // Wait for the time threshold to pass
        Thread.sleep(2500);

        // Next put (even empty) should trigger time-based flush
        task.put(new ArrayList<>());

        assertEquals(100, ClickHouseTestHelpers.countRows(chc, topic),
                "Buffered records should be flushed after time threshold");

        task.stop();
    }

    @Test
    public void bufferAccumulatesMultipleBatches() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "2500");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("buffer_multi_batch_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        // Simulate multiple poll() calls each delivering a small batch
        int totalRecords = 0;
        for (int i = 0; i < 5; i++) {
            List<SinkRecord> batch = SchemalessTestData.createPrimitiveTypes(topic, 1, 400);
            task.put(batch);
            totalRecords += batch.size();
        }

        // 5 * 400 = 2000, still below 2500 threshold
        assertEquals(0, ClickHouseTestHelpers.countRows(chc, topic),
                "Records should still be buffered (2000 < 2500)");

        // One more batch to push over threshold
        List<SinkRecord> finalBatch = SchemalessTestData.createPrimitiveTypes(topic, 1, 600);
        task.put(finalBatch);
        totalRecords += finalBatch.size();

        assertEquals(totalRecords, ClickHouseTestHelpers.countRows(chc, topic),
                "All accumulated records should be flushed after crossing threshold");

        task.stop();
    }

    @Test
    public void bufferIncompatibleWithExactlyOnce() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        props.put(ClickHouseSinkConfig.EXACTLY_ONCE, "true");

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        assertThrows(ConnectException.class, () -> task.start(props),
                "Buffering should not be allowed with exactly-once mode");
    }

    // ==================== Offset management tests (crash & rebalance safety) ====================

    @Test
    public void preCommitReturnsEmptyWhenAllBuffered() {
        // Simulates crash safety: if records are only buffered (not flushed to CH),
        // preCommit() must return empty so offsets are NOT committed.
        // On crash/restart, Kafka redelivers these records.
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "5000");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("precommit_empty_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        List<SinkRecord> records = SchemalessTestData.createPrimitiveTypes(topic, 1, 100);
        task.put(records);

        // All records are buffered, nothing flushed to CH
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new java.util.HashMap<>();
        currentOffsets.put(new TopicPartition(topic, 1), new OffsetAndMetadata(100));

        Map<TopicPartition, OffsetAndMetadata> result = task.preCommit(currentOffsets);
        assertTrue(result.isEmpty(),
                "preCommit must return empty when all records are still buffered — " +
                "crash at this point means Kafka redelivers from last committed offset");

        task.close(Collections.singletonList(new TopicPartition(topic, 1)));
        task.stop();
    }

    @Test
    public void preCommitReturnsCorrectOffsetsAfterFlush() {
        // After buffer flushes to ClickHouse, preCommit() must return the correct
        // offsets so the framework commits them.
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("precommit_offset_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        // Put 600 records across P0 and P1 → exceeds 500 threshold → flush
        List<SinkRecord> allRecords = new ArrayList<>();
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic, 0, 300));
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic, 1, 300));
        task.put(allRecords);

        // Verify data reached ClickHouse
        assertEquals(600, ClickHouseTestHelpers.countRows(chc, topic));

        // preCommit should return offsets for both partitions
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new java.util.HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> result = task.preCommit(currentOffsets);

        assertEquals(2, result.size(), "Should have offsets for 2 partitions");
        // SchemalessTestData creates records with offsets 0..299, so committed offset = 300
        assertEquals(300, result.get(new TopicPartition(topic, 0)).offset(),
                "P0 committed offset should be 300 (last offset 299 + 1)");
        assertEquals(300, result.get(new TopicPartition(topic, 1)).offset(),
                "P1 committed offset should be 300 (last offset 299 + 1)");

        task.stop();
    }

    @Test
    public void preCommitClearsAfterReturning() {
        // After preCommit() returns offsets, the next call should return empty
        // if no new data was flushed (matches S3's getOffsetToCommitAndReset pattern).
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("precommit_reset_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        List<SinkRecord> records = SchemalessTestData.createPrimitiveTypes(topic, 1, 600);
        task.put(records);

        // First preCommit: returns flushed offsets
        Map<TopicPartition, OffsetAndMetadata> result1 = task.preCommit(new java.util.HashMap<>());
        assertFalse(result1.isEmpty(), "First preCommit should return flushed offsets");

        // Second preCommit without new flushes: should be empty
        Map<TopicPartition, OffsetAndMetadata> result2 = task.preCommit(new java.util.HashMap<>());
        assertTrue(result2.isEmpty(), "Second preCommit should return empty — no new data flushed");

        task.stop();
    }

    @Test
    public void preCommitExcludesRevokedPartitionsAfterRebalance() {
        // After close() revokes a partition, preCommit() must NOT return offsets
        // for that partition, even if data was previously flushed for it.
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("precommit_rebalance_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        // Flush records from P0, P1, P2 (600 > 500 threshold)
        List<SinkRecord> allRecords = new ArrayList<>();
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic, 0, 200));
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic, 1, 200));
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic, 2, 200));
        task.put(allRecords);

        assertEquals(600, ClickHouseTestHelpers.countRows(chc, topic));

        // Simulate rebalance: revoke P2
        task.close(Collections.singletonList(new TopicPartition(topic, 2)));

        // preCommit should only return offsets for P0 and P1
        Map<TopicPartition, OffsetAndMetadata> result = task.preCommit(new java.util.HashMap<>());
        assertEquals(2, result.size(), "Should only have offsets for P0 and P1");
        assertTrue(result.containsKey(new TopicPartition(topic, 0)), "Should contain P0");
        assertTrue(result.containsKey(new TopicPartition(topic, 1)), "Should contain P1");
        assertFalse(result.containsKey(new TopicPartition(topic, 2)),
                "Should NOT contain revoked P2 — new owner manages its offsets");

        task.stop();
    }

    @Test
    public void crashAfterFlushIsIdempotent() {
        // Verifies that if ClickHouse receives data and then a crash happens
        // AFTER preCommit but BEFORE the framework commits, the data is safe:
        // Kafka will redeliver, and ClickHouse gets duplicates (at-least-once).
        // This test ensures the offsets returned by preCommit are correct.
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("crash_after_flush_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        // Batch 1: flush 600 records
        List<SinkRecord> batch1 = SchemalessTestData.createPrimitiveTypes(topic, 1, 600);
        task.put(batch1);
        assertEquals(600, ClickHouseTestHelpers.countRows(chc, topic));

        Map<TopicPartition, OffsetAndMetadata> offsets1 = task.preCommit(new java.util.HashMap<>());
        assertEquals(600, offsets1.get(new TopicPartition(topic, 1)).offset());

        // Batch 2: buffer 200 records (below threshold, not flushed)
        List<SinkRecord> batch2 = SchemalessTestData.createPrimitiveTypes(topic, 1, 200);
        task.put(batch2);
        assertEquals(600, ClickHouseTestHelpers.countRows(chc, topic),
                "Batch 2 should still be buffered");

        // preCommit returns empty — batch 2 not flushed
        Map<TopicPartition, OffsetAndMetadata> offsets2 = task.preCommit(new java.util.HashMap<>());
        assertTrue(offsets2.isEmpty(),
                "preCommit should be empty — only batch 2 is pending and it's still buffered");

        // CRASH HERE: Kafka committed offset 600 from offsets1.
        // On restart, Kafka redelivers from offset 600 → batch 2's records are redelivered.
        // Batch 1 is NOT redelivered. No data loss.

        task.close(Collections.singletonList(new TopicPartition(topic, 1)));
        task.stop();
    }

    // ==================== Insert failure edge cases (putDirect fails) ====================

    @Test
    public void putDirectFailsNoErrorTolerance_offsetsNotCommitted() {
        // Edge case: buffer flush triggers putDirect → insert fails → exception propagates.
        // Offsets must NOT be committed so Kafka redelivers on restart (at-least-once).
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("insert_fail_no_tolerance_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        // Buffer 300 records (below 500 threshold)
        List<SinkRecord> batch1 = SchemalessTestData.createPrimitiveTypes(topic, 1, 300);
        task.put(batch1);

        assertEquals(0, ClickHouseTestHelpers.countRows(chc, topic),
                "Records should be buffered, not flushed yet");

        // Drop the table to make the next insert fail
        ClickHouseTestHelpers.dropTable(chc, topic);

        // Add 300 more records to cross threshold → triggers flush → insert fails
        List<SinkRecord> batch2 = SchemalessTestData.createPrimitiveTypes(topic, 1, 300);
        assertThrows(RuntimeException.class, () -> task.put(batch2),
                "put() should propagate the exception when insert fails and errorsTolerance=false");

        // preCommit must return empty — no offsets should be committed for failed data
        Map<TopicPartition, OffsetAndMetadata> result = task.preCommit(new java.util.HashMap<>());
        assertTrue(result.isEmpty(),
                "preCommit must return empty when putDirect fails — " +
                "offsets for failed records must not be committed so Kafka redelivers them");

        task.stop();
    }

    @Test
    public void putDirectFailsWithErrorTolerance_offsetsCommitted() {
        // Edge case: buffer flush triggers putDirect → insert fails → error tolerance swallows it.
        // With error tolerance, records go to DLQ and offsets ARE committed (same as non-buffered behavior).
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        props.put(ClickHouseSinkConfig.ERRORS_TOLERANCE, ClickHouseSinkConfig.ERROR_TOLERANCE_ALL);
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("insert_fail_tolerance_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        // Buffer 300 records (below 500 threshold)
        List<SinkRecord> batch1 = SchemalessTestData.createPrimitiveTypes(topic, 1, 300);
        task.put(batch1);

        // Drop the table to make the next insert fail
        ClickHouseTestHelpers.dropTable(chc, topic);

        // Add 300 more records to cross threshold → triggers flush → insert fails but is tolerated
        List<SinkRecord> batch2 = SchemalessTestData.createPrimitiveTypes(topic, 1, 300);
        task.put(batch2);

        // With error tolerance, flushBuffer() continues after putDirect catches the exception.
        // Offsets should be committed because records were "handled" (sent to DLQ).
        Map<TopicPartition, OffsetAndMetadata> result = task.preCommit(new java.util.HashMap<>());
        assertFalse(result.isEmpty(),
                "preCommit should return offsets — error tolerance means records were handled (DLQ), " +
                "and offsets must advance to prevent infinite redelivery of bad records");
        assertEquals(300, result.get(new TopicPartition(topic, 1)).offset(),
                "Committed offset should be 300 (last offset 299 + 1) for the tolerated batch");

        task.stop();
    }

    // ==================== Partition tests ====================

    @Test
    public void bufferMultiplePartitions() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("buffer_multi_partition_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        // Records from different partitions should all be buffered together
        List<SinkRecord> allRecords = new ArrayList<>();
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic, 0, 200));
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic, 1, 200));
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic, 2, 200));

        task.put(allRecords);

        // 600 > 500 threshold, should be flushed
        assertEquals(600, ClickHouseTestHelpers.countRows(chc, topic),
                "Records from all partitions should be flushed together");

        task.stop();
    }
}
