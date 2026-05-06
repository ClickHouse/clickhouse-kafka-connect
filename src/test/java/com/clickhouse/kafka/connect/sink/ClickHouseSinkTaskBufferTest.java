package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
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

    private static final CreateTableStatement PRIMITIVE_TYPES_TABLE = new CreateTableStatement()
            .column("off16", "Int16")
            .column("str", "String")
            .column("p_int8", "Int8")
            .column("p_int16", "Int16")
            .column("p_int32", "Int32")
            .column("p_int64", "Int64")
            .column("p_float32", "Float32")
            .column("p_float64", "Float64")
            .column("p_bool", "Bool")
            .engine("MergeTree")
            .orderByColumn("off16");

    @Test
    public void bufferingDisabledByDefault() {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("buffer_disabled_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);
        task.put(sr);
        task.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void bufferFlushOnSizeThreshold() {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("buffer_size_flush_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

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
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "5000");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("buffer_shutdown_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

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
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "5000");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("buffer_rebalance_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

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
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "50000");
        props.put(ClickHouseSinkConfig.BUFFER_FLUSH_TIME, "2000");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("buffer_time_flush_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

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
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "2500");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("buffer_multi_batch_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

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
    public void exactlyOnceWithBufferRejectsTimeFlush() {
        // EO + buffer requires bufferFlushTime=0. Time-based flush is non-deterministic
        // across retries (wall clock), which breaks dedup-token reuse.
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        props.put(ClickHouseSinkConfig.BUFFER_FLUSH_TIME, "1000");
        props.put(ClickHouseSinkConfig.EXACTLY_ONCE, "true");

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        assertThrows(ConnectException.class, () -> task.start(props),
                "EO + buffer must throw when bufferFlushTime > 0");
    }

    @Test
    public void exactlyOnceWithBufferRejectsIgnorePartitions() {
        // EO + buffer requires per-partition batching. With ignorePartitionsWhenBatching=true,
        // QueryIdentifier is constructed via the (topic, queryId) ctor that sets partition=-1
        // (see QueryIdentifier.java:14-22). When partition == -1,
        // QueryIdentifier.getDeduplicationToken() returns null (QueryIdentifier.java:56-61).
        // A null token disables ClickHouse insert_deduplication_token entirely; CH falls back
        // to content-hash block dedup, which can miss whenever an SMT introduces any byte-level
        // variation across retries. The validator therefore rejects this combination at start().
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        props.put(ClickHouseSinkConfig.BUFFER_FLUSH_TIME, "0");
        props.put(ClickHouseSinkConfig.EXACTLY_ONCE, "true");
        props.put(ClickHouseSinkConfig.IGNORE_PARTITIONS_WHEN_BATCHING, "true");

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        assertThrows(ConnectException.class, () -> task.start(props),
                "EO + buffer must throw when ignorePartitionsWhenBatching=true");
    }

    // ==================== Offset management tests (crash & rebalance safety) ====================

    @Test
    public void preCommitReturnsEmptyWhenAllBuffered() {
        // Simulates crash safety: if records are only buffered (not flushed to CH),
        // preCommit() must return empty so offsets are NOT committed.
        // On crash/restart, Kafka redelivers these records.
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "5000");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("precommit_empty_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

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
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("precommit_offset_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

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
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("precommit_reset_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

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
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("precommit_rebalance_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

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
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("crash_after_flush_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

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
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("insert_fail_no_tolerance_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

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
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        props.put(ClickHouseSinkConfig.ERRORS_TOLERANCE, ClickHouseSinkConfig.ERROR_TOLERANCE_ALL);
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("insert_fail_tolerance_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

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
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "500");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("buffer_multi_partition_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

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

    // ==================== Exactly-once + buffer (strict chunking) tests ====================

    /**
     * Helper config for strict-chunking mode: EO on, buffer on, time disabled,
     * per-partition batching enforced.
     */
    private Map<String, String> strictChunkingProps(int bufferCount) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, String.valueOf(bufferCount));
        props.put(ClickHouseSinkConfig.BUFFER_FLUSH_TIME, "0");
        props.put(ClickHouseSinkConfig.EXACTLY_ONCE, "true");
        props.put(ClickHouseSinkConfig.IGNORE_PARTITIONS_WHEN_BATCHING, "false");
        return props;
    }

    @Test
    public void strictChunkingStartsWithValidConfig() {
        // Valid EO + buffer combo must start without throwing.
        Map<String, String> props = strictChunkingProps(500);
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("strict_start_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);
        task.stop();
    }

    @Test
    public void strictChunkingTailRecordsRemainBuffered() throws InterruptedException {
        // Below-threshold records must NOT flush. Tail stays in buffer for next put().
        // This is the core determinism guarantee — flushes only happen at fixed N boundary.
        Map<String, String> props = strictChunkingProps(500);
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("strict_tail_buffered_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        // 300 records below 500 threshold → all stay buffered.
        List<SinkRecord> batch = SchemalessTestData.createPrimitiveTypes(topic, 1, 300);
        task.put(batch);

        // Recheck after a short wait so a delayed/async insert path would still surface here.
        assertCountStaysAt(chc, topic, 0, 2_000,
                "Tail records below bufferCount must remain buffered");
        task.stop();
    }

    /**
     * Polls the row count for {@code waitMillis} milliseconds and fails if it ever
     * differs from {@code expected}. Guards against false-negative count==0 assertions
     * that race a delayed write.
     */
    private static void assertCountStaysAt(ClickHouseHelperClient chc, String topic,
                                           int expected, long waitMillis, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + waitMillis;
        do {
            assertEquals(expected, ClickHouseTestHelpers.countRows(chc, topic), message);
            Thread.sleep(250);
        } while (System.currentTimeMillis() < deadline);
    }

    /**
     * Parametrized: covers single-chunk, multi-chunk, exact-threshold, below-threshold,
     * and exact-multi-threshold cases for the strict-chunking flush logic.
     * bufferCount is fixed at 500 across all rows.
     */
    @ParameterizedTest(name = "{0} input records with bufferCount=500 → {1} flushed, {2} buffered")
    @CsvSource({
            "499,  0,    499",  // below threshold — full tail
            "500,  500,  0",    // exact threshold — single chunk, no tail
            "700,  500,  200",  // overshoot — single chunk, tail held back
            "1000, 1000, 0",    // exact two chunks
            "1100, 1000, 100",  // two chunks + tail
            "1500, 1500, 0"     // exact three chunks in one put()
    })
    public void strictChunkingFlushesNRecordChunks(int recordsIn, int expectedFlushed,
                                                   int expectedTail) {
        Map<String, String> props = strictChunkingProps(500);
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("strict_chunk_param_" + recordsIn);
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        List<SinkRecord> batch = SchemalessTestData.createPrimitiveTypes(topic, 1, recordsIn);
        task.put(batch);

        assertEquals(expectedFlushed, ClickHouseTestHelpers.countRows(chc, topic),
                String.format("Expected exactly %d records flushed, %d to stay in buffer (input %d)",
                        expectedFlushed, expectedTail, recordsIn));
        task.stop();
    }

    @Test
    public void strictChunkingPerPartitionIndependent() {
        // P0 hits threshold, P1 does not. Only P0 should flush. P1 stays buffered.
        // Validates that per-partition buckets do not pool toward a global threshold.
        Map<String, String> props = strictChunkingProps(500);
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("strict_per_partition_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        List<SinkRecord> mixed = new ArrayList<>();
        mixed.addAll(SchemalessTestData.createPrimitiveTypes(topic, 0, 500));   // exactly threshold
        mixed.addAll(SchemalessTestData.createPrimitiveTypes(topic, 1, 200));   // below threshold
        task.put(mixed);

        assertEquals(500, ClickHouseTestHelpers.countRows(chc, topic),
                "Only P0 should flush (hit threshold). P1 should remain buffered.");
        task.stop();
    }

    @Test
    public void strictChunkingTailNotFlushedOnStop() {
        // Stop with tail records still buffered. Records must NOT be inserted.
        // Their offsets were never committed → Kafka redelivers on restart.
        // Same idempotency contract as relaxed mode.
        Map<String, String> props = strictChunkingProps(500);
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("strict_stop_tail_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        List<SinkRecord> batch = SchemalessTestData.createPrimitiveTypes(topic, 1, 100);
        task.put(batch);
        task.close(Collections.singletonList(new TopicPartition(topic, 1)));
        task.stop();

        assertEquals(0, ClickHouseTestHelpers.countRows(chc, topic),
                "Tail records must not be flushed on stop — Kafka redelivers them on restart");
    }

    @Test
    public void strictChunkingPreCommitOnlyForFlushedChunks() {
        // After put with 700 records: 500 flushed (P1 offset advances to 500), 200 tail.
        // preCommit must return 500, not 700. Tail's offsets stay uncommitted.
        Map<String, String> props = strictChunkingProps(500);
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("strict_precommit_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        List<SinkRecord> batch = SchemalessTestData.createPrimitiveTypes(topic, 1, 700);
        task.put(batch);

        Map<TopicPartition, OffsetAndMetadata> result = task.preCommit(new java.util.HashMap<>());
        TopicPartition tp = new TopicPartition(topic, 1);
        assertEquals(500, result.get(tp).offset(),
                "preCommit must return offset 500 for flushed chunk only — not 700 (tail uncommitted)");
        task.stop();
    }

    @Test
    public void strictChunkingRebalanceDropsPartitionBucket() {
        // Buffer has tail records for P0 and P1. Revoke P1.
        // P1's bucket must be discarded entirely. P0's bucket must remain intact.
        Map<String, String> props = strictChunkingProps(500);
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("strict_rebalance_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);

        // Both partitions below threshold → both stay buffered.
        List<SinkRecord> mixed = new ArrayList<>();
        mixed.addAll(SchemalessTestData.createPrimitiveTypes(topic, 0, 200));
        mixed.addAll(SchemalessTestData.createPrimitiveTypes(topic, 1, 200));
        task.put(mixed);
        assertEquals(0, ClickHouseTestHelpers.countRows(chc, topic),
                "Tail records on both partitions stay buffered");

        // Revoke P1.
        task.close(Collections.singletonList(new TopicPartition(topic, 1)));

        // Push P0 over its threshold. Only P0 records flush.
        List<SinkRecord> moreP0 = SchemalessTestData.createPrimitiveTypes(topic, 0, 400);
        task.put(moreP0);

        // P0 had 200 + 400 = 600 → 1 chunk of 500 flushed, tail 100 buffered.
        assertEquals(500, ClickHouseTestHelpers.countRows(chc, topic),
                "Only P0's chunk should flush. P1 records were dropped on revoke.");

        Map<TopicPartition, OffsetAndMetadata> committed = task.preCommit(new java.util.HashMap<>());
        assertTrue(committed.containsKey(new TopicPartition(topic, 0)),
                "P0 offset committed");
        assertFalse(committed.containsKey(new TopicPartition(topic, 1)),
                "P1 must not appear — bucket dropped on revoke, no offsets to commit");

        task.stop();
    }

    // ==================== Replay & dedup tests (token + state machine) ====================

    /**
     * Verifies dedup token reuse on flush replay. Mirrors the most common real-world
     * crash recovery: the connector flushed a chunk to ClickHouse, but the broker offset
     * commit (or rebalance) was lost before Kafka recorded progress. On restart Kafka
     * redelivers the same offsets, the buffer fills to the same {@code bufferCount},
     * the chunk reproduces the same {@code (min, max)} → same dedup token →
     * ClickHouse no-ops the second insert.
     */
    @Test
    public void strictChunkingDedupesReplayedFlush() {
        Map<String, String> props = strictChunkingProps(500);
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("strict_dedup_replay_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

        // Run 1: flush 500 records → token topic-1-0-499, state stored AFTER_PROCESSING.
        ClickHouseSinkTask runOne = new ClickHouseSinkTask();
        runOne.start(props);
        runOne.put(SchemalessTestData.createPrimitiveTypes(topic, 1, 500));
        assertEquals(500, ClickHouseTestHelpers.countRows(chc, topic),
                "Run 1 should flush exactly 500 records");
        // Simulate crash: offset commit lost (do not call preCommit). Buffer was already
        // empty after the strict flush, so stop() drops nothing.
        runOne.stop();

        // Run 2: Kafka redelivers offsets [0, 499]. Buffer hits threshold, attempts flush.
        // Same offsets → same token → ClickHouse insert_deduplication_token catches it,
        // and the state machine's AFTER_PROCESSING + SAME branch suppresses the insert
        // entirely.
        ClickHouseSinkTask runTwo = new ClickHouseSinkTask();
        runTwo.start(props);
        runTwo.put(SchemalessTestData.createPrimitiveTypes(topic, 1, 500));
        assertEquals(500, ClickHouseTestHelpers.countRows(chc, topic),
                "Run 2 must not duplicate — token + state machine dedup the replayed chunk");
        runTwo.stop();
    }

    /**
     * Replay scenario where run 1 had records buffered (below threshold) at crash time.
     * Buffer content is volatile — lost on crash. Kafka offsets for those records were
     * never committed (preCommit only returns flushed offsets). On restart Kafka
     * redelivers the same offsets plus newly-arrived ones; the rebuilt buffer crosses
     * threshold and flushes deterministically.
     *
     * <p>Mirrors chernser's first scenario: 200 records buffered then crash, restart
     * sees the same 200 plus 300 new = 500 records → exactly 500 in CH.
     */
    @Test
    public void strictChunkingDedupesReplayWithBufferedTail() {
        Map<String, String> props = strictChunkingProps(500);
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("strict_dedup_tail_replay_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

        // Run 1: 200 records arrive, all buffered (below threshold). No flush, no state.
        ClickHouseSinkTask runOne = new ClickHouseSinkTask();
        runOne.start(props);
        runOne.put(SchemalessTestData.createPrimitiveTypes(topic, 1, 200));
        assertEquals(0, ClickHouseTestHelpers.countRows(chc, topic),
                "Run 1 buffered 200 records (below threshold) — none should reach CH");
        runOne.stop();

        // Run 2: Kafka redelivers offsets [0, 199] (uncommitted) plus 300 new = [0, 499].
        // Buffer fills to 500, flushes once with token topic-1-0-499.
        // No prior state → NONE branch → insert; state advances to AFTER_PROCESSING (0, 499).
        ClickHouseSinkTask runTwo = new ClickHouseSinkTask();
        runTwo.start(props);
        runTwo.put(SchemalessTestData.createPrimitiveTypes(topic, 1, 500));
        assertEquals(500, ClickHouseTestHelpers.countRows(chc, topic),
                "Run 2 must flush exactly 500 — the previously-buffered 200 plus 300 new");
        runTwo.stop();
    }

    /**
     * Replay scenario where run 1 successfully flushed a chunk but kept tail records
     * in the buffer, then crashed. On restart Kafka redelivers from the start because
     * the flushed chunk's offsets were not committed (preCommit was never called).
     * The state machine catches the replay via AFTER_PROCESSING + SAME, suppressing
     * the second insert; subsequent records continue from the next chunk boundary.
     *
     * <p>Mirrors chernser's second scenario: a sequence of partial deliveries that
     * cumulatively spans multiple chunks, with a replay in the middle, must end up
     * with exactly the unique-record count in ClickHouse.
     */
    @Test
    public void strictChunkingDedupesAcrossMultipleChunkReplays() {
        Map<String, String> props = strictChunkingProps(500);
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("strict_dedup_multi_chunk_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);

        // Run 1: 500 records flush as one chunk (state (0, 499, AFTER)), then 200 more
        // remain buffered as tail.
        ClickHouseSinkTask runOne = new ClickHouseSinkTask();
        runOne.start(props);
        runOne.put(SchemalessTestData.createPrimitiveTypes(topic, 1, 700));
        assertEquals(500, ClickHouseTestHelpers.countRows(chc, topic),
                "Run 1 should flush 500, hold 200 as tail");
        runOne.stop();

        // Run 2: Kafka redelivers offsets [0, 699] (offset commit lost; tail's offsets
        // were never committed and the flushed chunk's commit was lost too) plus 300 new
        // = [0, 999]. Buffer fills:
        //   - [0, 499] hits threshold first → flush. State machine sees AFTER (0, 499) +
        //     incoming (0, 499) → SAME → no insert.
        //   - [500, 999] hits threshold → flush. State sees AFTER (0, 499) + incoming
        //     (500, 999) → NEW → insert; state advances to (500, 999, AFTER).
        // Net effect: ClickHouse has 1000 unique records, not 1700.
        ClickHouseSinkTask runTwo = new ClickHouseSinkTask();
        runTwo.start(props);
        runTwo.put(SchemalessTestData.createPrimitiveTypes(topic, 1, 1000));
        assertEquals(1000, ClickHouseTestHelpers.countRows(chc, topic),
                "Run 2 must end with 1000 unique records — the first chunk dedups via " +
                "state machine SAME, the second chunk inserts as NEW");
        runTwo.stop();
    }
}
