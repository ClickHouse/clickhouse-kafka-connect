package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
    public void bufferFlushOnStop() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "5000");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("buffer_stop_flush_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        // Send records below threshold - should be buffered
        List<SinkRecord> records = SchemalessTestData.createPrimitiveTypes(topic, 1, 100);
        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);
        task.put(records);

        assertEquals(0, ClickHouseTestHelpers.countRows(chc, topic),
                "Records should be buffered, not flushed yet");

        // Stop should flush remaining buffered records
        task.stop();

        assertEquals(100, ClickHouseTestHelpers.countRows(chc, topic),
                "All buffered records should be flushed on stop");
    }

    @Test
    public void bufferFlushOnFrameworkFlush() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, "5000");
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("buffer_framework_flush_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, TABLE_CREATE_SQL);

        List<SinkRecord> records = SchemalessTestData.createPrimitiveTypes(topic, 1, 100);
        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);
        task.put(records);

        assertEquals(0, ClickHouseTestHelpers.countRows(chc, topic),
                "Records should be buffered, not flushed yet");

        // Framework flush() should flush buffered records to prevent data loss before offset commit
        task.flush(null);

        assertEquals(100, ClickHouseTestHelpers.countRows(chc, topic),
                "All buffered records should be flushed on framework flush");

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
