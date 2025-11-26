package com.clickhouse.kafka.connect.sink.dlq;

import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseBase;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkTask;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.SchemaTestData;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class FailureTest extends ClickHouseBase {

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
    }
    @Test
    void testSchemaValidationFailure() throws Exception {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("test_schema_validation_failure");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `uint8` UInt8, `uint16` UInt16, `uint32` UInt32, `uint64` UInt64) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> validRecordsPart1 = SchemaTestData.createUnsignedIntegers(topic, 1, 100);
        Collection<SinkRecord> invalidRecordsPart2 = createInvalidRecords(topic, 2, 100);
        Collection<SinkRecord> validRecordsPart3 =  SchemaTestData.createUnsignedIntegers(topic, 3, 100);

        List<SinkRecord> allRecords = new ArrayList<>(100 * 3);
        allRecords.addAll(validRecordsPart1);
        allRecords.addAll(invalidRecordsPart2);
        allRecords.addAll(validRecordsPart3);

        InMemoryDLQ dlq = new InMemoryDLQ();
        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.setErrorReporter(dlq);
        props.put(ClickHouseSinkConfig.ERRORS_TOLERANCE, "all");
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, "V1");
        props.put(ClickHouseSinkConfig.CLICKHOUSE_SETTINGS, "input_format_skip_unknown_fields=0");
        task.start(props);
        try {
            task.put(allRecords);
        } catch (Exception e) {
            fail("Should not throw exception");
        }

        assertEquals(100, dlq.size(), "Should have 100 records in DLQ");

        // Check metric
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final String mbeanName = SinkTaskStatistics.getMBeanName(task.taskId());
        ObjectName sinkMBean = new ObjectName(mbeanName);
        Object sentToDQL = mBeanServer.getAttribute(sinkMBean, "MessagesSentToDQL");
        assertEquals(dlq.size(), ((Long)sentToDQL).longValue());

        task.stop();
        assertEquals(200, ClickHouseTestHelpers.countRows(chc, topic));
        List<SinkRecord> sr = new ArrayList<>(200);
        sr.addAll(validRecordsPart1);
        sr.addAll(validRecordsPart3);

        assertTrue(ClickHouseTestHelpers.validateRows(chc, topic, sr));
    }

    public static List<SinkRecord> createInvalidRecords(String topic, int partition, int totalRecords) {
        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("uint8", Schema.OPTIONAL_INT8_SCHEMA)
                .field("uint16", Schema.INT64_SCHEMA)
                .field("uint32", Schema.OPTIONAL_INT32_SCHEMA)
                .field("uint64", Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, totalRecords).forEachOrdered(n -> {
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short) n)
                    .put("uint8", (byte) ThreadLocalRandom.current().nextInt(0, 127))
                    .put("uint16", (long) ThreadLocalRandom.current().nextInt(0, 32767))
                    .put("uint32", ThreadLocalRandom.current().nextInt(0, 2147483647))
                    .put("uint64", ThreadLocalRandom.current().nextLong(0, 2147483647));

            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });

        return array;
    }
}
