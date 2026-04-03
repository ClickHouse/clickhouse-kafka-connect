package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseDeploymentType;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;

public class ClickHouseSinkTaskTest extends ClickHouseBase {

    public static final int DEFAULT_TOTAL_RECORDS = 1000;

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
            .orderByColumn("off16");

    public Collection<SinkRecord> createDBTopicSplit(int dbRange, long timeStamp, String topic, int partition, String splitChar) {
        Gson gson = new Gson();
        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, dbRange).forEachOrdered(i -> {
            String newTopic = i + "_" + timeStamp + splitChar + topic  ;
            LongStream.range(0, DEFAULT_TOTAL_RECORDS).forEachOrdered(n -> {
                Map<String, Object> value_struct = new HashMap<>();
                value_struct.put("str", "num" + n);
                value_struct.put("off16", (short)n);
                value_struct.put("p_int8", (byte)n);
                value_struct.put("p_int16", (short)n);
                value_struct.put("p_int32", (int)n);
                value_struct.put("p_int64", (long)n);
                value_struct.put("p_float32", (float)n*1.1);
                value_struct.put("p_float64", (double)n*1.111111);
                value_struct.put("p_bool", (boolean)true);

                java.lang.reflect.Type gsonType = new TypeToken<HashMap>() {
                }.getType();
                String gsonString = gson.toJson(value_struct, gsonType);

                SinkRecord sr = new SinkRecord(
                        newTopic,
                        partition,
                        null,
                        null, null,
                        gsonString,
                        n,
                        System.currentTimeMillis(),
                        TimestampType.CREATE_TIME
                );
                array.add(sr);
            });
        });




        return array;
    }
    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void testExceptionHandling(ClickHouseDeploymentType deploymentType) {
        ClickHouseSinkTask task = new ClickHouseSinkTask();
        assertThrows(RuntimeException.class, () -> task.put(null));
        try {
            task.put(null);
        } catch (Exception e) {
            assertEquals(e.getClass(), RuntimeException.class);
            assertTrue(e.getCause() instanceof NullPointerException);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            assertTrue(sw.toString().contains("com.clickhouse.kafka.connect.util.Utils.handleException"));
        }
    }

    public ClickHouseResponseSummary dropTable(ClickHouseHelperClient chc, String tableName) {
        String dropTable = String.format("DROP TABLE IF EXISTS %s", tableName);
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer())
                     .query(dropTable)
                     .executeAndWait()) {
            return response.getSummary();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    @Disabled // TODO: Fix this test
    public void testDBTopicSplit(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props =  getBaseProps();
        props.put(ClickHouseSinkConfig.ENABLE_DB_TOPIC_SPLIT, "true");
        props.put(ClickHouseSinkConfig.DB_TOPIC_SPLIT_CHAR, ".");
        long timeStamp = System.currentTimeMillis();
        ClickHouseTestHelpers.createClient(props);
        String tableName = createTopicName("splitTopic");
        int dbRange = 10;
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        LongStream.range(0, dbRange).forEachOrdered(i -> {
            String databaseName = String.format("%d_%d" , i, timeStamp);
            String tmpTableName = String.format("`%s`.`%s`", databaseName, tableName);
            dropTable(chc, tmpTableName);
            ClickHouseTestHelpers.createDatabase(databaseName, chc, deploymentType);
            new CreateTableStatement(PRIMITIVE_TYPES_TABLE)
                    .tableName(tmpTableName)
                    .deploymentType(deploymentType)
                    .execute(chc);
        });

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        // Generate SinkRecords with different topics and check if they are split correctly
        Collection<SinkRecord> records = createDBTopicSplit(dbRange, timeStamp, tableName, 0, ".");
        try {
            task.start(props);
            task.put(records);
        } catch (Exception e) {
            fail("Exception should not be thrown");
        }
        // TODO: uncomment this when fixed
//        LongStream.range(0, dbRange).forEachOrdered(i -> {
//            int count = ClickHouseTestHelpers.countRows(chc, String.valueOf(i), tableName);
//            assertEquals(DEFAULT_TOTAL_RECORDS, count);
//        });
    }


    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void simplifiedBatchingSchemaless(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.IGNORE_PARTITIONS_WHEN_BATCHING, "true");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("schemaless_simple_batch_test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE)
                .tableName(topic)
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);
        sr.addAll(SchemalessTestData.createPrimitiveTypes(topic, 2));
        sr.addAll(SchemalessTestData.createPrimitiveTypes(topic, 3));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
        assertTrue(ClickHouseTestHelpers.validateRows(chc, topic, sr, deploymentType));
        //assertEquals(1, com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers.countInsertQueries(chc, topic));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    @Disabled
    public void clientNameTest(ClickHouseDeploymentType deploymentType) throws Exception {
        // TODO: fix instability of the test.
        if (isCloud) {
            // TODO: Temp disable for cloud because query logs not available in time. This is passing on cloud but is flaky.
            return;
        }
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.IGNORE_PARTITIONS_WHEN_BATCHING, "true");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("schemaless_simple_batch_test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE)
                .tableName(topic)
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);
        sr.addAll(SchemalessTestData.createPrimitiveTypes(topic, 2));
        sr.addAll(SchemalessTestData.createPrimitiveTypes(topic, 3));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
        assertTrue(ClickHouseTestHelpers.validateRows(chc, topic, sr, deploymentType));

        String flushLogsCluster = isCloud ? " ON CLUSTER 'default'"
                : deploymentType.isLocalCluster() ? " ON CLUSTER '" + deploymentType.clusterName + "'" : "";
        chc.queryV2("SYSTEM FLUSH LOGS" + flushLogsCluster).close();

        String queryLogFrom = deploymentType.isLocalCluster()
                ? "clusterAllReplicas('" + deploymentType.clusterName + "', system, query_log, rand())"
                : "system.query_log";
        String getLogRecords = String.format("SELECT http_user_agent, query FROM " + queryLogFrom +
                        "   WHERE query_kind = 'Insert' " +
                        "   AND type = 'QueryStart'" +
                        "   AND has(databases,'%1$s') " +
                        "   AND position(http_user_agent, '%2$s') > -1 LIMIT 100",
                chc.getDatabase(), ClickHouseHelperClient.CONNECT_CLIENT_NAME);

        String debugQuery = String.format("SELECT http_user_agent, query_kind, type FROM " + queryLogFrom + " LIMIT 10");
        List<GenericRecord> debugRecords = chc.getClient().queryAll(debugQuery);
        StringBuilder sb = new StringBuilder();
        for (GenericRecord record : debugRecords) {
            sb.append(record.getString("http_user_agent") + " " + record.getObject("query_kind") + " " + record.getObject("type") + ";");
        }

        List<GenericRecord> records = chc.getClient().queryAll(getLogRecords);
        assertFalse(records.isEmpty(), sb.toString());
        for (GenericRecord record : records) {
            assertTrue(record.getString(1).startsWith(ClickHouseHelperClient.CONNECT_CLIENT_NAME));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void statisticsTest(ClickHouseDeploymentType deploymentType) throws Exception {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.IGNORE_PARTITIONS_WHEN_BATCHING, "true");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("topic.statistics_test-01");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE)
                .tableName(topic)
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);
        sr.addAll(SchemalessTestData.createPrimitiveTypes(topic, 2));
        sr.addAll(SchemalessTestData.createPrimitiveTypes(topic, 3));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        final int taskId = chst.taskId();
        chst.put(sr);
        Thread.sleep(5000);
        chst.put(sr);

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        // Task metrics
        final String mbeanName = SinkTaskStatistics.getMBeanName(taskId);
        ObjectName sinkMBean = new ObjectName(mbeanName);
        Object receivedRecords = mBeanServer.getAttribute(sinkMBean, "ReceivedRecords");
        assertEquals(sr.size() * 2L, ((Long)receivedRecords).longValue());
        Object totalProcessingTime = mBeanServer.getAttribute(sinkMBean, "RecordProcessingTime");
        assertTrue((Long)totalProcessingTime > 1000L);
        Object totalTaskProcessingTime = mBeanServer.getAttribute(sinkMBean, "TaskProcessingTime");
        assertTrue((Long)totalTaskProcessingTime > 1000L);
        Object totalInsertedRecords = mBeanServer.getAttribute(sinkMBean, "InsertedRecords");
        assertEquals(sr.size() * 2L, ((Long)totalInsertedRecords).longValue());
        Object receivedBatches = mBeanServer.getAttribute(sinkMBean, "ReceivedBatches");
        assertEquals(2, ((Long)receivedBatches).longValue());
        Object failedRecords = mBeanServer.getAttribute(sinkMBean, "FailedRecords");
        assertEquals(0, ((Long)failedRecords).longValue());
        Object eventReceiveLag = mBeanServer.getAttribute(sinkMBean, "MeanReceiveLag");
        assertTrue((Long)eventReceiveLag > 0);
        Object insertedBytes = mBeanServer.getAttribute(sinkMBean, "InsertedBytes");
        assertTrue((Long)insertedBytes >= 872838);


        // Topic metrics
        final ObjectName topicMbeanName = new ObjectName(SinkTaskStatistics.getTopicMBeanName(taskId, topic));
        Object insertedRecords = mBeanServer.getAttribute(topicMbeanName, "TotalSuccessfulRecords");
        assertEquals(sr.size() * 2L, ((Long)insertedRecords).longValue());
        Object insertedBatches = mBeanServer.getAttribute(topicMbeanName, "TotalSuccessfulBatches");
        assertEquals(2, ((Long)insertedBatches).longValue());

        Object insertTime = mBeanServer.getAttribute(topicMbeanName, "MeanInsertTime");
        assertTrue((Long)insertTime >= 0);

        Object failedTopicRecords = mBeanServer.getAttribute(topicMbeanName, "TotalFailedRecords");
        assertEquals(0, ((Long)failedTopicRecords).longValue());
        Object failedTopicBatches = mBeanServer.getAttribute(topicMbeanName, "TotalFailedBatches");
        assertEquals(0, ((Long)failedTopicBatches).longValue());

        chst.stop();

        assertThrows(InstanceNotFoundException.class, () -> mBeanServer.getMBeanInfo(sinkMBean));
        assertThrows(InstanceNotFoundException.class, () -> mBeanServer.getMBeanInfo(topicMbeanName));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void receiveLagTimeTest(ClickHouseDeploymentType deploymentType) throws Exception {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.IGNORE_PARTITIONS_WHEN_BATCHING, "true");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("schemaless_simple_batch_test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE)
                .tableName(topic)
                .deploymentType(deploymentType)
                .execute(chc);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        final int taskId = chst.taskId();
        int n = 40;
        for (int i = 0; i < n; i++) {

            long k;
            if ( i < n * 0.25) {
                k = 2000;
            } else if ( i < n * 0.50) {
                k = 3000;
            } else {
                k = 500;
            }

            List<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);
            SinkRecord first = sr.get(0);
            long createTime = System.currentTimeMillis() - k;
            first = first.newRecord(first.topic(), first.kafkaPartition(), first.keySchema(), first.key(), first.valueSchema(),
                    first.value(), createTime);
            sr.set(0, first);
            chst.put(sr);

            Thread.sleep(1000);
        }
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        ObjectName topicMbeanName = new ObjectName(SinkTaskStatistics.getMBeanName(taskId));
        Object eventReceiveLag = mBeanServer.getAttribute(topicMbeanName, "MeanReceiveLag");
        assertTrue((Long)eventReceiveLag < 2000L);
        assertTrue((Long)eventReceiveLag > 400L, "eventReceiveLag: " + eventReceiveLag);

        for (int i = 0; i < n; i++) {

            long k = 300;
            List<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);
            SinkRecord first = sr.get(0);
            long createTime = System.currentTimeMillis() - k;
            first = first.newRecord(first.topic(), first.kafkaPartition(), first.keySchema(), first.key(), first.valueSchema(),
                    first.value(), createTime);
            sr.set(0, first);
            chst.put(sr);

            Thread.sleep(1000);
        }

        eventReceiveLag = mBeanServer.getAttribute(topicMbeanName, "MeanReceiveLag");
        assertTrue((Long)eventReceiveLag < 1000L, "eventReceiveLag: " + eventReceiveLag);
        assertTrue((Long)eventReceiveLag > 300L, "eventReceiveLag: " + eventReceiveLag);

        chst.stop();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void preCommitReturnsInsertedOffsetsForMultipleTopics(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.REPORT_INSERTED_OFFSETS, "true");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic1 = createTopicName("precommit_offsets_t1");
        String topic2 = createTopicName("precommit_offsets_t2");

        ClickHouseTestHelpers.dropTable(chc, topic1, deploymentType);
        ClickHouseTestHelpers.dropTable(chc, topic2, deploymentType);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE)
                .tableName(topic1)
                .deploymentType(deploymentType)
                .execute(chc);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE)
                .tableName(topic2)
                .deploymentType(deploymentType)
                .execute(chc);

        int totalRecordsTopic1 = 100;
        int totalRecordsTopic2 = 200;
        int partition = 0;

        List<SinkRecord> allRecords = new ArrayList<>();
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic1, partition, totalRecordsTopic1));
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic2, partition, totalRecordsTopic2));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(allRecords);

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        currentOffsets.put(new TopicPartition(topic1, partition), new OffsetAndMetadata(0));
        currentOffsets.put(new TopicPartition(topic2, partition), new OffsetAndMetadata(0));

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = chst.preCommit(currentOffsets);

        TopicPartition tp1 = new TopicPartition(topic1, partition);
        assertTrue(committedOffsets.containsKey(tp1), "Should contain offset for topic1");
        assertEquals(totalRecordsTopic1, committedOffsets.get(tp1).offset(),
                "Committed offset for topic1 should be maxOffset + 1");

        TopicPartition tp2 = new TopicPartition(topic2, partition);
        assertTrue(committedOffsets.containsKey(tp2), "Should contain offset for topic2");
        assertEquals(totalRecordsTopic2, committedOffsets.get(tp2).offset(),
                "Committed offset for topic2 should be maxOffset + 1");

        chst.stop();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void preCommitReturnsCurrentOffsetsWhenIgnorePartitions(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.IGNORE_PARTITIONS_WHEN_BATCHING, "true");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("precommit_ignore_partitions");

        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE)
                .tableName(topic)
                .deploymentType(deploymentType)
                .execute(chc);

        int totalRecords = 100;
        int partition = 0;

        List<SinkRecord> records = SchemalessTestData.createPrimitiveTypes(topic, partition, totalRecords);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(records);

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(totalRecords));

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = chst.preCommit(currentOffsets);

        assertSame(currentOffsets, committedOffsets,
                "preCommit should return the same currentOffsets map when ignorePartitionsWhenBatching is true");

        chst.stop();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void closeRemovesRevokedPartitionFromPreCommitOffsets(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.REPORT_INSERTED_OFFSETS, "true");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic1 = createTopicName("precommit_close_remove_t1");
        String topic2 = createTopicName("precommit_close_remove_t2");

        ClickHouseTestHelpers.dropTable(chc, topic1, deploymentType);
        ClickHouseTestHelpers.dropTable(chc, topic2, deploymentType);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE)
                .tableName(topic1)
                .deploymentType(deploymentType)
                .execute(chc);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE)
                .tableName(topic2)
                .deploymentType(deploymentType)
                .execute(chc);

        int totalRecordsTopic1 = 50;
        int totalRecordsTopic2 = 70;
        int partition = 0;

        List<SinkRecord> allRecords = new ArrayList<>();
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic1, partition, totalRecordsTopic1));
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic2, partition, totalRecordsTopic2));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(allRecords);

        TopicPartition revoked = new TopicPartition(topic1, partition);
        chst.close(List.of(revoked));

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        currentOffsets.put(revoked, new OffsetAndMetadata(0));
        TopicPartition active = new TopicPartition(topic2, partition);
        currentOffsets.put(active, new OffsetAndMetadata(0));

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = chst.preCommit(currentOffsets);

        assertFalse(committedOffsets.containsKey(revoked),
                "preCommit should not return offsets for revoked partition after close()");
        assertTrue(committedOffsets.containsKey(active),
                "preCommit should still return offsets for active partition");
        assertEquals(totalRecordsTopic2, committedOffsets.get(active).offset(),
                "Committed offset for active partition should be maxOffset + 1");

        chst.stop();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void onPartitionsRevokedRemovesRevokedPartitionFromPreCommitOffsets(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.REPORT_INSERTED_OFFSETS, "true");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic1 = createTopicName("precommit_revoke_remove_t1");
        String topic2 = createTopicName("precommit_revoke_remove_t2");

        ClickHouseTestHelpers.dropTable(chc, topic1, deploymentType);
        ClickHouseTestHelpers.dropTable(chc, topic2, deploymentType);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE)
                .tableName(topic1)
                .deploymentType(deploymentType)
                .execute(chc);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE)
                .tableName(topic2)
                .deploymentType(deploymentType)
                .execute(chc);

        int totalRecordsTopic1 = 40;
        int totalRecordsTopic2 = 60;
        int partition = 0;

        List<SinkRecord> allRecords = new ArrayList<>();
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic1, partition, totalRecordsTopic1));
        allRecords.addAll(SchemalessTestData.createPrimitiveTypes(topic2, partition, totalRecordsTopic2));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(allRecords);

        TopicPartition revoked = new TopicPartition(topic1, partition);
        chst.onPartitionsRevoked(List.of(revoked));

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        currentOffsets.put(revoked, new OffsetAndMetadata(0));
        TopicPartition active = new TopicPartition(topic2, partition);
        currentOffsets.put(active, new OffsetAndMetadata(0));

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = chst.preCommit(currentOffsets);

        assertFalse(committedOffsets.containsKey(revoked),
                "preCommit should not return offsets for revoked partition after onPartitionsRevoked()");
        assertTrue(committedOffsets.containsKey(active),
                "preCommit should still return offsets for active partition");
        assertEquals(totalRecordsTopic2, committedOffsets.get(active).offset(),
                "Committed offset for active partition should be maxOffset + 1");

        chst.stop();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void preCommitReturnsCurrentOffsetsWhenReportingInsertedOffsetsDisabled(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("precommit_report_offsets_off");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE)
                .tableName(topic)
                .deploymentType(deploymentType)
                .execute(chc);

        int partition = 0;
        int totalRecords = 100;
        List<SinkRecord> records = SchemalessTestData.createPrimitiveTypes(topic, partition, totalRecords);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(records);

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(999));

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = chst.preCommit(currentOffsets);

        assertSame(currentOffsets, committedOffsets,
                "preCommit should return currentOffsets when reportInsertedOffsets is false");
        assertEquals(999, committedOffsets.get(new TopicPartition(topic, partition)).offset(),
                "Offset should match the value from currentOffsets");

        chst.stop();
    }
}
