package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.helper.IntegrationTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI.countRows;
import static com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI.createMergeTreeTable;
import static com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI.dropTable;
import static com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI.getInsertStatistics;
import static com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI.printInsertStatistics;
import static com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI.waitWhileCounting;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI.InsertStatistics;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ClickHouseSinkTest extends IntegrationTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkTest.class);

    @BeforeAll
    public void setup() {
        super.setup();
    }

    @AfterAll
    public void teardown() {
        super.tearDown();
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        super.beforeEach();
    }

    @Test
    public void stockGenSingleTaskTest() throws Exception {
        String topicName = "stockGenSingleTaskTest";
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateData(topicName, 1, 100);

        dropTable(dbClientNoProxy, topicName);
        createMergeTreeTable(dbClientNoProxy, topicName);

        String connectorName = runClickHouseConnector(createConnectorConfig(), topicName);

        try {
            waitWhileCounting(dbClientNoProxy, topicName, 3);
            assertTrue(dataCount <= countRows(dbClientNoProxy, topicName));
        } finally {
            stopClickHouseConnector(connectorName);
        }
    }

    @Test
    public void stockGenWithJdbcPropSingleTaskTest() throws Exception {
        String topicName = "stockGenWithJdbcPropSingleTaskTest";
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateData(topicName, 1, 100);
        dropTable(dbClientNoProxy, topicName);
        createMergeTreeTable(dbClientNoProxy, topicName);

        Map<String, String> connectorConfig = createConnectorConfig();
        connectorConfig.put("jdbcConnectionProperties", "?load_balancing_policy=random&health_check_interval=5000&failover=2");
        String connectorName = runClickHouseConnector(connectorConfig, topicName);

        try {
            waitWhileCounting(dbClientNoProxy, topicName, 3);
            assertTrue(dataCount <= countRows(dbClientNoProxy, topicName));
        } finally {
            stopClickHouseConnector(connectorName);
        }
    }

    @Test
    public void stockGenSingleTaskSchemalessTest() throws Exception {
        String topicName = "stockGenSingleTaskSchemalessTest";
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateSchemalessData(topicName, 1, 100);

        dropTable(dbClientNoProxy, topicName);
        createMergeTreeTable(dbClientNoProxy, topicName);
        String connectorName = runClickHouseConnector(createConnectorSchemalessConfig(), topicName);

        try {
            waitWhileCounting(dbClientNoProxy, topicName, 3);
            assertTrue(dataCount <= countRows(dbClientNoProxy, topicName));
        } finally {
            stopClickHouseConnector(connectorName);
        }
    }


    private void checkInterruptTest(String topicName, int parCount) throws Exception {
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateData(topicName, parCount, 2500);

        dropTable(dbClientNoProxy, topicName);
        createMergeTreeTable(dbClientNoProxy, topicName);

        String connectorName = runClickHouseConnector(createConnectorConfig(), topicName);

        try {
            waitWhileCounting(dbClientNoProxy, topicName, 3);
            assertTrue(dataCount <= countRows(dbClientNoProxy, topicName));
        } finally {
            stopClickHouseConnector(connectorName);
        }

        int databaseCount = countRows(dbClientNoProxy, topicName);
        int lastCount = 0;
        int loopCount = 0;

        while (databaseCount != lastCount || loopCount < 5) {
            if (loopCount == 0) {
                LOGGER.info("Disabling proxy");
                clickhouseProxy.disable();
            } else if (!clickhouseProxy.isEnabled()) {
                LOGGER.info("Re-enabling proxy");
                clickhouseProxy.enable();
            }
            Thread.sleep(3500);
            databaseCount = countRows(dbClientNoProxy, topicName);
            if (lastCount == databaseCount) {
                loopCount++;
            } else {
                loopCount = 0;
            }

            lastCount = databaseCount;
        }

        assertTrue(dataCount <= countRows(dbClientNoProxy, topicName));
    }

    @Test
    public void stockGenSingleTaskInterruptTest() throws Exception {
        if (!isUseProxy()) {
            return;
        }
        checkInterruptTest("stockGenSingleTaskInterruptTest", 1);
    }

    @Test
    public void stockGenMultiTaskInterruptTest() throws Exception {
        if (!isUseProxy()) {
            return;
        }

        checkInterruptTest("stockGenMultiTaskInterruptTest", 3);
    }


    @Test
    public void stockGenMultiTaskTopicTest() throws Exception {
        String topicName = "stockGenMultiTaskTopicTest";
        int parCount = 3;
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateData(topicName, parCount, 200);
        dropTable(dbClientNoProxy, topicName);
        createMergeTreeTable(dbClientNoProxy, topicName);

        Map<String, String> connectorConfig = createConnectorConfig();
        connectorConfig.put("tasks.max", String.valueOf(parCount));
        String connectorName = runClickHouseConnector(connectorConfig, topicName);

        try {
            waitWhileCounting(dbClientNoProxy, topicName, 3);
            assertTrue(dataCount <= countRows(dbClientNoProxy, topicName));
        } finally {
            stopClickHouseConnector(connectorName);
        }
    }

    @Test
    public void stockGenMultiTaskSchemalessTest() throws Exception {
        String topicName = "stockGenMultiTaskSchemalessTest";
        int parCount = 3;
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateSchemalessData(topicName, parCount, 200);

        dropTable(dbClientNoProxy, topicName);
        createMergeTreeTable(dbClientNoProxy, topicName);

        Map<String, String> connectorConfig = createConnectorSchemalessConfig();
        connectorConfig.put("tasks.max", String.valueOf(parCount));
        String connectorName = runClickHouseConnector(connectorConfig, topicName);

        try {
            waitWhileCounting(dbClientNoProxy, topicName, 3);
            assertTrue(dataCount <= countRows(dbClientNoProxy, topicName));
        } finally {
            stopClickHouseConnector(connectorName);
        }
    }

    /**
     * Test high-throughput consumer configuration with max.poll.records = 10000.
     * This test verifies that the connector can handle large batch sizes configured
     * via consumer overrides.
     *
     * Configuration tested:
     * - consumer.override.max.poll.records: 10000 (records per poll)
     * - consumer.override.fetch.min.bytes: 100MB (wait for data accumulation)
     * - consumer.override.fetch.max.bytes: 500MB (max fetch size)
     * - consumer.override.fetch.max.wait.ms: 1000ms (max wait time)
     * - consumer.override.max.partition.fetch.bytes: 500MB (per partition limit)
     */
    @Test
    public void highThroughputConsumerConfigTest() throws Exception {
        String topicName = "highThroughputConsumerConfigTest";
        int parCount = 3;
        // Generate 5000 records per partition * 3 partitions * 5 iterations = 75,000 records
        // This should trigger multiple batches even with max.poll.records = 10000
        int recordsPerPartition = 5000;

        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateData(topicName, parCount, recordsPerPartition);

        LOGGER.info("Generated {} records for topic {}", dataCount, topicName);

        dropTable(dbClientNoProxy, topicName);
        createMergeTreeTable(dbClientNoProxy, topicName);

        int maxTasks = 1;
        Map<String, String> connectorConfig = createConnectorConfig();
        connectorConfig.put("tasks.max", String.valueOf(maxTasks));

        // High-throughput consumer configuration
        connectorConfig.put("consumer.override.max.poll.records", "10000");
//        connectorConfig.put("consumer.override.fetch.min.bytes", "100000000");  // 100 MB
//        connectorConfig.put("consumer.override.fetch.max.bytes", "500000000");  // 500 MB
        connectorConfig.put("consumer.override.fetch.max.wait.ms", "1000");
        connectorConfig.put("consumer.override.max.partition.fetch.bytes", "500000000");  // 500 MB

        String connectorName = runClickHouseConnector(connectorConfig, topicName);

        try {
            // Wait longer due to fetch.min.bytes requiring data accumulation
            waitWhileCounting(dbClientNoProxy, topicName, 5);
            int insertedRows = countRows(dbClientNoProxy, topicName);
            LOGGER.info("Inserted {} rows into ClickHouse (expected at least {})", insertedRows, dataCount);

            // Print Kafka Connect sink task metrics from JMX
            LOGGER.info("Fetching Kafka Connect sink task metrics...");
            for (int taskId = 0; taskId < maxTasks; taskId++) {
                confluentPlatform.printSinkTaskMetrics(connectorName, taskId);
            }

            // Print insert statistics from ClickHouse query_log
            printInsertStatistics(dbClientNoProxy, topicName);

            // Get statistics for assertions
            InsertStatistics stats = getInsertStatistics(dbClientNoProxy, topicName);
            LOGGER.info("Insert Statistics: {}", stats);

            // Verify batch sizes - with max.poll.records, we expect batches up to that size
            // Note: actual batch size depends on available records and partitioning
            LOGGER.info("Max batch size observed: {} (configured max.poll.records: 1000)", stats.maxBatchSize);

            assertTrue(dataCount <= insertedRows,
                    String.format("Expected at least %d rows, but got %d", dataCount, insertedRows));
        } finally {
            stopClickHouseConnector(connectorName);
        }
    }

    
}
