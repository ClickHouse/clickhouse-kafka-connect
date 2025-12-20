package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.query.Records;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI;
import com.clickhouse.kafka.connect.sink.helper.IntegrationTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExactlyOnceTest extends IntegrationTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExactlyOnceTest.class);
    private static ClickHouseAPI clickhouseAPI;
    private static final Properties properties = System.getProperties();
    private static final String SINK_CONNECTOR_NAME = "ClickHouseSinkConnector";

    @BeforeAll
    public void setUp() {
        super.setup();
        clickhouseAPI = new ClickHouseAPI(properties);
    }

    @AfterAll
    public void tearDown() {
        super.tearDown();
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        super.beforeEach();
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
    }

    private boolean compareSchemalessCounts(String topicName, int partitions) throws Exception {
        // createReplicatedMergeTreeTable(dbClientNoProxy, topicName); // for cluster version
        ClickHouseAPI.createMergeTreeTable(dbClientNoProxy, topicName);
        ClickHouseAPI.clearTable(dbClientNoProxy, topicName);
        confluentPlatform.createTopic(topicName, partitions);
        int count = generateSchemalessData(topicName, partitions, 250);
        LOGGER.info("Expected Total: {}", count);

        Map<String, String> connectorConfig = createConnectorSchemalessConfig();
        connectorConfig.put("tasks.max", String.valueOf(partitions));
        connectorConfig.put("exactlyOnce", "true");
        runClickHouseConnector(connectorConfig, topicName);

        ClickHouseAPI.waitWhileCounting(dbClientNoProxy, topicName, 5);

        int[] databaseCounts = ClickHouseAPI.getCounts(dbClientNoProxy, topicName); // Essentially the final count
        LOGGER.info("Database Counts: {}", Arrays.toString(databaseCounts));
        ClickHouseAPI.dropTable(dbClientNoProxy, topicName);
        return databaseCounts[2] == 0 && databaseCounts[1] == count;
    }

    @Test
    public void checkTotalsEqual() throws Exception {
        assertTrue(compareSchemalessCounts("singlePartitionTopic", 1));
    }

    @Test
    public void checkTotalsEqualMulti() throws Exception {
        assertTrue(compareSchemalessCounts("multiPartitionTopic", 3));
    }

    private void checkSpottyNetworkSchemaless(String topicName, int numberOfPartitions) throws Exception {
        boolean allSuccess = true;
        int runCount = 1;
        do {
            LOGGER.info("Run: {}", runCount);
            confluentPlatform.createTopic(topicName, numberOfPartitions);

            ClickHouseAPI.createMergeTreeTable(dbClientNoProxy, topicName);
            ClickHouseAPI.clearTable(dbClientNoProxy, topicName);

            int count = generateSchemalessData(topicName, numberOfPartitions, 1500);

            Map<String, String> connectorConfig = createConnectorSchemalessConfig();
            connectorConfig.put("tasks.max", String.valueOf(numberOfPartitions));
            connectorConfig.put("exactlyOnce", "true");
            String connectorName = runClickHouseConnector(connectorConfig, topicName);

            // clickhouseAPI.restartService(); // should be replaced with container restart? 
            confluentPlatform.restartConnector(connectorName);

            LOGGER.info("Expected Total: {}", count);
            ClickHouseAPI.waitWhileCounting(dbClientNoProxy, topicName, 7);

            int[] databaseCounts = ClickHouseAPI.getCounts(dbClientNoProxy, topicName); // Essentially the final count
            if (databaseCounts[2] != 0 || databaseCounts[1] != count) {
                allSuccess = false;
                LOGGER.error("Duplicates: {}", databaseCounts[2]);
                Records records = ClickHouseAPI.selectDuplicates(dbClientNoProxy, topicName);
                records.forEach(record -> LOGGER.error("Duplicate: {}", record));
            }

            stopClickHouseConnector(connectorName);
            runCount++;
        } while (runCount < 3 && allSuccess);

        assertTrue(allSuccess);
    }

    @Test
    public void checkSpottyNetwork() throws Exception {
        if (IntegrationTestBase.isCloud()) {
            return;
        }
        checkSpottyNetworkSchemaless("checkSpottyNetworkSinglePartition", 1);
    }

    @Test
    public void checkSpottyNetworkMulti() throws Exception {
        if (IntegrationTestBase.isCloud()) {
            return;
        }
        checkSpottyNetworkSchemaless("checkSpottyNetworkMultiPartitions", 3);
    }
}
