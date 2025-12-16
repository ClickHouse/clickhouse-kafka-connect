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
import static com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI.waitWhileCounting;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
