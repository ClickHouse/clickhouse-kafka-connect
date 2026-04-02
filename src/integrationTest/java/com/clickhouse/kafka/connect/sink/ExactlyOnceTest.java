package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.query.Records;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseCluster;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseDeploymentType;
import com.clickhouse.kafka.connect.sink.helper.ConfluentPlatform;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NOTE: this test does NOT run against standalone ClickHouse
 */
public class ExactlyOnceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExactlyOnceTest.class);
    public static ConfluentPlatform confluentPlatform;
    private static ClickHouseAPI clickhouseAPI;
    private static ClickHouseHelperClient chc;
    private static final Properties cloudProperties = System.getProperties();
    private static final String SINK_CONNECTOR_NAME = "ClickHouseSinkConnector";
    private static final boolean isCluster = ClickHouseTestHelpers.isCluster();
    private static final CreateTableStatement STOCK_TABLE = new CreateTableStatement()
            .column("side", "String")
            .column("quantity", "Int32")
            .column("symbol", "String")
            .column("price", "Int32")
            .column("account", "String")
            .column("userid", "String")
            .column("insertTime", "DateTime DEFAULT now()")
            .orderByColumn("symbol");

    public static Stream<ClickHouseDeploymentType> clusterConfigs() {
        return ClickHouseTestHelpers.clusterConfigs();
    }

    @BeforeAll
    public static void checkPropsExistAndSetUp() {
        if (isCluster) {
            chc = new ClickHouseHelperClient.ClickHouseClientBuilder(
                    ClickHouseCluster.getHost(), ClickHouseCluster.getPort(), ClickHouseProxyType.IGNORE, null, -1)
                    .setDatabase(ClickHouseTestHelpers.DATABASE_DEFAULT)
                    .setUsername(ClickHouseTestHelpers.USERNAME_DEFAULT)
                    .setPassword("")
                    .sslEnable(false)
                    .useClientV2(true)
                    .build();
            // clickhouseAPI is intentionally left null — restartService() is cloud-only
        } else {
            // cloud
            ClickHouseTestHelpers.logAndThrowIfCloudPropNotExists(LOGGER, cloudProperties, ClickHouseTestHelpers.CLICKHOUSE_CLOUD_HOST_SYSTEM_PROP);
            ClickHouseTestHelpers.logAndThrowIfCloudPropNotExists(LOGGER, cloudProperties, ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PORT_SYSTEM_PROP);
            ClickHouseTestHelpers.logAndThrowIfCloudPropNotExists(LOGGER, cloudProperties, ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PASSWORD_SYSTEM_PROP);

            chc = new ClickHouseHelperClient.ClickHouseClientBuilder(
                    cloudProperties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_HOST_SYSTEM_PROP),
                    Integer.parseInt(cloudProperties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PORT_SYSTEM_PROP)),
                    ClickHouseProxyType.IGNORE, null, -1)
                    .setUsername(ClickHouseTestHelpers.USERNAME_DEFAULT)
                    .setPassword(cloudProperties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PASSWORD_SYSTEM_PROP))
                    .sslEnable(true)
                    .useClientV2(true)
                    .build();
            clickhouseAPI = new ClickHouseAPI(cloudProperties);
        }

        Network network = Network.newNetwork();
        List<String> connectorPath = new LinkedList<>();
        String confluentArchive = new File(Paths.get("build/confluentArchive").toString()).getAbsolutePath();
        connectorPath.add(confluentArchive);
        confluentPlatform = new ConfluentPlatform(network, connectorPath);
    }

    @AfterAll
    public static void tearDown() {
        if (confluentPlatform != null) {
            confluentPlatform.close();
        }
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void checkTotalsEqual(ClickHouseDeploymentType clusterConfig) throws InterruptedException, IOException {
        assertTrue(compareSchemalessCounts("singlePartitionTopic_" + clusterConfig, 1, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void checkTotalsEqualMulti(ClickHouseDeploymentType clusterConfig) throws InterruptedException, IOException {
        assertTrue(compareSchemalessCounts("multiPartitionTopic_" + clusterConfig, 3, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void checkSpottyNetwork(ClickHouseDeploymentType clusterConfig) throws InterruptedException, IOException, URISyntaxException {
        Assumptions.assumeFalse(isCluster,
                "checkSpottyNetwork requires ClickHouse Cloud API to stop/restart the service; not supported in cluster mode");
        checkSpottyNetworkSchemaless("checkSpottyNetworkSinglePartition_" + clusterConfig, 1, clusterConfig);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void checkSpottyNetworkMulti(ClickHouseDeploymentType clusterConfig) throws InterruptedException, IOException, URISyntaxException {
        Assumptions.assumeFalse(isCluster,
                "checkSpottyNetworkMulti requires ClickHouse Cloud API to stop/restart the service; not supported in cluster mode");
        checkSpottyNetworkSchemaless("checkSpottyNetworkMultiPartitions_" + clusterConfig, 3, clusterConfig);
    }

    private static void setupSchemaConnector(String topicName, int taskCount, ClickHouseDeploymentType clusterConfig) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector...");
        setupConnector("src/integrationTest/resources/clickhouse_sink_no_proxy.json", topicName, taskCount, clusterConfig);
        Thread.sleep(5 * 1000);
    }

    private static void setupSchemalessConnector(String topicName, int taskCount, ClickHouseDeploymentType clusterConfig) throws IOException, InterruptedException {
        LOGGER.info("Setting schemaless up connector...");
        setupConnector("src/integrationTest/resources/clickhouse_sink_no_proxy_schemaless.json", topicName, taskCount, clusterConfig);
        Thread.sleep(5 * 1000);
    }

    private static void setupConnector(String fileName, String topicName, int taskCount, ClickHouseDeploymentType clusterConfig) throws IOException {
        System.out.println("Setting up connector...");
        dropTable(chc, topicName, clusterConfig);
        new CreateTableStatement(STOCK_TABLE)
                .tableName(topicName).clusterConfig(clusterConfig).execute(chc);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get(fileName)));
        String jsonString;
        if (isCluster) {
            jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                    "host.docker.internal", ClickHouseCluster.getPort().toString(),
                    ClickHouseTestHelpers.DATABASE_DEFAULT,
                    ClickHouseTestHelpers.USERNAME_DEFAULT, "",
                    false, true); // ssl=false, exactlyOnce=true
        } else {
            jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                    cloudProperties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_HOST_SYSTEM_PROP),
                    cloudProperties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PORT_SYSTEM_PROP),
                    ClickHouseTestHelpers.DATABASE_DEFAULT,
                    ClickHouseTestHelpers.USERNAME_DEFAULT,
                    cloudProperties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PASSWORD_SYSTEM_PROP),
                    true, true); // ssl=true, exactlyOnce=true
        }

        confluentPlatform.createConnect(jsonString);
    }

    private int generateData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen.json", topicName, numberOfPartitions, numberOfRecords);
    }

    private int generateSchemalessData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen_json.json", topicName, numberOfPartitions, numberOfRecords);
    }

    private boolean compareSchemalessCounts(String topicName, int partitions, ClickHouseDeploymentType clusterConfig) throws InterruptedException, IOException {
        confluentPlatform.createTopic(topicName, partitions);
        int count = generateSchemalessData(topicName, partitions, 250);
        LOGGER.info("Expected Total: {}", count);
        setupSchemalessConnector(topicName, partitions, clusterConfig);
        ClickHouseAPI.waitWhileCounting(chc, topicName, 5, clusterConfig);

        int[] databaseCounts = ClickHouseAPI.getCounts(chc, topicName, clusterConfig);
        ClickHouseAPI.dropTable(chc, topicName, clusterConfig);
        return databaseCounts[2] == 0 && databaseCounts[1] == count;
    }

    private void checkSpottyNetworkSchemaless(String topicName, int numberOfPartitions, ClickHouseDeploymentType clusterConfig) throws InterruptedException, IOException, URISyntaxException {
        boolean allSuccess = true;
        int runCount = 1;
        do {
            LOGGER.info("Run: {}", runCount);
            confluentPlatform.createTopic(topicName, numberOfPartitions);

            int count = generateSchemalessData(topicName, numberOfPartitions, 1500);
            setupSchemalessConnector(topicName, numberOfPartitions, clusterConfig);

            clickhouseAPI.restartService();
            confluentPlatform.restartConnector(SINK_CONNECTOR_NAME);

            LOGGER.info("Expected Total: {}", count);
            ClickHouseAPI.waitWhileCounting(chc, topicName, 7, clusterConfig);

            int[] databaseCounts = ClickHouseAPI.getCounts(chc, topicName, clusterConfig);
            if (databaseCounts[2] != 0 || databaseCounts[1] != count) {
                allSuccess = false;
                LOGGER.error("Duplicates: {}", databaseCounts[2]);
                try (Records records = ClickHouseAPI.selectDuplicates(chc, topicName)) {
                    records.forEach(record -> LOGGER.error("Duplicate: {}", record));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
            confluentPlatform.deleteTopic(topicName);
            ClickHouseAPI.dropTable(chc, topicName, clusterConfig);
            runCount++;
        } while (runCount < 3 && allSuccess);

        assertTrue(allSuccess);
    }
}
