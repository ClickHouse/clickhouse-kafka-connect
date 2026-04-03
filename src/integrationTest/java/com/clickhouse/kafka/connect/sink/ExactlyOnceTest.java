package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.query.Records;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseCloudAPI;
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

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NOTE: this test does NOT run against standalone ClickHouse
 */
public class ExactlyOnceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExactlyOnceTest.class);
    public static ConfluentPlatform confluentPlatform;
    private static ClickHouseCloudAPI clickhouseAPI;
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

    public static Stream<ClickHouseDeploymentType> deploymentTypesForTests() {
        return ClickHouseTestHelpers.deploymentTypesForTests();
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
            clickhouseAPI = new ClickHouseCloudAPI(cloudProperties);
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
    @MethodSource("deploymentTypesForTests")
    public void checkTotalsEqual(ClickHouseDeploymentType deploymentType) throws InterruptedException, IOException {
        assertTrue(compareSchemalessCounts("singlePartitionTopic_" + deploymentType, 1, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void checkTotalsEqualMulti(ClickHouseDeploymentType deploymentType) throws InterruptedException, IOException {
        assertTrue(compareSchemalessCounts("multiPartitionTopic_" + deploymentType, 3, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void checkSpottyNetwork(ClickHouseDeploymentType deploymentType) throws InterruptedException, IOException, URISyntaxException {
        Assumptions.assumeFalse(isCluster,
                "checkSpottyNetwork requires ClickHouse Cloud API to stop/restart the service; not supported in cluster mode");
        checkSpottyNetworkSchemaless("checkSpottyNetworkSinglePartition_" + deploymentType, 1, deploymentType);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void checkSpottyNetworkMulti(ClickHouseDeploymentType deploymentType) throws InterruptedException, IOException, URISyntaxException {
        Assumptions.assumeFalse(isCluster,
                "checkSpottyNetworkMulti requires ClickHouse Cloud API to stop/restart the service; not supported in cluster mode");
        checkSpottyNetworkSchemaless("checkSpottyNetworkMultiPartitions_" + deploymentType, 3, deploymentType);
    }

    private static void setupSchemaConnector(String topicName, int taskCount, ClickHouseDeploymentType deploymentType) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector...");
        setupConnector("src/integrationTest/resources/clickhouse_sink_no_proxy.json", topicName, taskCount, deploymentType);
        Thread.sleep(5 * 1000);
    }

    private static void setupSchemalessConnector(String topicName, int taskCount, ClickHouseDeploymentType deploymentType) throws IOException, InterruptedException {
        LOGGER.info("Setting schemaless up connector...");
        setupConnector("src/integrationTest/resources/clickhouse_sink_no_proxy_schemaless.json", topicName, taskCount, deploymentType);
        Thread.sleep(5 * 1000);
    }

    private static void setupConnector(String fileName, String topicName, int taskCount, ClickHouseDeploymentType deploymentType) throws IOException {
        System.out.println("Setting up connector...");
        ClickHouseTestHelpers.dropTable(chc, topicName, deploymentType);
        new CreateTableStatement(STOCK_TABLE)
                .tableName(topicName).deploymentType(deploymentType).execute(chc);

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

    private boolean compareSchemalessCounts(String topicName, int partitions, ClickHouseDeploymentType deploymentType) throws InterruptedException, IOException {
        confluentPlatform.createTopic(topicName, partitions);
        int count = generateSchemalessData(topicName, partitions, 250);
        LOGGER.info("Expected Total: {}", count);
        setupSchemalessConnector(topicName, partitions, deploymentType);
        ClickHouseTestHelpers.waitWhileCounting(chc, topicName, 5, deploymentType);

        int[] databaseCounts = getCounts(chc, topicName, deploymentType);
        ClickHouseTestHelpers.dropTable(chc, topicName, deploymentType);
        return databaseCounts[2] == 0 && databaseCounts[1] == count;
    }

    private static int[] getCounts(ClickHouseHelperClient chc, String tableName, ClickHouseDeploymentType deploymentType) {
        String from = ClickHouseTestHelpers.buildFromClause(chc, tableName, deploymentType);
        String queryCount = "SELECT count(*) as total, uniqExact(*) as uniqueTotal, total - uniqueTotal FROM " + from + " SETTINGS select_sequential_consistency = 1";
        try (Records records = chc.queryV2(queryCount)) {
            var record = records.iterator().next();
            int total = Integer.parseInt(record.getString(1));
            int unique = Integer.parseInt(record.getString(2));
            return new int[]{total, unique, total - unique};
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Records selectDuplicates(ClickHouseHelperClient chc, String tableName) {
        String queryString = String.format("SELECT `side`, `quantity`, `symbol`, `price`, `account`, `userid`, `insertTime`, COUNT(*) " +
                "FROM %s " +
                "GROUP BY `side`, `quantity`, `symbol`, `price`, `account`, `userid`, `insertTime` " +
                "HAVING COUNT(*) > 1", tableName);
        return chc.queryV2(queryString);
    }

    private void checkSpottyNetworkSchemaless(String topicName, int numberOfPartitions, ClickHouseDeploymentType deploymentType) throws InterruptedException, IOException, URISyntaxException {
        boolean allSuccess = true;
        int runCount = 1;
        do {
            LOGGER.info("Run: {}", runCount);
            confluentPlatform.createTopic(topicName, numberOfPartitions);

            int count = generateSchemalessData(topicName, numberOfPartitions, 1500);
            setupSchemalessConnector(topicName, numberOfPartitions, deploymentType);

            clickhouseAPI.restartService();
            confluentPlatform.restartConnector(SINK_CONNECTOR_NAME);

            LOGGER.info("Expected Total: {}", count);
            ClickHouseTestHelpers.waitWhileCounting(chc, topicName, 7, deploymentType);

            int[] databaseCounts = getCounts(chc, topicName, deploymentType);
            if (databaseCounts[2] != 0 || databaseCounts[1] != count) {
                allSuccess = false;
                LOGGER.error("Duplicates: {}", databaseCounts[2]);
                try (Records records = selectDuplicates(chc, topicName)) {
                    records.forEach(record -> LOGGER.error("Duplicate: {}", record));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
            confluentPlatform.deleteTopic(topicName);
            ClickHouseTestHelpers.dropTable(chc, topicName, deploymentType);
            runCount++;
        } while (runCount < 3 && allSuccess);

        assertTrue(allSuccess);
    }
}
