package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseCloudAPI;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseCluster;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.ConfluentPlatform;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NOTE: this test does NOT run against standalone ClickHouse
 */
public class ExactlyOnceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExactlyOnceTest.class);
    public static ConfluentPlatform confluentPlatform;
    private static ClickHouseCloudAPI clickhouseCloudAPI;
    private static ClickHouseCluster cluster;
    private static ClickHouseHelperClient chc;
    private static final Properties cloudProperties = System.getProperties();
    private static final String SINK_CONNECTOR_NAME = "ClickHouseSinkConnector";
    private static final boolean isCluster = ClickHouseTestHelpers.isCluster();
    private static final boolean isCloud = ClickHouseTestHelpers.isCloud();
    private static final CreateTableStatement STOCK_TABLE = new CreateTableStatement()
            .column("side", "String")
            .column("quantity", "Int32")
            .column("symbol", "String")
            .column("price", "Int32")
            .column("account", "String")
            .column("userid", "String")
            .column("insertTime", "DateTime DEFAULT now()")
            .orderByColumn("symbol");
    private static final String SCHEMALESS_SINK_CONFIG = "src/integrationTest/resources/clickhouse_sink_no_proxy_schemaless.json";
    private static final String SCHEMALESS_SINK_CONFIG_CLUSTER = "src/integrationTest/resources/clickhouse_sink_no_proxy_schemaless_cluster.json";
    private static final String SCHEMA_SINK_CONFIG = "src/integrationTest/resources/clickhouse_sink_no_proxy.json";
    private static final String SCHEMA_SINK_CONFIG_CLUSTER = "src/integrationTest/resources/clickhouse_sink_no_proxy_cluster.json";

    private static Map<String, String> getTestProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConfig.PROXY_TYPE, "IGNORE");
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, "V2");
        if (isCluster) {
            props.putAll(cluster.getClusterProps(ClickHouseTestHelpers.DATABASE_DEFAULT));
        } else {
            // cloud
            props.putAll(Map.of(
                    ClickHouseSinkConnector.HOSTNAME, cloudProperties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_HOST_SYSTEM_PROP),
                    ClickHouseSinkConnector.PORT, cloudProperties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PORT_SYSTEM_PROP),
                    ClickHouseSinkConnector.DATABASE, ClickHouseTestHelpers.DATABASE_DEFAULT,
                    ClickHouseSinkConnector.USERNAME, ClickHouseTestHelpers.USERNAME_DEFAULT,
                    ClickHouseSinkConnector.PASSWORD, cloudProperties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PASSWORD_SYSTEM_PROP),
                    ClickHouseSinkConnector.SSL_ENABLED, "true"
            ));
        }
        return props;
    }

    @BeforeAll
    public static void checkPropsExistAndSetUp() {
        Assumptions.assumeTrue(isCluster || isCloud, "ExactlyOnceTest in not supported against standalone");
        if (isCloud) {
            ClickHouseTestHelpers.logAndThrowIfCloudPropNotExists(LOGGER, cloudProperties, ClickHouseTestHelpers.CLICKHOUSE_CLOUD_HOST_SYSTEM_PROP);
            ClickHouseTestHelpers.logAndThrowIfCloudPropNotExists(LOGGER, cloudProperties, ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PORT_SYSTEM_PROP);
            ClickHouseTestHelpers.logAndThrowIfCloudPropNotExists(LOGGER, cloudProperties, ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PASSWORD_SYSTEM_PROP);
            clickhouseCloudAPI = new ClickHouseCloudAPI(cloudProperties);
        } else {
            cluster = ClickHouseCluster.getClusterFromEnvVarOrThrow();
            cluster.start();
        }

        chc = ClickHouseTestHelpers.createClient(getTestProperties());
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
        if (cluster != null) {
            cluster.stop();
        }
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
    }

    @Test
    public void checkTotalsEqual() throws InterruptedException, IOException {
        assertTrue(compareSchemalessCounts("singlePartitionTopic", 1));
    }

    @Test
    public void checkTotalsEqualMulti() throws InterruptedException, IOException {
        assertTrue(compareSchemalessCounts("multiPartitionTopic", 3));
    }

    @Test
    public void checkSpottyNetwork() throws InterruptedException, IOException, URISyntaxException {
        Assumptions.assumeFalse(isCluster,
                "checkSpottyNetwork requires ClickHouse Cloud API to stop/restart the service; not supported in cluster mode");
        checkSpottyNetworkSchemaless("checkSpottyNetworkSinglePartition", 1);
    }

    @Test
    public void checkSpottyNetworkMulti() throws InterruptedException, IOException, URISyntaxException {
        Assumptions.assumeFalse(isCluster,
                "checkSpottyNetworkMulti requires ClickHouse Cloud API to stop/restart the service; not supported in cluster mode");
        checkSpottyNetworkSchemaless("checkSpottyNetworkMultiPartitions", 3);
    }

    private static void setupSchemaConnector(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector...");
        setupConnector(topicName, taskCount, false);
        Thread.sleep(5 * 1000);
    }

    private static void setupSchemalessConnector(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting schemaless up connector...");
        setupConnector(topicName, taskCount, true);
        Thread.sleep(5 * 1000);
    }

    private static void setupConnector(String topicName, int taskCount, boolean schemaless) throws IOException {
        System.out.println("Setting up connector...");
        ClickHouseTestHelpers.dropTable(chc, topicName);
        new CreateTableStatement(STOCK_TABLE)
                .tableName(topicName).execute(chc);

        String fileName;
        if (isCloud) {
            fileName = schemaless ? SCHEMALESS_SINK_CONFIG : SCHEMA_SINK_CONFIG;
        } else {
            fileName = schemaless ? SCHEMALESS_SINK_CONFIG_CLUSTER : SCHEMA_SINK_CONFIG_CLUSTER;
        }

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get(fileName)));
        String jsonString;
        if (isCluster) {
            jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                    "host.docker.internal", cluster.getPort().toString(),
                    chc.getDatabase(),
                    chc.getUsername(),
                    chc.getPassword(),
                    false, true, // ssl=false, exactlyOnce=true
                    ClickHouseCluster.getClusterFromEnvVarOrThrow().getName() // keeperOnCluster=<clusterName>
            );
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

    private boolean compareSchemalessCounts(String topicName, int partitions) throws InterruptedException, IOException {
        confluentPlatform.createTopic(topicName, partitions);
        int count = generateSchemalessData(topicName, partitions, 250);
        LOGGER.info("Expected Total: {}", count);
        setupSchemalessConnector(topicName, partitions);
        ClickHouseTestHelpers.waitWhileCounting(chc, topicName, 5);

        int[] databaseCounts = getCounts(chc, topicName);
        ClickHouseTestHelpers.dropTable(chc, topicName);
        return databaseCounts[2] == 0 && databaseCounts[1] == count;
    }

    private void checkSpottyNetworkSchemaless(String topicName, int numberOfPartitions) throws InterruptedException, IOException, URISyntaxException {
        boolean allSuccess = true;
        int runCount = 1;
        do {
            LOGGER.info("Run: {}", runCount);
            confluentPlatform.createTopic(topicName, numberOfPartitions);

            int count = generateSchemalessData(topicName, numberOfPartitions, 1500);
            setupSchemalessConnector(topicName, numberOfPartitions);

            clickhouseCloudAPI.restartService();
            confluentPlatform.restartConnector(SINK_CONNECTOR_NAME);

            LOGGER.info("Expected Total: {}", count);
            ClickHouseTestHelpers.waitWhileCounting(chc, topicName, 7);

            int[] databaseCounts = getCounts(chc, topicName);
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
            ClickHouseTestHelpers.dropTable(chc, topicName);
            runCount++;
        } while (runCount < 3 && allSuccess);

        assertTrue(allSuccess);
    }

    private static Records selectDuplicates(ClickHouseHelperClient chc, String tableName) {
        String queryString = "SELECT `side`, `quantity`, `symbol`, `price`, `account`, `userid`, `insertTime`, COUNT(*) FROM " + tableName +
                " GROUP BY `side`, `quantity`, `symbol`, `price`, `account`, `userid`, `insertTime` HAVING COUNT(*) > 1";
        return chc.queryV2(queryString);
    }

    private static int[] getCounts(ClickHouseHelperClient chc, String tableName) {
        String from = ClickHouseTestHelpers.buildFromClause(chc, tableName);
        String queryCount = "SELECT count(*) as total, uniqExact(*) as uniqueTotal, total - uniqueTotal FROM " + from + " SETTINGS select_sequential_consistency = 1";
        try (Records records = chc.queryV2(queryCount)) {
            GenericRecord first = StreamSupport.stream(records.spliterator(), false).findFirst().orElseThrow();
            return Stream.of(first.getInteger(1), first.getInteger(2), first.getInteger(3)).mapToInt(Integer::intValue).toArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
