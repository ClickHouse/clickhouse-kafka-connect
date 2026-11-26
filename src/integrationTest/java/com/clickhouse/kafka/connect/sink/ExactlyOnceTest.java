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
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
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
    private static ClickHouseHelperClient chcNoProxy;
    private static final Properties cloudProperties = System.getProperties();
    private static final String SINK_CONNECTOR_NAME = "ClickHouseSinkConnector";
    private static final boolean isCluster = ClickHouseTestHelpers.isCluster();
    private static final boolean isCloud = ClickHouseTestHelpers.isCloud();
    private static final Supplier<CreateTableStatement> STOCK_TABLE = () -> CreateTableStatement.create()
            .column("side", "String")
            .column("quantity", "Int32")
            .column("symbol", "String")
            .column("price", "Int32")
            .column("account", "String")
            .column("userid", "String")
            .column("kafka_partition", "Int32")
            .column("kafka_offset", "Int64")
            .column("insertTime", "DateTime DEFAULT now()")
            .orderByColumn("symbol");
    private static final String SCHEMALESS_SINK_CONFIG = "src/integrationTest/resources/clickhouse_sink_no_proxy_schemaless.json";
    private static final String SCHEMALESS_SINK_CONFIG_CLUSTER = "src/integrationTest/resources/clickhouse_sink_no_proxy_schemaless_cluster.json";
    private static final String SCHEMA_SINK_CONFIG = "src/integrationTest/resources/clickhouse_sink_no_proxy.json";
    private static final String SCHEMA_SINK_CONFIG_CLUSTER = "src/integrationTest/resources/clickhouse_sink_no_proxy_cluster.json";
    private static final String BUFFER_CONFIG_SUFFIX = "_buffer.json";
    // On a cluster the buffered configs connect through Toxiproxy so the spotty-network tests can cut the connection.
    private static final String BUFFER_CLUSTER_CONFIG_SUFFIX = "_buffer_cluster.json";
    private static final int PROXY_PORT = 8666;
    // Slows each request so the connection is cut while the drain is still in progress.
    private static final int SPOTTY_NETWORK_LATENCY_MS = 500;
    private static final long SPOTTY_NETWORK_OUTAGE_MS = 20_000;
    private static final long SPOTTY_NETWORK_RECOVERY_MS = 20_000;
    private static ToxiproxyContainer toxiproxy;
    private static Proxy clickhouseProxy;

    private static Map<String, String> getTestProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConfig.PROXY_TYPE, "IGNORE");
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, ClickHouseTestHelpers.extractClientVersion());
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
    public static void checkPropsExistAndSetUp() throws IOException {
        Assumptions.assumeTrue(isCluster || isCloud, "ExactlyOnceTest in not supported against standalone");
        Assertions.assertFalse(isCluster && isCloud, String.format("Invalid configuration: both %s=<> and %s=cloud are set. Please set only one or the other.", ClickHouseTestHelpers.CLICKHOUSE_CLUSTER_NAME, ClickHouseTestHelpers.CLICKHOUSE_VERSION));

        if (isCloud) {
            ClickHouseTestHelpers.logAndThrowIfCloudPropNotExists(LOGGER, cloudProperties, ClickHouseTestHelpers.CLICKHOUSE_CLOUD_HOST_SYSTEM_PROP);
            ClickHouseTestHelpers.logAndThrowIfCloudPropNotExists(LOGGER, cloudProperties, ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PORT_SYSTEM_PROP);
            ClickHouseTestHelpers.logAndThrowIfCloudPropNotExists(LOGGER, cloudProperties, ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PASSWORD_SYSTEM_PROP);
            clickhouseCloudAPI = new ClickHouseCloudAPI(cloudProperties);
            if (clickhouseCloudAPI.isConfigured()) {
                clickhouseCloudAPI.ensureServiceRunning();
            } else {
                LOGGER.warn("Cloud API properties are not set; not checking that the service is running (the spotty-network tests need them to restart the service)");
            }
        } else {
            cluster = ClickHouseCluster.getClusterFromEnvVarOrThrow();
            cluster.start();
        }

        chcNoProxy = ClickHouseTestHelpers.createClient(getTestProperties());
        Network network = Network.newNetwork();
        List<String> connectorPath = new LinkedList<>();
        String confluentArchive = new File(Paths.get("build/confluentArchive").toString()).getAbsolutePath();
        connectorPath.add(confluentArchive);
        confluentPlatform = new ConfluentPlatform(network, connectorPath);

        if (isCluster) {
            toxiproxy = new ToxiproxyContainer(ClickHouseTestHelpers.TOXIPROXY_DOCKER_IMAGE_NAME)
                    .withNetwork(network)
                    .withNetworkAliases(ClickHouseTestHelpers.TOXIPROXY_NETWORK_ALIAS)
                    .withExtraHost("host.docker.internal", "host-gateway");
            toxiproxy.start();
            clickhouseProxy = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort())
                    .createProxy("clickhouse-proxy", "0.0.0.0:" + PROXY_PORT, "host.docker.internal:" + cluster.getPort());
        }
    }

    @AfterAll
    public static void tearDown() {
        if (confluentPlatform != null) {
            confluentPlatform.close();
        }
        if (toxiproxy != null) {
            toxiproxy.stop();
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
    }

    /**
     * Schemaless config pair: existing EO config + buffering variant from PR #736.
     */
    private static Stream<String> schemalessConfigs() {
        return Stream.of(
                "src/integrationTest/resources/clickhouse_sink_no_proxy_schemaless.json",
                "src/integrationTest/resources/clickhouse_sink_no_proxy_schemaless_buffer.json"
        );
    }

    /**
     * Schema (Avro) config pair: existing EO config + buffering variant from PR #736.
     */
    private static Stream<String> schemaConfigs() {
        return Stream.of(
                "src/integrationTest/resources/clickhouse_sink_no_proxy.json",
                "src/integrationTest/resources/clickhouse_sink_no_proxy_buffer.json"
        );
    }

    @ParameterizedTest(name = "config={0}")
    @MethodSource("schemalessConfigs")
    public void checkTotalsEqual(String configPath) throws InterruptedException, IOException {
        assertTrue(compareSchemalessCounts("singlePartitionTopic", 1, configPath));
    }

    @ParameterizedTest(name = "config={0}")
    @MethodSource("schemalessConfigs")
    public void checkTotalsEqualMulti(String configPath) throws InterruptedException, IOException {
        assertTrue(compareSchemalessCounts("multiPartitionTopic", 3, configPath));
    }

    @ParameterizedTest(name = "config={0}")
    @MethodSource("schemalessConfigs")
    public void checkSpottyNetwork(String configPath) throws InterruptedException, IOException, URISyntaxException {
        assumeSpottyNetworkSupported(configPath);
        checkSpottyNetworkSchemaless("checkSpottyNetworkSinglePartition", 1, configPath);
    }

    @ParameterizedTest(name = "config={0}")
    @MethodSource("schemalessConfigs")
    public void checkSpottyNetworkMulti(String configPath) throws InterruptedException, IOException, URISyntaxException {
        assumeSpottyNetworkSupported(configPath);
        checkSpottyNetworkSchemaless("checkSpottyNetworkMultiPartitions", 3, configPath);
    }

    @ParameterizedTest(name = "config={0}")
    @MethodSource("schemaConfigs")
    public void checkTotalsEqualSchema(String configPath) throws InterruptedException, IOException {
        assertTrue(compareSchemaCounts("singlePartitionTopicSchema", 1, configPath));
    }

    @ParameterizedTest(name = "config={0}")
    @MethodSource("schemaConfigs")
    public void checkTotalsEqualSchemaMulti(String configPath) throws InterruptedException, IOException {
        assertTrue(compareSchemaCounts("multiPartitionTopicSchema", 3, configPath));
    }

    @ParameterizedTest(name = "config={0}")
    @MethodSource("schemaConfigs")
    public void checkSpottyNetworkSchema(String configPath) throws InterruptedException, IOException, URISyntaxException {
        assumeSpottyNetworkSupported(configPath);
        runSpottyNetworkSchema("checkSpottyNetworkSinglePartitionSchema", 1, configPath);
    }

    @ParameterizedTest(name = "config={0}")
    @MethodSource("schemaConfigs")
    public void checkSpottyNetworkSchemaMulti(String configPath) throws InterruptedException, IOException, URISyntaxException {
        assumeSpottyNetworkSupported(configPath);
        runSpottyNetworkSchema("checkSpottyNetworkMultiPartitionsSchema", 3, configPath);
    }

    private static void setupSchemaConnector(String topicName, int taskCount, String configPath)
            throws IOException, InterruptedException {
        LOGGER.info("Setting schema up connector with config {}", configPath);
        setupConnector(topicName, taskCount, false, configPath);
        Thread.sleep(5 * 1000);
    }

    private static void setupSchemalessConnector(String topicName, int taskCount, String configPath)
            throws IOException, InterruptedException {
        LOGGER.info("Setting schemaless up connector with config {}", configPath);
        setupConnector(topicName, taskCount, true, configPath);
        Thread.sleep(5 * 1000);
    }

    private static void setupConnector(String topicName, int taskCount, boolean schemaless) throws IOException {
        setupConnector(topicName, taskCount, schemaless, null);
    }

    private static void setupConnector(String topicName, int taskCount, boolean schemaless, String configOverride) throws IOException {
        System.out.println("Setting up connector...");
        ClickHouseTestHelpers.dropTable(chcNoProxy, topicName);
        STOCK_TABLE.get()
                .tableName(topicName).execute(chcNoProxy);

        boolean bufferedOnCluster = isCluster && configOverride != null && isBufferConfig(configOverride);
        String fileName;
        if (bufferedOnCluster) {
            fileName = configOverride.replace(BUFFER_CONFIG_SUFFIX, BUFFER_CLUSTER_CONFIG_SUFFIX);
        } else if (isCluster) {
            fileName = schemaless ? SCHEMALESS_SINK_CONFIG_CLUSTER : SCHEMA_SINK_CONFIG_CLUSTER;
        } else if (configOverride != null) {
            fileName = configOverride;
        } else {
            fileName = schemaless ? SCHEMALESS_SINK_CONFIG : SCHEMA_SINK_CONFIG;
        }

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get(fileName)));
        String jsonString;
        if (isCluster) {
            jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                    bufferedOnCluster ? ClickHouseTestHelpers.TOXIPROXY_NETWORK_ALIAS : "host.docker.internal",
                    bufferedOnCluster ? String.valueOf(PROXY_PORT) : cluster.getPort().toString(),
                    chcNoProxy.getDatabase(),
                    chcNoProxy.getUsername(),
                    chcNoProxy.getPassword(),
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

        confluentPlatform.createConnectorAndWaitUntilRunning(SINK_CONNECTOR_NAME, jsonString);
    }

    private int generateData(String topicName, int numberOfPartitions, int numberOfRecords, boolean evenPartitions) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen.json", topicName, numberOfPartitions, numberOfRecords, evenPartitions);
    }

    private int generateSchemalessData(String topicName, int numberOfPartitions, int numberOfRecords, boolean evenPartitions) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen_json.json", topicName, numberOfPartitions, numberOfRecords, evenPartitions);
    }

    private boolean compareSchemalessCounts(String topicName, int partitions, String configPath)
            throws InterruptedException, IOException {
        return compareCounts(topicName, partitions, configPath, /* schema */ false);
    }

    private boolean compareSchemaCounts(String topicName, int partitions, String configPath)
            throws InterruptedException, IOException {
        return compareCounts(topicName, partitions, configPath, /* schema */ true);
    }

    /**
     * Each config parameter needs its own topic: the expected count is the topic's end offset, while the
     * connector (same name, so same consumer group and exactly-once Keeper state) resumes after the previous
     * parameter's records. A shared topic made every second parameter expect twice the rows it could write.
     */
    private static String topicForConfig(String topicName, String configPath) {
        return topicName + "_" + Paths.get(configPath).getFileName().toString().replace(".json", "");
    }

    private static boolean isBufferConfig(String configPath) {
        return configPath.endsWith(BUFFER_CONFIG_SUFFIX);
    }

    /**
     * Cloud restarts the service through the Cloud API. On a cluster only the buffered configs run: they connect
     * through Toxiproxy, which cuts the connection instead.
     */
    private static void assumeSpottyNetworkSupported(String configPath) {
        Assumptions.assumeFalse(isCluster && !isBufferConfig(configPath),
                "On a cluster the spotty-network tests only run the buffered configs (through Toxiproxy)");
    }

    /**
     * Makes ClickHouse unreachable for the connector mid-drain, then reachable again. With retryOnSocketException the
     * failed flushes are retriable, so Kafka Connect keeps redelivering the same batch to the running task. The
     * connector is restarted only after it had time to flush again, as after a Cloud service restart.
     */
    private static void interruptClusterConnection() throws IOException, InterruptedException {
        clickhouseProxy.disable();
        try {
            Thread.sleep(SPOTTY_NETWORK_OUTAGE_MS);
        } finally {
            clickhouseProxy.enable();
            removeToxics();
        }
        Thread.sleep(SPOTTY_NETWORK_RECOVERY_MS);
    }

    private static void removeToxics() throws IOException {
        for (var toxic : clickhouseProxy.toxics().getAll()) {
            toxic.remove();
        }
    }

    private boolean compareCounts(String baseTopicName, int partitions, String configPath, boolean schema)
            throws InterruptedException, IOException {
        String topicName = topicForConfig(baseTopicName, configPath);
        STOCK_TABLE.get() // implicitly SharedMergeTree in CH Cloud
                .tableName(topicName).ifNotExists(true).execute(chcNoProxy);
        ClickHouseTestHelpers.clearTable(chcNoProxy, topicName);
        confluentPlatform.createTopic(topicName, partitions);
        // Buffered exactly-once flushes each partition only in full bufferCount (100) chunks and keeps the rest.
        // For buffered configs, spread records evenly over the partitions; 300 per datagen task splits evenly over
        // 1 or 3 partitions, so every partition ends on a full chunk.
        boolean buffered = isBufferConfig(configPath);
        int count = schema ? generateData(topicName, partitions, 300, buffered)
                : generateSchemalessData(topicName, partitions, 300, buffered);
        LOGGER.info("Expected Total: {}", count);
        if (schema) {
            setupSchemaConnector(topicName, partitions, configPath);
        } else {
            setupSchemalessConnector(topicName, partitions, configPath);
        }
        ClickHouseTestHelpers.waitWhileCounting(chcNoProxy, topicName, 5);

        int[] databaseCounts = getCounts(chcNoProxy, topicName);
        ClickHouseTestHelpers.dropTable(chcNoProxy, topicName);
        assertTrue(databaseCounts[2] == 0 && databaseCounts[1] == count, String.format("%s: expected %d Kafka records exactly once, "
                + "ClickHouse has total=%d distinct records=%d duplicates=%d", topicName, count, databaseCounts[0], databaseCounts[1], databaseCounts[2]));
        return true;
    }

    private void checkSpottyNetworkSchemaless(String topicName, int numberOfPartitions, String configPath)
            throws InterruptedException, IOException, URISyntaxException {
        runSpottyNetwork(topicName, numberOfPartitions, configPath, /* schema */ false);
    }

    private void runSpottyNetworkSchema(String topicName, int numberOfPartitions, String configPath)
            throws InterruptedException, IOException, URISyntaxException {
        runSpottyNetwork(topicName, numberOfPartitions, configPath, /* schema */ true);
    }

    private void runSpottyNetwork(String baseTopicName, int numberOfPartitions, String configPath, boolean schema)
            throws InterruptedException, IOException, URISyntaxException {
        String topicName = topicForConfig(baseTopicName, configPath);
        boolean allSuccess = true;
        String failure = null;
        int runCount = 1;
        do {
            LOGGER.info("Run: {}", runCount);
            confluentPlatform.createTopic(topicName, numberOfPartitions);

            int count = schema ? generateData(topicName, numberOfPartitions, 1500, isBufferConfig(configPath))
                    : generateSchemalessData(topicName, numberOfPartitions, 1500, isBufferConfig(configPath));
            if (isCluster) {
                removeToxics();
                clickhouseProxy.toxics().latency("slow-drain", ToxicDirection.DOWNSTREAM, SPOTTY_NETWORK_LATENCY_MS);
            }
            if (schema) {
                setupSchemaConnector(topicName, numberOfPartitions, configPath);
            } else {
                setupSchemalessConnector(topicName, numberOfPartitions, configPath);
            }

            if (isCluster) {
                interruptClusterConnection();
            } else {
                clickhouseCloudAPI.restartService();
            }
            confluentPlatform.restartConnector(SINK_CONNECTOR_NAME);

            LOGGER.info("Expected Total: {}", count);
            ClickHouseTestHelpers.waitWhileCounting(chcNoProxy, topicName, 7);

            int[] databaseCounts = getCounts(chcNoProxy, topicName);
            if (databaseCounts[2] != 0 || databaseCounts[1] != count) {
                allSuccess = false;
                failure = String.format("%s run %d: expected %d Kafka records exactly once, ClickHouse has total=%d distinct records=%d duplicates=%d",
                        topicName, runCount, count, databaseCounts[0], databaseCounts[1], databaseCounts[2]);
                LOGGER.error(failure);
                try (Records records = selectDuplicates(chcNoProxy, topicName)) {
                    records.forEach(record -> LOGGER.error("Duplicate: partition={} offset={} copies={}",
                            record.getString(1), record.getString(2), record.getString(3)));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
            confluentPlatform.deleteTopic(topicName);
            ClickHouseTestHelpers.dropTable(chcNoProxy, topicName);
            runCount++;
        } while (runCount < 3 && allSuccess);

        assertTrue(allSuccess, failure);
    }

    private static Records selectDuplicates(ClickHouseHelperClient chc, String tableName) {
        String queryString = "SELECT `kafka_partition`, `kafka_offset`, COUNT(*) FROM " + tableName +
                " GROUP BY `kafka_partition`, `kafka_offset` HAVING COUNT(*) > 1";
        return chc.queryV2(queryString);
    }

    private static int[] getCounts(ClickHouseHelperClient chc, String tableName) {
        String from = ClickHouseTestHelpers.buildFromClause(chc, tableName);
        String queryCount = "SELECT count(*) as total, uniqExact(kafka_partition, kafka_offset) as uniqueTotal, total - uniqueTotal FROM " + from + " SETTINGS select_sequential_consistency = 1";
        try (Records records = chc.queryV2(queryCount)) {
            GenericRecord first = StreamSupport.stream(records.spliterator(), false).findFirst().orElseThrow();
            return Stream.of(first.getInteger(1), first.getInteger(2), first.getInteger(3)).mapToInt(Integer::intValue).toArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
