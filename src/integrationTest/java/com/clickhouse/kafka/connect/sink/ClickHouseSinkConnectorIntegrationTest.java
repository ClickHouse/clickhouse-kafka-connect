package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseCluster;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseDeploymentType;
import com.clickhouse.kafka.connect.sink.helper.ConfluentPlatform;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <pre>
 * NOTE 1: this test does NOT run against ClickHouse cloud
 * </pre>
 * <pre>
 * NOTE 2: this test explicitly connects to the proxy endpoint and avoids setting PROXY_HOST/PROXY_PORT
 * because the client makes requests with absolute URI's to the server when the proxy config is set.
 * TODO: Once <a href="https://github.com/ClickHouse/ClickHouse/issues/58828">this issue</a> is fixed, we can revert this test to use the client proxy config.
 * </pre>
 */
public class ClickHouseSinkConnectorIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkConnectorIntegrationTest.class);
    public static ConfluentPlatform confluentPlatform;
    private static ClickHouseContainer db;
    private static ClickHouseHelperClient chc;
    public static ToxiproxyContainer toxiproxy;
    public static Proxy proxy;
    private static final int PROXY_PORT = 8666;
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

    public static Stream<ClickHouseDeploymentType> deploymentTypesForTests() {
        if (isCluster) {
            return Stream.of(ClickHouseDeploymentType.THREE_SHARDS_ONE_REPLICA_EACH, ClickHouseDeploymentType.ONE_SHARD_THREE_REPLICAS);
        }
        return Stream.of(ClickHouseDeploymentType.STANDALONE);
    }

    @BeforeAll
    public static void setup() throws IOException {
        Assumptions.assumeFalse(isCloud, "ClickHouseSinkConnectorIntegrationTest is not supported against cloud");
        Network network = Network.newNetwork();
        List<String> connectorPath = new LinkedList<>();
        String confluentArchive = new File(Paths.get("build/confluentArchive").toString()).getAbsolutePath();
        connectorPath.add(confluentArchive);
        confluentPlatform = new ConfluentPlatform(network, connectorPath);

        if (!isCluster) {
            db = new ClickHouseContainer(ClickHouseTestHelpers.CLICKHOUSE_DOCKER_IMAGE)
                    .withNetwork(network)
                    .withNetworkAliases(ClickHouseTestHelpers.CLICKHOUSE_DB_NETWORK_ALIAS);
            db.start();
        }

        toxiproxy = new ToxiproxyContainer(ClickHouseTestHelpers.TOXIPROXY_DOCKER_IMAGE_NAME)
                .withNetwork(network)
                .withNetworkAliases(ClickHouseTestHelpers.TOXIPROXY_NETWORK_ALIAS);
        if (isCluster) {
            toxiproxy = toxiproxy.withExtraHost("host.docker.internal", "host-gateway");
        }
        toxiproxy.start();

        LOGGER.info("Started proxy container: {}", toxiproxy.getControlPort());
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());

        String upstream;
        if (isCluster) {
            upstream = String.format("host.docker.internal:%d", ClickHouseCluster.getPort());
        } else {
            upstream = String.format("%s:%d", ClickHouseTestHelpers.CLICKHOUSE_DB_NETWORK_ALIAS, ClickHouseProtocol.HTTP.getDefaultPort());
        }
        proxy = toxiproxyClient.createProxy("clickhouse-proxy", "0.0.0.0:" + PROXY_PORT, upstream);
        LOGGER.info("Proxy configured {}", proxy.getListen());
        chc = ClickHouseTestHelpers.createClient(getTestProperties());
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
    }

    @AfterAll
    public static void tearDown() {
        if (!isCluster) {
            db.stop();
        }
        toxiproxy.stop();
        confluentPlatform.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void stockGenSingleTaskTest(ClickHouseDeploymentType deploymentType) throws IOException, InterruptedException {
        String topicName = "stockGenSingleTaskTest_" + deploymentType;
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateData(topicName, 1, 100);
        setupConnector(topicName, 1, deploymentType);
        ClickHouseTestHelpers.waitWhileCounting(chc, topicName, 3, deploymentType);
        assertTrue(dataCount <= ClickHouseTestHelpers.countRows(chc, topicName, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void stockGenWithJdbcPropSingleTaskTest(ClickHouseDeploymentType deploymentType) throws IOException, InterruptedException {
        String topicName = "stockGenWithJdbcPropSingleTaskTest_" + deploymentType;
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateData(topicName, 1, 100);
        setupConnectorWithJdbcProperties(topicName, 1, deploymentType);
        ClickHouseTestHelpers.waitWhileCounting(chc, topicName, 3, deploymentType);
        assertTrue(dataCount <= ClickHouseTestHelpers.countRows(chc, topicName, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void stockGenSingleTaskSchemalessTest(ClickHouseDeploymentType deploymentType) throws IOException, InterruptedException {
        String topicName = "stockGenSingleTaskSchemalessTest_" + deploymentType;
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateSchemalessData(topicName, 1, 100);
        setupSchemalessConnector(topicName, 1, deploymentType);
        ClickHouseTestHelpers.waitWhileCounting(chc, topicName, 3, deploymentType);
        assertTrue(dataCount <= ClickHouseTestHelpers.countRows(chc, topicName, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void stockGenSingleTaskInterruptTest(ClickHouseDeploymentType deploymentType) throws IOException, InterruptedException {
        checkInterruptTest("stockGenSingleTaskInterruptTest_" + deploymentType, 1, deploymentType);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void stockGenMultiTaskInterruptTest(ClickHouseDeploymentType deploymentType) throws IOException, InterruptedException {
        checkInterruptTest("stockGenMultiTaskInterruptTest_" + deploymentType, 3, deploymentType);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void stockGenMultiTaskTopicTest(ClickHouseDeploymentType deploymentType) throws IOException, InterruptedException {
        String topicName = "stockGenMultiTaskTopicTest_" + deploymentType;
        int parCount = 3;
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateData(topicName, parCount, 200);
        setupConnector(topicName, parCount, deploymentType);
        ClickHouseTestHelpers.waitWhileCounting(chc, topicName, 3, deploymentType);
        LOGGER.info(confluentPlatform.getConnectors());
        assertTrue(dataCount <= ClickHouseTestHelpers.countRows(chc, topicName, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void stockGenMultiTaskSchemalessTest(ClickHouseDeploymentType deploymentType) throws IOException, InterruptedException {
        String topicName = "stockGenMultiTaskSchemalessTest_" + deploymentType;
        int parCount = 3;
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateSchemalessData(topicName, parCount, 200);
        setupSchemalessConnector(topicName, parCount, deploymentType);
        ClickHouseTestHelpers.waitWhileCounting(chc, topicName, 3, deploymentType);
        LOGGER.info(confluentPlatform.getConnectors());
        assertTrue(dataCount <= ClickHouseTestHelpers.countRows(chc, topicName, deploymentType));
    }

    private static Map<String, String> getTestProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConfig.PROXY_TYPE, "IGNORE");
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, "V2");
        if (isCluster) {
            props.putAll(ClickHouseCluster.getClusterProps(ClickHouseTestHelpers.DATABASE_DEFAULT));
        } else {
            // standalone
            props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
            props.put(ClickHouseSinkConnector.PORT, String.valueOf(db.getMappedPort(ClickHouseProtocol.HTTP.getDefaultPort())));
            props.put(ClickHouseSinkConnector.DATABASE, ClickHouseTestHelpers.DATABASE_DEFAULT);
            props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
            props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        }
        return props;
    }

    private int generateData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen.json", topicName, numberOfPartitions, numberOfRecords);
    }

    private int generateSchemalessData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen_json.json", topicName, numberOfPartitions, numberOfRecords);
    }

    private void setupConnector(String topicName, int taskCount, ClickHouseDeploymentType deploymentType) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector...");
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
        ClickHouseTestHelpers.dropTable(chc, topicName, deploymentType);
        new CreateTableStatement(STOCK_TABLE).tableName(topicName).deploymentType(deploymentType).execute(chc);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink.json")));
        String jsonString;
        jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                "toxiproxy", PROXY_PORT, chc.getUsername(), chc.getPassword());

        confluentPlatform.createConnect(jsonString);
        Thread.sleep(1000);
    }

    private void setupSchemalessConnector(String topicName, int taskCount, ClickHouseDeploymentType deploymentType) throws IOException, InterruptedException {
        LOGGER.info("Setting up schemaless connector...");
        ClickHouseTestHelpers.dropTable(chc, topicName, deploymentType);
        new CreateTableStatement(STOCK_TABLE).tableName(topicName).deploymentType(deploymentType).execute(chc);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink_schemaless.json")));
        String jsonString;
        jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                "toxiproxy", PROXY_PORT, chc.getUsername(), chc.getPassword());

        confluentPlatform.createConnect(jsonString);
        Thread.sleep(1000);
    }

    private void setupConnectorWithJdbcProperties(String topicName, int taskCount, ClickHouseDeploymentType deploymentType) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector with jdbc properties...");
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
        ClickHouseTestHelpers.dropTable(chc, topicName, deploymentType);
        new CreateTableStatement(STOCK_TABLE).tableName(topicName).deploymentType(deploymentType).execute(chc);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink_with_jdbc_prop.json")));
        String jsonString;
        jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                "toxiproxy", PROXY_PORT, chc.getUsername(), chc.getPassword());

        confluentPlatform.createConnect(jsonString);
        Thread.sleep(1000);
    }

    private void checkInterruptTest(String topicName, int parCount, ClickHouseDeploymentType deploymentType) throws InterruptedException, IOException {
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateData(topicName, parCount, 2500);
        setupConnector(topicName, parCount, deploymentType);
        int databaseCount = ClickHouseTestHelpers.countRows(chc, topicName, deploymentType);
        int lastCount = 0;
        int loopCount = 0;

        while (databaseCount != lastCount || loopCount < 5) {
            if (loopCount == 0) {
                LOGGER.info("Disabling proxy");
                proxy.disable();
            } else if (!proxy.isEnabled()) {
                LOGGER.info("Re-enabling proxy");
                proxy.enable();
            }
            Thread.sleep(3500);
            databaseCount = ClickHouseTestHelpers.countRows(chc, topicName, deploymentType);
            if (lastCount == databaseCount) {
                loopCount++;
            } else {
                loopCount = 0;
            }

            lastCount = databaseCount;
        }

        assertTrue(dataCount <= ClickHouseTestHelpers.countRows(chc, topicName, deploymentType));
    }
}
