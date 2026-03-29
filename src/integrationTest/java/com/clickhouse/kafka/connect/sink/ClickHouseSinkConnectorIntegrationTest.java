package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseCluster;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.ClusterConfig;
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

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClickHouseSinkConnectorIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkConnectorIntegrationTest.class);
    public static ConfluentPlatform confluentPlatform;
    private static ClickHouseContainer db;
    private static ClickHouseHelperClient chcNoProxy;
    public static ToxiproxyContainer toxiproxy;
    public static Proxy clickhouseProxy;
    private static final String SINK_CONNECTOR_NAME = "ClickHouseSinkConnector";
    private static final String CLICKHOUSE_DB_NETWORK_ALIAS = "clickhouse";
    private static final String TOXIPROXY_DOCKER_IMAGE_NAME = "ghcr.io/shopify/toxiproxy:2.7.0";
    private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";
    private static final boolean isCluster = ClickHouseTestHelpers.isCluster();
    private static final CreateTableStatement STOCK_TABLE = new CreateTableStatement()
            .column("side", "String")
            .column("quantity", "Int32")
            .column("symbol", "String")
            .column("price", "Int32")
            .column("account", "String")
            .column("userid", "String")
            .column("insertTime", "DateTime DEFAULT now()")
            .engine("MergeTree")
            .orderByColumn("symbol");

    public static Stream<ClusterConfig> clusterConfigs() {
        return ClickHouseTestHelpers.clusterConfigs();
    }

    @BeforeAll
    public static void setup() {
        Network network = Network.newNetwork();
        List<String> connectorPath = new LinkedList<>();
        String confluentArchive = new File(Paths.get("build/confluentArchive").toString()).getAbsolutePath();
        connectorPath.add(confluentArchive);
        confluentPlatform = new ConfluentPlatform(network, connectorPath);

        if (isCluster) {
            ClickHouseCluster cluster = new ClickHouseCluster();
            chcNoProxy = new ClickHouseHelperClient.ClickHouseClientBuilder(
                    cluster.getHost(), cluster.getPort(), ClickHouseProxyType.IGNORE, null, -1)
                    .setDatabase("default")
                    .setUsername(ClickHouseTestHelpers.USERNAME_DEFAULT)
                    .setPassword("")
                    .sslEnable(false)
                    .useClientV2(true)
                    .build();
        } else {
            db = new ClickHouseContainer(ClickHouseTestHelpers.CLICKHOUSE_DOCKER_IMAGE)
                    .withNetwork(network)
                    .withNetworkAliases(CLICKHOUSE_DB_NETWORK_ALIAS);
            db.start();

            toxiproxy = new ToxiproxyContainer(TOXIPROXY_DOCKER_IMAGE_NAME)
                    .withNetwork(network)
                    .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);
            toxiproxy.start();

            chcNoProxy = createClientNoProxy(getTestProperties());
        }
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);

        if (!isCluster) {
            if (clickhouseProxy != null) {
                clickhouseProxy.delete();
            }
            ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            clickhouseProxy = toxiproxyClient.createProxy("clickhouse-proxy", "0.0.0.0:8666",
                    String.format("%s:%d", CLICKHOUSE_DB_NETWORK_ALIAS, ClickHouseProtocol.HTTP.getDefaultPort()));
        }
    }

    @AfterAll
    public static void tearDown() {
        if (!isCluster) {
            db.stop();
            toxiproxy.stop();
        }
        confluentPlatform.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void stockGenSingleTaskTest(ClusterConfig clusterConfig) throws IOException, InterruptedException {
        String topicName = "stockGenSingleTaskTest";
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateData(topicName, 1, 100);
        setupConnector(topicName, 1, clusterConfig);
        waitWhileCounting(chcNoProxy, topicName, 3, clusterConfig);
        assertTrue(dataCount <= countRows(chcNoProxy, topicName, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void stockGenWithJdbcPropSingleTaskTest(ClusterConfig clusterConfig) throws IOException, InterruptedException {
        String topicName = "stockGenWithJdbcPropSingleTaskTest";
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateData(topicName, 1, 100);
        setupConnectorWithJdbcProperties(topicName, 1, clusterConfig);
        waitWhileCounting(chcNoProxy, topicName, 3, clusterConfig);
        assertTrue(dataCount <= countRows(chcNoProxy, topicName, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void stockGenSingleTaskSchemalessTest(ClusterConfig clusterConfig) throws IOException, InterruptedException {
        String topicName = "stockGenSingleTaskSchemalessTest";
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateSchemalessData(topicName, 1, 100);
        setupSchemalessConnector(topicName, 1, clusterConfig);
        waitWhileCounting(chcNoProxy, topicName, 3, clusterConfig);
        assertTrue(dataCount <= countRows(chcNoProxy, topicName, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void stockGenSingleTaskInterruptTest(ClusterConfig clusterConfig) throws IOException, InterruptedException {
        Assumptions.assumeFalse(isCluster, "Interrupt tests require toxiproxy and are skipped in cluster mode");
        checkInterruptTest("stockGenSingleTaskInterruptTest", 1, clusterConfig);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void stockGenMultiTaskInterruptTest(ClusterConfig clusterConfig) throws IOException, InterruptedException {
        Assumptions.assumeFalse(isCluster, "Interrupt tests require toxiproxy and are skipped in cluster mode");
        checkInterruptTest("stockGenMultiTaskInterruptTest", 3, clusterConfig);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void stockGenMultiTaskTopicTest(ClusterConfig clusterConfig) throws IOException, InterruptedException {
        String topicName = "stockGenMultiTaskTopicTest";
        int parCount = 3;
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateData(topicName, parCount, 200);
        setupConnector(topicName, parCount, clusterConfig);
        waitWhileCounting(chcNoProxy, topicName, 3, clusterConfig);
        LOGGER.info(confluentPlatform.getConnectors());
        assertTrue(dataCount <= countRows(chcNoProxy, topicName, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void stockGenMultiTaskSchemalessTest(ClusterConfig clusterConfig) throws IOException, InterruptedException {
        String topicName = "stockGenMultiTaskSchemalessTest";
        int parCount = 3;
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateSchemalessData(topicName, parCount, 200);
        setupSchemalessConnector(topicName, parCount, clusterConfig);
        waitWhileCounting(chcNoProxy, topicName, 3, clusterConfig);
        LOGGER.info(confluentPlatform.getConnectors());
        assertTrue(dataCount <= countRows(chcNoProxy, topicName, clusterConfig));
    }

    private static Map<String, String> getTestProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, String.valueOf(db.getMappedPort(ClickHouseProtocol.HTTP.getDefaultPort())));
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        props.put(ClickHouseSinkConfig.PROXY_TYPE, "HTTP");
        props.put(ClickHouseSinkConfig.PROXY_HOST, toxiproxy.getHost());
        props.put(ClickHouseSinkConfig.PROXY_PORT, String.valueOf(toxiproxy.getMappedPort(8666)));
        return props;
    }

    private static ClickHouseHelperClient createClient(Map<String, String> props) {
        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(props);
        return new ClickHouseHelperClient.ClickHouseClientBuilder(csc.getHostname(), csc.getPort(), csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
                .setDatabase(csc.getDatabase())
                .setUsername(csc.getUsername())
                .setPassword(csc.getPassword())
                .sslEnable(csc.isSslEnabled())
                .setTimeout(csc.getTimeout())
                .setRetry(csc.getRetry())
                .useClientV2(true)
                .build();
    }

    private static ClickHouseHelperClient createClientNoProxy(Map<String, String> props) {
        props.put(ClickHouseSinkConfig.PROXY_TYPE, "IGNORE");
        return createClient(props);
    }

    private int generateData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen.json", topicName, numberOfPartitions, numberOfRecords);
    }

    private int generateSchemalessData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen_json.json", topicName, numberOfPartitions, numberOfRecords);
    }

    private void setupConnector(String topicName, int taskCount, ClusterConfig clusterConfig) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector...");
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
        dropTable(chcNoProxy, topicName, clusterConfig);
        new CreateTableStatement(STOCK_TABLE).tableName(topicName).clusterConfig(clusterConfig).execute(chcNoProxy);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink.json")));
        String jsonString;
        if (isCluster) {
            jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                    "host.docker.internal", 8123, ClickHouseTestHelpers.USERNAME_DEFAULT, "");
        } else {
            jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                    "toxiproxy", 8666, db.getUsername(), db.getPassword());
        }

        confluentPlatform.createConnect(jsonString);
        Thread.sleep(1000);
    }

    private void setupSchemalessConnector(String topicName, int taskCount, ClusterConfig clusterConfig) throws IOException, InterruptedException {
        LOGGER.info("Setting up schemaless connector...");
        dropTable(chcNoProxy, topicName, clusterConfig);
        new CreateTableStatement(STOCK_TABLE).tableName(topicName).clusterConfig(clusterConfig).execute(chcNoProxy);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink_schemaless.json")));
        String jsonString;
        if (isCluster) {
            jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                    "host.docker.internal", 8123, ClickHouseTestHelpers.USERNAME_DEFAULT, "");
        } else {
            jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                    "toxiproxy", 8666, db.getUsername(), db.getPassword());
        }

        confluentPlatform.createConnect(jsonString);
        Thread.sleep(1000);
    }

    private void setupConnectorWithJdbcProperties(String topicName, int taskCount, ClusterConfig clusterConfig) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector with jdbc properties...");
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
        dropTable(chcNoProxy, topicName, clusterConfig);
        new CreateTableStatement(STOCK_TABLE).tableName(topicName).clusterConfig(clusterConfig).execute(chcNoProxy);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink_with_jdbc_prop.json")));
        String jsonString;
        if (isCluster) {
            jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                    "host.docker.internal", 8123, ClickHouseTestHelpers.USERNAME_DEFAULT, "");
        } else {
            jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                    "toxiproxy", 8666, db.getUsername(), db.getPassword());
        }

        confluentPlatform.createConnect(jsonString);
        Thread.sleep(1000);
    }

    private void checkInterruptTest(String topicName, int parCount, ClusterConfig clusterConfig) throws InterruptedException, IOException {
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateData(topicName, parCount, 2500);
        setupConnector(topicName, parCount, clusterConfig);
        int databaseCount = countRows(chcNoProxy, topicName, clusterConfig);
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
            databaseCount = countRows(chcNoProxy, topicName, clusterConfig);
            if (lastCount == databaseCount) {
                loopCount++;
            } else {
                loopCount = 0;
            }

            lastCount = databaseCount;
        }

        assertTrue(dataCount <= countRows(chcNoProxy, topicName, clusterConfig));
    }
}
