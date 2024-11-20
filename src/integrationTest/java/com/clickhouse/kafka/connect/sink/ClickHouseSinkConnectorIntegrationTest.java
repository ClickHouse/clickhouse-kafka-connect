package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.ConfluentPlatform;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

    @BeforeAll
    public static void setup() {
        Network network = Network.newNetwork();
        List<String> connectorPath = new LinkedList<>();
        String confluentArchive = new File(Paths.get("build/confluentArchive").toString()).getAbsolutePath();
        connectorPath.add(confluentArchive);
        confluentPlatform = new ConfluentPlatform(network, connectorPath);

        db = new ClickHouseContainer(ClickHouseTestHelpers.CLICKHOUSE_DOCKER_IMAGE).withNetwork(network).withNetworkAliases("clickhouse");
        db.start();

        toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.7.0").withNetwork(network).withNetworkAliases("toxiproxy");
        toxiproxy.start();

        chcNoProxy = createClientNoProxy(getTestProperties());
    }

    @AfterAll
    public static void tearDown() {
        db.stop();
        toxiproxy.stop();
        confluentPlatform.close();
    }

    private static Map<String, String> getTestProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, String.valueOf(db.getMappedPort(ClickHouseProtocol.HTTP.getDefaultPort())));
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        props.put(ClickHouseSinkConfig.PROXY_TYPE, ClickHouseProxyType.HTTP.name());
        props.put(ClickHouseSinkConfig.PROXY_HOST, toxiproxy.getHost());
        props.put(ClickHouseSinkConfig.PROXY_PORT, String.valueOf(toxiproxy.getMappedPort(8666)));
        return props;
    }

    private static ClickHouseHelperClient createClient(Map<String,String> props) {
        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(props);
        return new ClickHouseHelperClient.ClickHouseClientBuilder(csc.getHostname(), csc.getPort(), csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
                .setDatabase(csc.getDatabase())
                .setUsername(csc.getUsername())
                .setPassword(csc.getPassword())
                .sslEnable(csc.isSslEnabled())
                .setTimeout(csc.getTimeout())
                .setRetry(csc.getRetry())
                .build();
    }
    private static ClickHouseHelperClient createClientNoProxy(Map<String,String> props) {
        props.put(ClickHouseSinkConfig.PROXY_TYPE, ClickHouseProxyType.IGNORE.name());
        return createClient(props);
    }








    private int generateData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen.json", topicName, numberOfPartitions, numberOfRecords);
    }
    private int generateSchemalessData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen_json.json", topicName, numberOfPartitions, numberOfRecords);
    }




    private void setupConnector(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector...");
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
        dropTable(chcNoProxy, topicName);
        createMergeTreeTable(chcNoProxy, topicName);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink.json")));
        String jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                "clickhouse", ClickHouseProtocol.HTTP.getDefaultPort(), db.getPassword(), "toxiproxy", 8666);

        confluentPlatform.createConnect(jsonString);
        Thread.sleep(1000);
    }

    private void setupSchemalessConnector(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting up schemaless connector...");
        dropTable(chcNoProxy, topicName);
        createMergeTreeTable(chcNoProxy, topicName);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink_schemaless.json")));
        String jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                "clickhouse", ClickHouseProtocol.HTTP.getDefaultPort(), db.getPassword(), "toxiproxy", 8666);

        confluentPlatform.createConnect(jsonString);
        Thread.sleep(1000);
    }

    private void setupConnectorWithJdbcProperties(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector with jdbc properties...");
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
        dropTable(chcNoProxy, topicName);
        createMergeTreeTable(chcNoProxy, topicName);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink_with_jdbc_prop.json")));
        String jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                "clickhouse", ClickHouseProtocol.HTTP.getDefaultPort(), db.getPassword(), "toxiproxy", 8666);

        confluentPlatform.createConnect(jsonString);
        Thread.sleep(1000);
    }


    @BeforeEach
    public void beforeEach() throws IOException {
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);

        if (clickhouseProxy != null) {
            clickhouseProxy.delete();
        }

        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        clickhouseProxy = toxiproxyClient.createProxy("clickhouse-proxy", "0.0.0.0:8666", "clickhouse:" + ClickHouseProtocol.HTTP.getDefaultPort());
    }





    @Test
    public void stockGenSingleTaskTest() throws IOException, InterruptedException {
        String topicName = "stockGenSingleTaskTest";
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateData(topicName, 1, 100);
        setupConnector(topicName, 1);
        waitWhileCounting(chcNoProxy, topicName, 3);
        assertTrue(dataCount <= countRows(chcNoProxy, topicName));
    }

    @Test
    public void stockGenWithJdbcPropSingleTaskTest() throws IOException, InterruptedException {
        String topicName = "stockGenWithJdbcPropSingleTaskTest";
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateData(topicName, 1, 100);
        setupConnectorWithJdbcProperties(topicName, 1);
        waitWhileCounting(chcNoProxy, topicName, 3);
        assertTrue(dataCount <= countRows(chcNoProxy, topicName));
    }

    @Test
    public void stockGenSingleTaskSchemalessTest() throws IOException, InterruptedException {
        String topicName = "stockGenSingleTaskSchemalessTest";
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateSchemalessData(topicName, 1, 100);
        setupSchemalessConnector(topicName, 1);
        waitWhileCounting(chcNoProxy, topicName, 3);
        assertTrue(dataCount <= countRows(chcNoProxy, topicName));
    }


    private void checkInterruptTest(String topicName, int parCount) throws InterruptedException, IOException {
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateData(topicName, parCount, 2500);
        setupConnector(topicName, parCount);
        int databaseCount = countRows(chcNoProxy, topicName);
        int lastCount = 0;
        int loopCount = 0;

        while(databaseCount != lastCount || loopCount < 5) {
            if (loopCount == 0) {
                LOGGER.info("Disabling proxy");
                clickhouseProxy.disable();
            } else if (!clickhouseProxy.isEnabled()) {
                LOGGER.info("Re-enabling proxy");
                clickhouseProxy.enable();
            }
            Thread.sleep(3500);
            databaseCount = countRows(chcNoProxy, topicName);
            if (lastCount == databaseCount) {
                loopCount++;
            } else {
                loopCount = 0;
            }

            lastCount = databaseCount;
        }

        assertTrue(dataCount <= countRows(chcNoProxy, topicName));
    }
    @Test
    public void stockGenSingleTaskInterruptTest() throws IOException, InterruptedException {
        checkInterruptTest("stockGenSingleTaskInterruptTest", 1);
    }

    @Test
    public void stockGenMultiTaskInterruptTest() throws IOException, InterruptedException {
        checkInterruptTest("stockGenMultiTaskInterruptTest", 3);
    }


    @Test
    public void stockGenMultiTaskTopicTest() throws IOException, InterruptedException {
        String topicName = "stockGenMultiTaskTopicTest";
        int parCount = 3;
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateData(topicName, parCount, 200);
        setupConnector(topicName, parCount);
        waitWhileCounting(chcNoProxy, topicName, 3);
        LOGGER.info(confluentPlatform.getConnectors());
        assertTrue(dataCount <= countRows(chcNoProxy, topicName));
    }

    @Test
    public void stockGenMultiTaskSchemalessTest() throws IOException, InterruptedException {
        String topicName = "stockGenMultiTaskSchemalessTest";
        int parCount = 3;
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateSchemalessData(topicName, parCount, 200);
        setupSchemalessConnector(topicName, parCount);
        waitWhileCounting(chcNoProxy, topicName, 3);
        LOGGER.info(confluentPlatform.getConnectors());
        assertTrue(dataCount <= countRows(chcNoProxy, topicName));
    }


}
