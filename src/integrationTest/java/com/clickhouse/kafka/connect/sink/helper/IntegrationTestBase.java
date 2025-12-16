package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.Version;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class IntegrationTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTestBase.class);

    private static final String SINK_CONNECTOR_NAME = "ClickHouseSinkConnector";
    private static final String CLICKHOUSE_DEFAULT_VERSION = "latest";

    // singletons
    private static final AtomicInteger instanceCount = new AtomicInteger(0);
    private static final Object lock = new Object();
    protected static Network network = Network.newNetwork();
    protected static ConfluentPlatform confluentPlatform;

    // instance variables
    protected final String chNetAlias;
    protected final ClickHouseContainer db;
    protected ClickHouseHelperClient dbClientNoProxy;

    // proxy
    private final boolean useProxy = Boolean.getBoolean("integrationTest.useProxy");
    protected ToxiproxyContainer toxiproxy;
    protected Proxy clickhouseProxy;

    public IntegrationTestBase() {

        chNetAlias = "clickhouse-" + instanceCount.getAndIncrement();
        final String chImage = String.format("clickhouse/clickhouse-server:%s", getClickHouseVersion());
        db = new ClickHouseContainer(chImage)
                .withNetwork(network)
                .withNetworkAliases(chNetAlias)
                .withCopyFileToContainer(MountableFile.forClasspathResource("/keeper_map_config.xml"), "/etc/clickhouse-server/config.d/keeper_map_config.xml");

        if (useProxy) {
            toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.12.0").withNetwork(network).withNetworkAliases("toxiproxy");
        }

    }

    public void setup() {
        LOGGER.info("Setting up integration test...");

        if (!isCloud()) {
            LOGGER.info("Testing against containerized ClickHouse");
            synchronized (lock) {
                if (confluentPlatform == null) {
                    List<String> connectorPath = new LinkedList<>();
                    String confluentArchive = new File(Paths.get("build/confluentArchive").toString()).getAbsolutePath();
                    connectorPath.add(confluentArchive);
                    confluentPlatform = new ConfluentPlatform(network, connectorPath);
                }
            }

            db.start();
        }

        if (useProxy) {
            toxiproxy.start();
            ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            try {
                clickhouseProxy = toxiproxyClient.createProxy("clickhouse-proxy", "0.0.0.0:8666", "clickhouse:" + ClickHouseProtocol.HTTP.getDefaultPort());
            } catch (Exception e) {
                LOGGER.error("Failed to create proxy", e);
                throw new RuntimeException(e);
            }
        }
        dbClientNoProxy = createClientNoProxy(createLocalClientConfig());
    }

    public void tearDown() {
        if (toxiproxy != null) {
            toxiproxy.stop();
        }

        if (!isCloud()) {
            db.stop();
            confluentPlatform.close();
        }
    }


    public void beforeEach() throws IOException {

    }

    public Map<String, String> createConnectorSchemalessConfig() {
        Map<String, String> config = createConnectorConfig();
        config.put("value.converter.schemas.enable", "false");
        config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        return config;
    }

    public Map<String, String> createConnectorConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", ClickHouseSinkConnector.class.getName());
        config.put("connector.plugin.version", Version.ARTIFACT_VERSION);
        config.put("tasks.max", "1");
        config.put(ClickHouseSinkConfig.EXACTLY_ONCE, "false");
        String clientVersion = getClientVersion();
        config.put(ClickHouseSinkConnector.CLIENT_VERSION, clientVersion);
        config.put(ClickHouseSinkConnector.DATABASE, db.getDatabaseName());

        if (isCloud()) {
            config.put(ClickHouseSinkConnector.HOSTNAME, System.getenv("CLICKHOUSE_CLOUD_HOST"));
            config.put(ClickHouseSinkConnector.PORT, ClickHouseTestHelpers.HTTPS_PORT);
            config.put(ClickHouseSinkConnector.USERNAME, ClickHouseTestHelpers.USERNAME_DEFAULT);
            config.put(ClickHouseSinkConnector.PASSWORD, System.getenv("CLICKHOUSE_CLOUD_PASSWORD"));
            config.put(ClickHouseSinkConnector.SSL_ENABLED, "true");
            config.put(String.valueOf(ClickHouseClientOption.CONNECTION_TIMEOUT), "60000");
            config.put("clickhouseSettings", "insert_quorum=3");
        } else {
            config.put(ClickHouseSinkConnector.HOSTNAME, chNetAlias);
            config.put(ClickHouseSinkConnector.PORT, "8123");
            config.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
            config.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
            config.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        }
        return config;
    }

    public Map<String, String> createLocalClientConfig() {
        Map<String, String> config = createConnectorConfig();
        if (!isCloud()) {
            config.put(ClickHouseSinkConnector.HOSTNAME, "localhost");
            config.put(ClickHouseSinkConnector.PORT, String.valueOf(db.getMappedPort(8123)));
            config.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        }
        return config;
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
                .build();
    }

    public static String getClientVersion() {
        String clientVersion = System.getProperty("clickhouseClientVersion");
        if (clientVersion != null && clientVersion.equals("V1")) {
            return "V1";
        } else {
            return "V2";
        }
    }

    protected static ClickHouseHelperClient createClientNoProxy(Map<String, String> props) {
        props.put(ClickHouseSinkConfig.PROXY_TYPE, ClickHouseProxyType.IGNORE.name());
        return createClient(props);
    }

    public static String getClickHouseVersion() {
        return System.getProperty("lickhouseVersion", CLICKHOUSE_DEFAULT_VERSION);
    }

    public static boolean isCloud() {
        return "cloud".equalsIgnoreCase(getClickHouseVersion());
    }

    public boolean isUseProxy() {
        return useProxy;
    }

    protected int generateData(String topicName, int numberOfPartitions, int numberOfRecords) throws Exception {
        Map<String, Object> config = new HashMap<>(DATA_GEN_BASE_CONFIG);
        return runDataGenConnector(config, topicName, numberOfPartitions, numberOfRecords);
    }

    protected int generateSchemalessData(String topicName, int numberOfPartitions, int numberOfRecords) throws Exception {
        Map<String, Object> config = new HashMap<>(DATA_GEN_BASE_CONFIG);
        config.put("value.converter", ConfluentPlatform.KAFKA_JSON_CONVERTER);
        DATA_GEN_BASE_CONFIG.put("value.converter.schemas.enable", "false");
        return runDataGenConnector(config, topicName, numberOfPartitions, numberOfRecords);
    }

    private final ObjectMapper json = new ObjectMapper(new JsonFactory());

    public static final Map<String, Object> DATA_GEN_BASE_CONFIG = new HashMap<>();

    static {
        DATA_GEN_BASE_CONFIG.put("connector.class", ConfluentPlatform.DATA_GEN_CONNECTOR_CLASS);
        DATA_GEN_BASE_CONFIG.put("tasks.max", "1");
        DATA_GEN_BASE_CONFIG.put("max.interval", "100");
        DATA_GEN_BASE_CONFIG.put("quickstart", "Stock_Trades");
    }

    public int runDataGenConnector(Map<String, Object> config, String topicName, int numberOfPartitions, int numberOfRecords) throws Exception {

        String connectorName = "DataGen_" + topicName;
        config.put("name", connectorName);
        config.put("kafka.topic", topicName);
        config.put("iterations", numberOfRecords);
        config.put("tasks.max", numberOfPartitions);
        ObjectNode jsonConfig = json.createObjectNode();
        jsonConfig.put("name", connectorName);
        jsonConfig.set("config", json.valueToTree(config));
        String connectorConfig = json.writeValueAsString(jsonConfig);
        LOGGER.info("Connector config: {}", json.writerWithDefaultPrettyPrinter().writeValueAsString(jsonConfig));
        confluentPlatform.createConnect(connectorConfig);

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);

            String currentState = confluentPlatform.getConnectors();
            LOGGER.debug(currentState);
        }

        confluentPlatform.deleteConnectors(connectorName);
        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);

            String currentState = confluentPlatform.getConnectors();
            LOGGER.debug(currentState);
        }

        long offsetTotal = 0;
        for (int i = 0; i < numberOfPartitions; i++) {
            offsetTotal += confluentPlatform.getOffset(topicName, i);
        }
        int runningTotal = numberOfPartitions * numberOfRecords * 5;
        LOGGER.info("Generated records for [{}]: Total (By Offset): [{}], Diff from Theoretical Total: [{}]", topicName, offsetTotal, runningTotal - offsetTotal);
        return (int) offsetTotal;
    }


    public String runClickHouseConnector(Map<String, String> config, String topicName) throws Exception {

        String connectorName = "ClickHouseSink_" + topicName;
        config.put("name", connectorName);
        config.put("topics", topicName);
        ObjectNode jsonConfig = json.createObjectNode();
        jsonConfig.put("name", connectorName);
        jsonConfig.set("config", json.valueToTree(config));
        String connectorConfig = json.writeValueAsString(jsonConfig);
        confluentPlatform.createConnect(connectorConfig);

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);

            String currentState = confluentPlatform.getConnectors();
            LOGGER.debug(currentState);
        }
        return connectorName;
    }

    public void stopClickHouseConnector(String connectorName) throws Exception {
        confluentPlatform.deleteConnectors(connectorName);
        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);

            String currentState = confluentPlatform.getConnectors();
            LOGGER.debug(currentState);
        }
    }
}
