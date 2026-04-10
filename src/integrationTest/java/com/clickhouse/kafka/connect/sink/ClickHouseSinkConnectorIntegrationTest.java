package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.ConfluentPlatform;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
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

    @BeforeAll
    public static void setup() {
        Network network = Network.newNetwork();
        List<String> connectorPath = new LinkedList<>();
        String confluentArchive = new File(Paths.get("build/confluentArchive").toString()).getAbsolutePath();
        connectorPath.add(confluentArchive);
        confluentPlatform = new ConfluentPlatform(network, connectorPath);

        db = new ClickHouseContainer(ClickHouseTestHelpers.CLICKHOUSE_DOCKER_IMAGE).withNetwork(network).withNetworkAliases(CLICKHOUSE_DB_NETWORK_ALIAS);
        db.start();

        toxiproxy = new ToxiproxyContainer(TOXIPROXY_DOCKER_IMAGE_NAME).withNetwork(network).withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);
        toxiproxy.start();

        chcNoProxy = createClientNoProxy(getTestProperties());
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);

        if (clickhouseProxy != null) {
            clickhouseProxy.delete();
        }

        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        clickhouseProxy = toxiproxyClient.createProxy("clickhouse-proxy", "0.0.0.0:8666", String.format("%s:%d", CLICKHOUSE_DB_NETWORK_ALIAS, ClickHouseProtocol.HTTP.getDefaultPort()));
    }

    @AfterAll
    public static void tearDown() {
        db.stop();
        toxiproxy.stop();
        confluentPlatform.close();
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

    private void setupConnector(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector...");
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
        dropTable(chcNoProxy, topicName);
        new CreateTableStatement(STOCK_TABLE).tableName(topicName).execute(chcNoProxy);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink.json")));
        // The client makes requests with absolute URIs when a proxy is configured - currently, requests with absolute paths are rejected by CH server.
        // To work around this, transparently connect to the toxiproxy endpoint and avoid configuring the proxy settings on the client. The proxy will relay relative URIs, which the CH server expects.
        String jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName, "toxiproxy", 8666, db.getUsername(), db.getPassword());

        confluentPlatform.createConnect(jsonString);
        Thread.sleep(1000);
    }

    private void setupSchemalessConnector(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting up schemaless connector...");
        dropTable(chcNoProxy, topicName);
        new CreateTableStatement(STOCK_TABLE).tableName(topicName).execute(chcNoProxy);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink_schemaless.json")));
        String jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName, "toxiproxy", 8666, db.getUsername(), db.getPassword());

        confluentPlatform.createConnect(jsonString);
        Thread.sleep(1000);
    }

    private void setupAvroConnector(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting up Avro connector for topic {}...", topicName);
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);

        // TODO: extract this into a json file
        JSONObject config = new JSONObject();
        config.put("name", SINK_CONNECTOR_NAME);
        config.put("connector.class", "com.clickhouse.kafka.connect.ClickHouseSinkConnector");
        config.put("tasks.max", String.valueOf(taskCount));
        config.put("topics", topicName);
        config.put("hostname", CLICKHOUSE_DB_NETWORK_ALIAS);
        config.put("port", "8123");
        config.put("database", "default");
        config.put("username", db.getUsername());
        config.put("password", db.getPassword());
        config.put("ssl", "false");
        config.put("exactlyOnce", "false");
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", "http://schema-registry:8081");

        JSONObject payload = new JSONObject();
        payload.put("name", SINK_CONNECTOR_NAME);
        payload.put("config", config);

        confluentPlatform.createConnect(payload.toString());
        Thread.sleep(1000);
    }

    private void setupConnectorWithJdbcProperties(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector with jdbc properties...");
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
        dropTable(chcNoProxy, topicName);
        new CreateTableStatement(STOCK_TABLE).tableName(topicName).execute(chcNoProxy);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get("src/integrationTest/resources/clickhouse_sink_with_jdbc_prop.json")));
        String jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName, "toxiproxy", 8666, db.getUsername(), db.getPassword());

        confluentPlatform.createConnect(jsonString);
        Thread.sleep(1000);
    }

    private void checkInterruptTest(String topicName, int parCount) throws InterruptedException, IOException {
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateData(topicName, parCount, 2500);
        setupConnector(topicName, parCount);
        int databaseCount = countRows(chcNoProxy, topicName);
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

    private static Stream<String> lsAvroSchemas() {
        String schemaDir = "src/testFixtures/avro/schemas/breaking";
        return Stream.of(Objects.requireNonNull(new File(schemaDir).listFiles())).map(file -> schemaDir + "/" + file.getName());
    }

    @ParameterizedTest
    @MethodSource("lsAvroSchemas")
    public void avroSchemaTest(String fixtureFilePath) throws Exception {
        String fixtureContent = String.join("", Files.readAllLines(Paths.get(fixtureFilePath)));
        JSONObject fixture = new JSONObject(fixtureContent);

        String description = fixture.getString("description");
        JSONObject schema = fixture.getJSONObject("schema");
        JSONObject clickhouseColumns = fixture.getJSONObject("clickhouse_columns");
        String orderBy = fixture.getString("clickhouse_order_by");
        JSONArray records = fixture.getJSONArray("records");
        int expectedRowCount = fixture.getInt("expected_row_count");

        // Derive topic name from fixture filename (remove .json extension)
        String fileName = Paths.get(fixtureFilePath).getFileName().toString();
        String topicName = "chaos_" + fileName.replace(".json", "").replace("-", "_");

        LOGGER.info("Running chaos test: {} (topic: {}, expected rows: {})", description, topicName, expectedRowCount);

        // 1. Create topic
        confluentPlatform.createTopic(topicName, 1);

        // 2. Create ClickHouse table
        dropTable(chcNoProxy, topicName);
        CreateTableStatement tableStmt = new CreateTableStatement()
                .tableName(topicName)
                .engine("MergeTree")
                .orderByColumn(orderBy);
        for (String colName : clickhouseColumns.keySet()) {
            tableStmt.column(colName, clickhouseColumns.getString(colName));
        }
        tableStmt.execute(chcNoProxy);

        // 3. Produce Avro records via REST proxy
        int producedCount = confluentPlatform.produceAvroRecords(
                topicName,
                schema.toString(),
                records
        );
        LOGGER.info("Produced {} records to topic {}", producedCount, topicName);

        // 4. Setup sink connector with Avro converter
        setupAvroConnector(topicName, 1);

        // 5. Wait for data to flow through
        waitWhileCounting(chcNoProxy, topicName, 3);

        // 6. Check for connector task failure
        Optional<String> taskFailure = confluentPlatform.getFirstTaskFailureOpt(SINK_CONNECTOR_NAME);
        if (taskFailure.isPresent()) {
            throw new Exception(String.format("Connector task failed for %s: %s", fileName, taskFailure.get()));
        }

        // 7. Check row count
        int rowCount = countRows(chcNoProxy, topicName);
        LOGGER.info("ClickHouse row count for {}: {} (expected: {})", topicName, rowCount, expectedRowCount);

        if (rowCount < expectedRowCount) {
            throw new Exception(String.format("Data loss detected for %s: got %d rows, expected %d", fileName, rowCount, expectedRowCount));
        }
    }
}
