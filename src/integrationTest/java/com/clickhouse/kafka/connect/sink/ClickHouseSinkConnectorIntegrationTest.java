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
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClickHouseSinkConnectorIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkConnectorIntegrationTest.class);
    public static ConfluentPlatform confluentPlatform;
    private static ClickHouseContainer db;
    private static ClickHouseHelperClient chcNoProxy;
    public static ToxiproxyContainer toxiproxy;
    public static Proxy clickhouseProxy;
    private static final String SINK_CONNECTOR_NAME = "ClickHouseSinkConnector";
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
    private static final Path RESOURCES_ROOT = Path.of("src", "integrationTest", "resources");

    @BeforeAll
    public static void setup() {
        Network network = Network.newNetwork();
        List<String> connectorPath = new LinkedList<>();
        String confluentArchive = new File(Path.of("build", "confluentArchive").toString()).getAbsolutePath();
        connectorPath.add(confluentArchive);
        confluentPlatform = new ConfluentPlatform(network, connectorPath);

        db = new ClickHouseContainer(ClickHouseTestHelpers.CLICKHOUSE_DOCKER_IMAGE).withNetwork(network).withNetworkAliases(ClickHouseTestHelpers.CLICKHOUSE_DB_NETWORK_ALIAS);
        db.start();

        toxiproxy = new ToxiproxyContainer(ClickHouseTestHelpers.TOXIPROXY_DOCKER_IMAGE_NAME).withNetwork(network).withNetworkAliases(ClickHouseTestHelpers.TOXIPROXY_NETWORK_ALIAS);
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
        clickhouseProxy = toxiproxyClient.createProxy("clickhouse-proxy", "0.0.0.0:8666", String.format("%s:%d", ClickHouseTestHelpers.CLICKHOUSE_DB_NETWORK_ALIAS, ClickHouseProtocol.HTTP.getDefaultPort()));
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
        ClickHouseTestHelpers.waitWhileCounting(chcNoProxy, topicName, 3);
        assertTrue(dataCount <= ClickHouseTestHelpers.countRows(chcNoProxy, topicName));
    }

    @Test
    public void stockGenWithJdbcPropSingleTaskTest() throws IOException, InterruptedException {
        String topicName = "stockGenWithJdbcPropSingleTaskTest";
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateData(topicName, 1, 100);
        setupConnectorWithJdbcProperties(topicName, 1);
        ClickHouseTestHelpers.waitWhileCounting(chcNoProxy, topicName, 3);
        assertTrue(dataCount <= ClickHouseTestHelpers.countRows(chcNoProxy, topicName));
    }

    @Test
    public void stockGenSingleTaskSchemalessTest() throws IOException, InterruptedException {
        String topicName = "stockGenSingleTaskSchemalessTest";
        confluentPlatform.createTopic(topicName, 1);
        int dataCount = generateSchemalessData(topicName, 1, 100);
        setupSchemalessConnector(topicName, 1);
        ClickHouseTestHelpers.waitWhileCounting(chcNoProxy, topicName, 3);
        assertTrue(dataCount <= ClickHouseTestHelpers.countRows(chcNoProxy, topicName));
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
        ClickHouseTestHelpers.waitWhileCounting(chcNoProxy, topicName, 3);
        LOGGER.info(confluentPlatform.getConnectors());
        assertTrue(dataCount <= ClickHouseTestHelpers.countRows(chcNoProxy, topicName));
    }

    @Test
    public void stockGenMultiTaskSchemalessTest() throws IOException, InterruptedException {
        String topicName = "stockGenMultiTaskSchemalessTest";
        int parCount = 3;
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateSchemalessData(topicName, parCount, 200);
        setupSchemalessConnector(topicName, parCount);
        ClickHouseTestHelpers.waitWhileCounting(chcNoProxy, topicName, 3);
        LOGGER.info(confluentPlatform.getConnectors());
        assertTrue(dataCount <= ClickHouseTestHelpers.countRows(chcNoProxy, topicName));
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

    private static ClickHouseHelperClient createClientNoProxy(Map<String, String> props) {
        props.put(ClickHouseSinkConfig.PROXY_TYPE, "IGNORE");
        return ClickHouseTestHelpers.createClient(props);
    }

    private int generateData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData(Path.of(RESOURCES_ROOT.toString(), "stock_gen.json").toString(), topicName, numberOfPartitions, numberOfRecords);
    }

    private int generateSchemalessData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData(Path.of(RESOURCES_ROOT.toString(), "stock_gen_json.json").toString(), topicName, numberOfPartitions, numberOfRecords);
    }

    private void setupConnector(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector...");
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
        ClickHouseTestHelpers.dropTable(chcNoProxy, topicName);
        new CreateTableStatement(STOCK_TABLE).tableName(topicName).execute(chcNoProxy);
        int responseCode = confluentPlatform.createConnect(SinkConfigs.BASE.getJsonPayload(taskCount, topicName));
        Assertions.assertTrue(responseCode >= 200 && responseCode < 300);
    }

    private void setupSchemalessConnector(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting up schemaless connector...");
        ClickHouseTestHelpers.dropTable(chcNoProxy, topicName);
        new CreateTableStatement(STOCK_TABLE).tableName(topicName).execute(chcNoProxy);
        int responseCode = confluentPlatform.createConnect(SinkConfigs.BASE_SCHEMALESS.getJsonPayload(taskCount, topicName));
        Assertions.assertTrue(responseCode >= 200 && responseCode < 300);
    }

    private void setupAvroConnector(String topicName) throws IOException, InterruptedException {
        LOGGER.info("Setting up Avro connector for topic {}...", topicName);
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
        int responseCode = confluentPlatform.createConnect(SinkConfigs.AVRO.getJsonPayload(1, topicName));
        Assertions.assertTrue(responseCode >= 200 && responseCode < 300);
    }

    private void setupProtobufConnector(String topicName) throws IOException, InterruptedException {
        LOGGER.info("Setting up Protobuf connector for topic {}...", topicName);
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
        int responseCode = confluentPlatform.createConnect(SinkConfigs.PROTOBUF.getJsonPayload(1, topicName));
        Assertions.assertTrue(responseCode >= 200 && responseCode < 300);
    }

    private void setupConnectorWithJdbcProperties(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector with jdbc properties...");
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
        ClickHouseTestHelpers.dropTable(chcNoProxy, topicName);
        new CreateTableStatement(STOCK_TABLE).tableName(topicName).execute(chcNoProxy);
        int responseCode = confluentPlatform.createConnect(SinkConfigs.JDBC_PROP.getJsonPayload(taskCount, topicName));
        Assertions.assertTrue(responseCode >= 200 && responseCode < 300);
    }

    private void checkInterruptTest(String topicName, int parCount) throws InterruptedException, IOException {
        confluentPlatform.createTopic(topicName, parCount);
        int dataCount = generateData(topicName, parCount, 2500);
        setupConnector(topicName, parCount);
        int databaseCount = ClickHouseTestHelpers.countRows(chcNoProxy, topicName);
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
            databaseCount = ClickHouseTestHelpers.countRows(chcNoProxy, topicName);
            if (lastCount == databaseCount) {
                loopCount++;
            } else {
                loopCount = 0;
            }

            lastCount = databaseCount;
        }

        assertTrue(dataCount <= ClickHouseTestHelpers.countRows(chcNoProxy, topicName));
    }

    private static Stream<Path> getCompatibleAvroSchemaPaths() {
        final File schemaDir = new File(Path.of("src", "testFixtures", "proto", "schemas", "compatible").toString());
        Assertions.assertTrue(schemaDir.exists());
        return Stream.of(Objects.requireNonNull(schemaDir.listFiles())).map(file -> Path.of(schemaDir.toPath().toString(), file.getName()));
    }


    /*
        Test sinking Avro records with various schemas that the connector currently supports.

        To add a new compatible schema to test:
        1. create a new JSON file in `src/testFixtures/avro/schemas/compatible`
        2. copy the structure from another schema file - ensure the following fields exists: [description, schema, clickhouse_columns, clickhouse_order_by, records, expected_row_count]

        The test will pick up the new schema automatically.

        To add a new incompatible schema, follow the same instructions as above but add the schema to `src/testFixtures/avro/schemas/incompatible`.
        Over time, the goal is to fix the connector to make previously incompatible schemas compatible wherever possible.
     */
    @ParameterizedTest
    @MethodSource("getCompatibleAvroSchemaPaths")
    public void avroSchemaTest(Path schemaPath) throws Exception {
        JSONObject fixture = new JSONObject(String.join("", Files.readAllLines(schemaPath)));

        // fixture JSON keys
        final String descriptionKey = "description";
        final String schemaKey = "schema";
        final String clickhouseColumnsKey = "clickhouse_columns";
        final String clickhouseOrderByKey = "clickhouse_order_by";
        final String recordsKey = "records";
        final String expectedRowCountKey = "expected_row_count";

        int expectedRowCount = fixture.getInt(expectedRowCountKey);

        // Derive topic name from fixture filename (remove .json extension)
        String fileName = schemaPath.getFileName().toString();
        String topicName = "avro_test_" + fileName.replace(".json", "").replace("-", "_");

        LOGGER.info("Running avro integration test: {} (topic: {}, expected rows: {})", fixture.getString(descriptionKey), topicName, expectedRowCount);

        // 1. Create topic
        confluentPlatform.createTopic(topicName, 1);

        // 2. Create ClickHouse table
        ClickHouseTestHelpers.dropTable(chcNoProxy, topicName);
        CreateTableStatement tableStmt = new CreateTableStatement()
                .tableName(topicName)
                .engine("MergeTree")
                .orderByColumn(fixture.getString(clickhouseOrderByKey));
        JSONObject clickhouseColumns = fixture.getJSONObject(clickhouseColumnsKey);
        for (String colName : clickhouseColumns.keySet()) {
            tableStmt.column(colName, clickhouseColumns.getString(colName));
        }
        tableStmt.execute(chcNoProxy);

        // 3. Produce Avro records via REST proxy
        int producedCount = confluentPlatform.produceAvroRecords(
                topicName,
                fixture.getJSONObject(schemaKey).toString(),
                fixture.getJSONArray(recordsKey)
        );
        LOGGER.info("Produced {} records to topic {}", producedCount, topicName);

        // 4. Setup sink connector with Avro converter
        setupAvroConnector(topicName);

        // 5. Wait for data to flow through
        ClickHouseTestHelpers.waitWhileCounting(chcNoProxy, topicName, 3);

        // 6. Check for connector task failure
        Optional<String> taskFailure = confluentPlatform.getFirstTaskFailureOpt(SINK_CONNECTOR_NAME);
        if (taskFailure.isPresent()) {
            Assertions.fail(String.format("Connector task failed for %s: %s", fileName, taskFailure.orElseThrow()));
        }

        // 7. Check row count
        Assertions.assertEquals(expectedRowCount, ClickHouseTestHelpers.countRows(chcNoProxy, topicName));
    }

    private static Stream<Path> getCompatibleProtoSchemaPaths() {
        final File schemaDir = new File(Path.of("src", "testFixtures", "proto", "schemas", "compatible").toString());
        Assertions.assertTrue(schemaDir.exists());
        return Stream.of(Objects.requireNonNull(schemaDir.listFiles((dir, name) -> name.endsWith(".json")))).map(file -> Path.of(schemaDir.toPath().toString(), file.getName()));
    }

    /*
        Test sinking Protobuf records with various schemas that the connector currently supports.

        Each fixture is a pair of files sharing a basename, located under `src/testFixtures/proto/schemas/compatible`:
          - <name>.proto: proto3 IDL. The first top-level message is the record type.
          - <name>.json:  { description, clickhouse_columns, clickhouse_order_by, records, expected_row_count }
                          Records follow protobuf JSON encoding (https://protobuf.dev/programming-guides/json/).

        To add a new compatible schema, add a matching <name>.proto + <name>.json pair into the directory above with the same structure as other files.
        The test picks up new fixtures automatically.

        To add a new incompatible schema, follow the same instructions as above but add the .json + .proto pair under `src/testFixtures/proto/schemas/incompatible`.
        Over time, the goal is to fix the connector to make previously incompatible schemas compatible wherever possible.
     */
    @ParameterizedTest
    @MethodSource("getCompatibleProtoSchemaPaths")
    public void protoSchemaTest(Path jsonPath) throws Exception {
        JSONObject fixture = new JSONObject(String.join("", Files.readAllLines(jsonPath)));

        // fixture JSON keys
        final String descriptionKey = "description";
        final String clickhouseColumnsKey = "clickhouse_columns";
        final String clickhouseOrderByKey = "clickhouse_order_by";
        final String recordsKey = "records";
        final String expectedRowCountKey = "expected_row_count";

        int expectedRowCount = fixture.getInt(expectedRowCountKey);

        // Derive basename and sibling .proto path
        String fileName = jsonPath.getFileName().toString();
        String baseName = fileName.replace(".json", "");
        Path protoPath = jsonPath.resolveSibling(baseName + ".proto");
        String protoSchema = String.join("\n", Files.readAllLines(protoPath));

        String topicName = "proto_test_" + baseName.replace("-", "_");

        LOGGER.info("Running protobuf integration test: {} (topic: {}, expected rows: {})", fixture.getString(descriptionKey), topicName, expectedRowCount);

        // 1. Create topic
        confluentPlatform.createTopic(topicName, 1);

        // 2. Create ClickHouse table
        ClickHouseTestHelpers.dropTable(chcNoProxy, topicName);
        CreateTableStatement tableStmt = new CreateTableStatement()
                .tableName(topicName)
                .engine("MergeTree")
                .orderByColumn(fixture.getString(clickhouseOrderByKey));
        JSONObject clickhouseColumns = fixture.getJSONObject(clickhouseColumnsKey);
        for (String colName : clickhouseColumns.keySet()) {
            tableStmt.column(colName, clickhouseColumns.getString(colName));
        }
        tableStmt.execute(chcNoProxy);

        // 3. Produce Protobuf records via REST proxy
        int producedCount = confluentPlatform.produceProtoRecords(
                topicName,
                protoSchema,
                fixture.getJSONArray(recordsKey)
        );
        LOGGER.info("Produced {} records to topic {}", producedCount, topicName);

        // 4. Setup sink connector with Protobuf converter
        setupProtobufConnector(topicName);

        // 5. Wait for data to flow through
        ClickHouseTestHelpers.waitWhileCounting(chcNoProxy, topicName, 3);

        // 6. Check for connector task failure
        Optional<String> taskFailure = confluentPlatform.getFirstTaskFailureOpt(SINK_CONNECTOR_NAME);
        if (taskFailure.isPresent()) {
            Assertions.fail(String.format("Connector task failed for %s: %s", fileName, taskFailure.orElseThrow()));
        }

        // 7. Check row count
        Assertions.assertEquals(expectedRowCount, ClickHouseTestHelpers.countRows(chcNoProxy, topicName));
    }

    private static enum SinkConfigs {
        BASE("clickhouse_sink.json"),
        BASE_SCHEMALESS("clickhouse_sink_schemaless.json"),
        PROTOBUF("clickhouse_sink_protobuf.json"),
        AVRO("clickhouse_sink_avro.json"),
        JDBC_PROP("clickhouse_sink_with_jdbc_prop.json");

        final Path pathToJsonConfig;
        SinkConfigs(String jsonFileName) {
            pathToJsonConfig = RESOURCES_ROOT.resolve(jsonFileName);
            Assertions.assertTrue(Files.exists(pathToJsonConfig));
        }

        public String getJsonPayload(int taskCount, String topicName) throws IOException {
            String configFormat = String.join("", Files.readAllLines(pathToJsonConfig));
            // The client makes requests with absolute URIs when a proxy is configured - currently, requests with absolute paths are rejected by CH server.
            // To work around this, transparently connect to the toxiproxy endpoint and avoid configuring the proxy settings on the client. The proxy will relay relative URIs, which the CH server expects.
            return String.format(configFormat, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName, "toxiproxy", 8666, db.getUsername(), db.getPassword());
        }
    }
}
