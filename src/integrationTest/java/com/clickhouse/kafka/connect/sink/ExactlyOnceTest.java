package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.query.Records;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI;
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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI.*;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ExactlyOnceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExactlyOnceTest.class);
    public static ConfluentPlatform confluentPlatform;
    private static ClickHouseAPI clickhouseAPI;
    private static ClickHouseHelperClient chcNoProxy;
    private static final Properties properties = System.getProperties();
    private static final String SINK_CONNECTOR_NAME = "ClickHouseSinkConnector";
    private static LinkedHashMap<String, String> stockSchema() {
        LinkedHashMap<String, String> s = new LinkedHashMap<>();
        s.put("side", "String"); s.put("quantity", "Int32"); s.put("symbol", "String");
        s.put("price", "Int32"); s.put("account", "String"); s.put("userid", "String");
        s.put("insertTime", "DateTime DEFAULT now()");
        return s;
    }

    @BeforeAll
    public static void checkPropsExistAndSetUp() {
        ClickHouseTestHelpers.logAndThrowIfPropNotExists(LOGGER, properties, ClickHouseTestHelpers.CLICKHOUSE_HOST);
        ClickHouseTestHelpers.logAndThrowIfPropNotExists(LOGGER, properties, ClickHouseTestHelpers.CLICKHOUSE_PORT);
        ClickHouseTestHelpers.logAndThrowIfPropNotExists(LOGGER, properties, ClickHouseTestHelpers.CLICKHOUSE_PASSWORD);

        chcNoProxy = new ClickHouseHelperClient.ClickHouseClientBuilder(properties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_HOST),
                Integer.parseInt(properties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_PORT)), ClickHouseProxyType.IGNORE, null, -1)
                .setUsername(ClickHouseTestHelpers.USERNAME_DEFAULT)
                .setPassword(properties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_PASSWORD))
                .sslEnable(true)
                .useClientV2(true)
                .build();
        clickhouseAPI = new ClickHouseAPI(properties);


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
        checkSpottyNetworkSchemaless("checkSpottyNetworkSinglePartition", 1);
    }

    @Test
    public void checkSpottyNetworkMulti() throws InterruptedException, IOException, URISyntaxException {
        checkSpottyNetworkSchemaless("checkSpottyNetworkMultiPartitions", 3);
    }

    private static void setupSchemaConnector(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting up connector...");
        setupConnector("src/integrationTest/resources/clickhouse_sink_no_proxy.json", topicName, taskCount);
        Thread.sleep(5 * 1000);
    }

    private static void setupSchemalessConnector(String topicName, int taskCount) throws IOException, InterruptedException {
        LOGGER.info("Setting schemaless up connector...");
        setupConnector("src/integrationTest/resources/clickhouse_sink_no_proxy_schemaless.json", topicName, taskCount);
        Thread.sleep(5 * 1000);
    }

    private static void setupConnector(String fileName, String topicName, int taskCount) throws IOException {
        System.out.println("Setting up connector...");
        dropTable(chcNoProxy, topicName);
        new CreateTableStatement() // implicitly SharedMergeTree in CH Cloud
                .setTableName(topicName).setSchema(stockSchema())
                .setEngine("MergeTree").setOrderByColumn("symbol").execute();

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get(fileName)));
        String jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                properties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_HOST),
                properties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_PORT),
                ClickHouseTestHelpers.DATABASE_DEFAULT,
                ClickHouseTestHelpers.USERNAME_DEFAULT,
                properties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_PASSWORD),
                true);

        confluentPlatform.createConnect(jsonString);
    }


    private int generateData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen.json", topicName, numberOfPartitions, numberOfRecords);
    }

    private int generateSchemalessData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen_json.json", topicName, numberOfPartitions, numberOfRecords);
    }

    private boolean compareSchemalessCounts(String topicName, int partitions) throws InterruptedException, IOException {
        new CreateTableStatement() // implicitly SharedMergeTree in CH Cloud
                .setTableName(topicName).setSchema(stockSchema()).setIfNotExists(true)
                .setEngine("MergeTree").setOrderByColumn("symbol").execute();
        ClickHouseAPI.clearTable(chcNoProxy, topicName);
        confluentPlatform.createTopic(topicName, partitions);
        int count = generateSchemalessData(topicName, partitions, 250);
        LOGGER.info("Expected Total: {}", count);
        setupSchemalessConnector(topicName, partitions);
        ClickHouseAPI.waitWhileCounting(chcNoProxy, topicName, 5);

        int[] databaseCounts = ClickHouseAPI.getCounts(chcNoProxy, topicName);//Essentially the final count
        ClickHouseAPI.dropTable(chcNoProxy, topicName);
        return databaseCounts[2] == 0 && databaseCounts[1] == count;
    }


    private void checkSpottyNetworkSchemaless(String topicName, int numberOfPartitions) throws InterruptedException, IOException, URISyntaxException {
        boolean allSuccess = true;
        int runCount = 1;
        do {
            LOGGER.info("Run: {}", runCount);
            confluentPlatform.createTopic(topicName, numberOfPartitions);
            new CreateTableStatement() // implicitly SharedMergeTree in CH Cloud
                .setTableName(topicName).setSchema(stockSchema()).setIfNotExists(true)
                .setEngine("MergeTree").setOrderByColumn("symbol").execute();
            ClickHouseAPI.clearTable(chcNoProxy, topicName);

            int count = generateSchemalessData(topicName, numberOfPartitions, 1500);
            setupSchemalessConnector(topicName, numberOfPartitions);

            clickhouseAPI.restartService();
            confluentPlatform.restartConnector(SINK_CONNECTOR_NAME);

            LOGGER.info("Expected Total: {}", count);
            ClickHouseAPI.waitWhileCounting(chcNoProxy, topicName, 7);

            int[] databaseCounts = ClickHouseAPI.getCounts(chcNoProxy, topicName);//Essentially the final count
            if (databaseCounts[2] != 0 || databaseCounts[1] != count) {
                allSuccess = false;
                LOGGER.error("Duplicates: {}", databaseCounts[2]);
                Records records = ClickHouseAPI.selectDuplicates(chcNoProxy, topicName);
                records.forEach(record -> LOGGER.error("Duplicate: {}", record));
            }

            confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
            confluentPlatform.deleteTopic(topicName);
            ClickHouseAPI.dropTable(chcNoProxy, topicName);
            runCount++;
        } while (runCount < 3 && allSuccess);

        assertTrue(allSuccess);
    }
}
