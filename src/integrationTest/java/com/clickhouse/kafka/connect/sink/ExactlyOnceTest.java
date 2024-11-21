package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI;
import com.clickhouse.kafka.connect.sink.helper.ConfluentPlatform;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI.createReplicatedMergeTreeTable;
import static com.clickhouse.kafka.connect.sink.helper.ClickHouseAPI.dropTable;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ExactlyOnceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExactlyOnceTest.class);
    public static ConfluentPlatform confluentPlatform;
    private static ClickHouseAPI clickhouseAPI;
    private static ClickHouseHelperClient chcNoProxy;
    private static final Properties properties = System.getProperties();
    private static final String SINK_CONNECTOR_NAME = "ClickHouseSinkConnector";

    @BeforeAll
    public static void setUp() {
        chcNoProxy = new ClickHouseHelperClient.ClickHouseClientBuilder(properties.getProperty("clickhouse.host"), Integer.parseInt(properties.getProperty("clickhouse.port")),
                ClickHouseProxyType.IGNORE, null, -1)
                .setUsername((String) properties.getOrDefault("clickhouse.username", "default"))
                .setPassword(properties.getProperty("clickhouse.password"))
                .sslEnable(true)
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
        confluentPlatform.close();
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
        createReplicatedMergeTreeTable(chcNoProxy, topicName);

        String payloadClickHouseSink = String.join("", Files.readAllLines(Paths.get(fileName)));
        String jsonString = String.format(payloadClickHouseSink, SINK_CONNECTOR_NAME, SINK_CONNECTOR_NAME, taskCount, topicName,
                properties.getOrDefault("clickhouse.host", "clickhouse"),
                properties.getOrDefault("clickhouse.port", ClickHouseProtocol.HTTP.getDefaultPort()),
                properties.getOrDefault("clickhouse.database", "default"),
                properties.getOrDefault("clickhouse.username", "default"),
                properties.getOrDefault("clickhouse.password", ""),
                true);

        confluentPlatform.createConnect(jsonString);
    }



    private int generateData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen.json", topicName, numberOfPartitions, numberOfRecords);
    }
    private int generateSchemalessData(String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        return confluentPlatform.generateData("src/integrationTest/resources/stock_gen_json.json", topicName, numberOfPartitions, numberOfRecords);
    }





    @BeforeEach
    public void beforeEach() throws IOException {
        confluentPlatform.deleteConnectors(SINK_CONNECTOR_NAME);
    }


    private boolean compareSchemalessCounts(String topicName, int partitions) throws InterruptedException, IOException {
        createReplicatedMergeTreeTable(chcNoProxy, topicName);
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

    @Test
    public void checkTotalsEqual() throws InterruptedException, IOException {
        assertTrue(compareSchemalessCounts("singlePartitionTopic", 1));
    }

    @Test
    public void checkTotalsEqualMulti() throws InterruptedException, IOException {
        assertTrue(compareSchemalessCounts("multiPartitionTopic", 3));
    }


    private void checkSpottyNetworkSchemaless(String topicName, int numberOfPartitions) throws InterruptedException, IOException, URISyntaxException {
        boolean allSuccess = true;
        int runCount = 1;
        do {
            LOGGER.info("Run: {}", runCount);
            confluentPlatform.createTopic(topicName, numberOfPartitions);
            createReplicatedMergeTreeTable(chcNoProxy, topicName);
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

    @Test
    public void checkSpottyNetwork() throws InterruptedException, IOException, URISyntaxException {
        checkSpottyNetworkSchemaless("checkSpottyNetworkSinglePartition", 1);
    }

    @Test
    public void checkSpottyNetworkMulti() throws InterruptedException, IOException, URISyntaxException {
        checkSpottyNetworkSchemaless("checkSpottyNetworkMultiPartitions", 3);
    }
}
