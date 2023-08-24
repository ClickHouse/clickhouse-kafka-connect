package com.clickhouse;

import com.clickhouse.client.*;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.helpers.ClickHouseAPI;
import com.clickhouse.helpers.ConfluentAPI;
import com.clickhouse.helpers.ConnectAPI;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.*;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testcontainers.utility.MountableFile;

import java.io.*;
import java.net.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;


public class ExactlyOnceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExactlyOnceTest.class);
    static final Properties properties = loadProperties();
    public static final String topicCode = RandomStringUtils.randomAlphanumeric(8);
    public static final String CONSUMER_GROUP = "connect-group" + topicCode;
    public static final String singlePartitionTopic = "test_exactlyOnce_data_" + topicCode;
    public static final String multiPartitionTopic = "test_exactlyOnce_data_multi_" + topicCode;

    private static final Network network = Network.newNetwork();
    private static GenericContainer<?> connectContainer;

    private static final String VERSION;

    static {
        try {
            VERSION = FileUtils.readFileToString(new File("VERSION"), "UTF-8").trim();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    private static ConfluentAPI confluentAPI;
    private static ConnectAPI connectAPI;
    private static ClickHouseAPI clickhouseAPI;

    private static Producer<String, String> producer;

    private static Properties loadProperties() {
        try {
            Properties cfg = new Properties();
            try (InputStream inputStream = new FileInputStream("build/resources/exactlyOnceTest/exactlyOnce.local.properties")) {
                cfg.load(inputStream);
            }
            LOGGER.info(String.valueOf(cfg));
            return cfg;
        } catch (IOException e) {
            LOGGER.info("File does not exist");
            LOGGER.info(String.valueOf(System.getProperties()));
            return System.getProperties();
        }
    }

    @BeforeAll
    public static void setUp() throws IOException, URISyntaxException, InterruptedException {
        LOGGER.info("Version: " + VERSION);
        LOGGER.info("Topic code: " + topicCode);

        confluentAPI = new ConfluentAPI(properties);
        LOGGER.info(String.valueOf(confluentAPI.createTopic("test_exactlyOnce_configs_" + topicCode, 1, 3, true, false)));
        LOGGER.info(String.valueOf(confluentAPI.createTopic("test_exactlyOnce_offsets_" + topicCode, 1, 3, true, false)));
        LOGGER.info(String.valueOf(confluentAPI.createTopic("test_exactlyOnce_status_" + topicCode, 1, 3, true, false)));
        LOGGER.info(String.valueOf(confluentAPI.createTopic("test_exactlyOnce_data_" + topicCode, 1, 3, false, false)));
        LOGGER.info(String.valueOf(confluentAPI.createTopic("test_exactlyOnce_data_multi_" + topicCode, 3, 3, false, false)));

        Thread.sleep(15000);

        connectContainer = new GenericContainer<>("confluentinc/cp-kafka-connect:latest")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withNetwork(network)
                .withNetworkAliases("connect")
                .withExposedPorts(8083)
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", properties.getProperty("bootstrap.servers"))
                .withEnv("CONNECT_SECURITY_PROTOCOL", properties.getOrDefault("security.protocol", "SASL_SSL").toString())
                .withEnv("CONNECT_CONSUMER_SECURITY_PROTOCOL", properties.getOrDefault("security.protocol", "SASL_SSL").toString())
                .withEnv("CONNECT_SASL_MECHANISM", properties.getOrDefault("sasl.mechanism", "PLAIN").toString())
                .withEnv("CONNECT_CONSUMER_SASL_MECHANISM", properties.getOrDefault("sasl.mechanism", "PLAIN").toString())
                .withEnv("CONNECT_SASL_JAAS_CONFIG", properties.getOrDefault("sasl.jaas.config", "").toString())
                .withEnv("CONNECT_CONSUMER_SASL_JAAS_CONFIG", properties.getOrDefault("sasl.jaas.config", "").toString())
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
                .withEnv("CONNECT_GROUP_ID", CONSUMER_GROUP)
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "test_exactlyOnce_configs_" + topicCode)
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "test_exactlyOnce_offsets_" + topicCode)
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "test_exactlyOnce_status_" + topicCode)
                .withEnv("CONNECT_OFFSET_FLUSH_INTERVAL_MS", "10000")
                .withEnv("CONNECT_CONSUMER_MAX_POLL_RECORDS", "5000")
                .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
                .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/confluent-hub-components,/usr/share/dockershare")
                .withEnv("CONNECT_LOG4J_LOGGERS", "org.apache.zookeeper=ERROR,org.I0Itec.zkclient=ERROR,org.reflections=ERROR,com.clickhouse=DEBUG")
                .withCopyToContainer(MountableFile.forHostPath("build/confluentArchive/clickhouse-kafka-connect-" + VERSION), "/usr/share/dockershare/")
                .waitingFor(Wait.forHttp("/connectors").forStatusCode(200))
                .withStartupTimeout(Duration.ofMinutes(1));
        connectContainer.start();

        clickhouseAPI = new ClickHouseAPI(properties);
        LOGGER.info(String.valueOf(clickhouseAPI.createTable(singlePartitionTopic)));
        LOGGER.info(String.valueOf(clickhouseAPI.createTable(multiPartitionTopic)));

        connectAPI = new ConnectAPI(properties, connectContainer);
        LOGGER.info(String.valueOf(connectAPI.createConnector(singlePartitionTopic, true, 1)));
        LOGGER.info(String.valueOf(connectAPI.createConnector(multiPartitionTopic, true, 3)));

        //TODO: Check programatically rather than just waiting for a while
        Thread.sleep(30 * 1000);//We need to make sure the topics exist before we start the producer
        producer = new KafkaProducer<>(properties);
    }

    @AfterAll
    public static void tearDown() throws IOException, URISyntaxException, InterruptedException {
        producer.close();
        connectContainer.stop();
        LOGGER.info(String.valueOf(confluentAPI.deleteTopic("test_exactlyOnce_configs_" + topicCode)));
        LOGGER.info(String.valueOf(confluentAPI.deleteTopic("test_exactlyOnce_offsets_" + topicCode)));
        LOGGER.info(String.valueOf(confluentAPI.deleteTopic("test_exactlyOnce_status_" + topicCode)));
        LOGGER.info(String.valueOf(confluentAPI.deleteTopic(singlePartitionTopic)));
        LOGGER.info(String.valueOf(confluentAPI.deleteTopic(multiPartitionTopic)));
        LOGGER.info(String.valueOf(clickhouseAPI.dropTable(singlePartitionTopic)));
        LOGGER.info(String.valueOf(clickhouseAPI.dropTable(multiPartitionTopic)));
    }


    private void compareCounts(String topicName) throws InterruptedException {
        ClickHouseResponse clickHouseResponse = clickhouseAPI.clearTable(topicName);
        LOGGER.info(String.valueOf(clickHouseResponse));
        clickHouseResponse.close();

        Integer count = sendDataToTopic(topicName);

        Thread.sleep(60 * 1000);
        LOGGER.info("Actual Total: {}", count);
        int[] counts = clickhouseAPI.count(topicName);
        if (counts != null) {
            assertEquals(count, counts[1]);
        } else {
            LOGGER.info("Counts are null");
            fail();
        }
    }

    @Test
    public void checkTotalsEqual() throws InterruptedException {
        compareCounts(singlePartitionTopic);
    }

    @Test
    public void checkTotalsEqualMulti() throws InterruptedException {
        compareCounts(multiPartitionTopic);
    }


    private void checkSpottyNetwork(String topicName) throws InterruptedException, IOException, URISyntaxException {
        boolean allSuccess = true;
        int runCount = 0;
        do {
            ClickHouseResponse clickHouseResponse = clickhouseAPI.clearTable(topicName);
            LOGGER.info(String.valueOf(clickHouseResponse));
            clickHouseResponse.close();

            Date start = new Date();
            Integer count = sendDataToTopic(topicName);
            Date end = new Date();
            LOGGER.info("Time to send: {}", end.getTime() - start.getTime());
            LOGGER.info("Connector State: {}", connectAPI.getConnectorState(topicName));
            clickhouseAPI.stopInstance(properties.getProperty("clickhouse.cloud.serviceId"));

            String serviceState = clickhouseAPI.getServiceState(properties.getProperty("clickhouse.cloud.serviceId"));
            int loopCount = 0;
            while(!serviceState.equals("STOPPED") && loopCount < 30) {
                LOGGER.info("Service State: {}", serviceState);
                Thread.sleep(5 * 1000);
                serviceState = clickhouseAPI.getServiceState(properties.getProperty("clickhouse.cloud.serviceId"));
                loopCount++;
            }

            clickhouseAPI.startInstance(properties.getProperty("clickhouse.cloud.serviceId"));
            serviceState = clickhouseAPI.getServiceState(properties.getProperty("clickhouse.cloud.serviceId"));
            loopCount = 0;
            while(!serviceState.equals("RUNNING")) {
                LOGGER.info("Service State: {}", serviceState);
                Thread.sleep(5 * 1000);
                serviceState = clickhouseAPI.getServiceState(properties.getProperty("clickhouse.cloud.serviceId"));

                if (loopCount >= 60) {
                    LOGGER.error("Exceeded the maximum number of loops.");
                    fail("Exceeded the maximum number of loops.");
                }
                loopCount++;
            }

            LOGGER.info("Service State: {}", serviceState);

            connectAPI.restartConnector(topicName);

            LOGGER.info("Expected Total: {}", count);
            int[] databaseCounts = clickhouseAPI.count(topicName);
            int lastCount = 0;
            loopCount = 0;
            while(databaseCounts[1] != lastCount || loopCount < 5) {
                Thread.sleep(5 * 1000);
                databaseCounts = clickhouseAPI.count(topicName);
                if (lastCount == databaseCounts[1]) {
                    loopCount++;
                } else {
                    loopCount = 0;
                }

                lastCount = databaseCounts[1];
            }

            databaseCounts = clickhouseAPI.count(topicName);
            if (databaseCounts != null) {
                LOGGER.info("Unique Counts: {}, Total Counts: {}, Difference: {}", databaseCounts[0], databaseCounts[1], databaseCounts[2]);
                LOGGER.info("Counts Difference: {}", databaseCounts[1] - count);

                if (databaseCounts[1] - count != 0) {
                    allSuccess = false;
                }
            } else {
                LOGGER.error("Counts are null");
                fail();
            }
            runCount++;
        } while (runCount < 3 && allSuccess);

        assertTrue(allSuccess);
    }



    @Test
    public void checkSpottyNetwork() throws InterruptedException, IOException, URISyntaxException {
        checkSpottyNetwork(singlePartitionTopic);
    }

    @Test
    public void checkSpottyNetworkMulti() throws InterruptedException, IOException, URISyntaxException {
        checkSpottyNetwork(multiPartitionTopic);
    }




    private Integer sendDataToTopic(String topicName) throws InterruptedException {
        int NUM_THREADS = Integer.parseInt(properties.getProperty("messages.threads"));
        int NUM_MESSAGES = Integer.parseInt(properties.getProperty("messages.per.thread"));
        int MESSAGE_INTERVAL_MS = Integer.parseInt(properties.getProperty("messages.interval.ms", "1000"));
        int MIN_MESSAGES = Integer.parseInt(properties.getProperty("messages.min", "1000"));

        AtomicInteger counter = new AtomicInteger(0);

        // Schedule tasks to send messages at a fixed rate using multiple threads
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(NUM_THREADS);
        for (int i = 0; i < NUM_THREADS; i++) {
            int threadId = i;
            Runnable sendMessageTask = () -> {
                for (int j = 0; j < NUM_MESSAGES; j++) {
                    String line = "{" +
                            "\"generationTimestamp\": \"" + Instant.now().toString() + "\"" +
                            ",\"raw\": " + "\"{\\\"user_id\\\": \\\"" + threadId + "-" + j + "-" + UUID.randomUUID() + "\\\"}\"" +
                            ",\"randomData\": " + "\"" + RandomStringUtils.randomAlphanumeric(0,2500) + "\"" +
                            "}";
                    producer.send(new ProducerRecord<>(topicName, line));
                }
                counter.addAndGet(NUM_MESSAGES);
            };
            scheduler.scheduleAtFixedRate(sendMessageTask, 0, MESSAGE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }

        while(counter.get() < MIN_MESSAGES) {
            Thread.sleep(1000);
            LOGGER.info("Messages sent: {}", counter.get());
        }
        scheduler.shutdown();
        Thread.sleep(5 * 1000);
        return counter.get();
    }



    private static Map<ClickHouseOption, Serializable> getClientOptions() {
        return Collections.singletonMap(ClickHouseHttpOption.CONNECTION_PROVIDER,
                HttpConnectionProvider.HTTP_URL_CONNECTION);
    }
}
