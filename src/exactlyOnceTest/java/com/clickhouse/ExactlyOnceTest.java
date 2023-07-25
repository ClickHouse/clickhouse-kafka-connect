package com.clickhouse;

import com.clickhouse.client.*;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.helpers.ClickHouseAPI;
import com.clickhouse.helpers.ConfluentAPI;
import com.clickhouse.helpers.ConnectAPI;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.*;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testcontainers.utility.DockerImageName;
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

    private static final Network network = Network.newNetwork();
    private static GenericContainer<?> connectContainer;

    private static ToxiproxyContainer proxyContainer;
    private static ToxiproxyClient proxyClient;
    private static Proxy proxy;

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

        proxyContainer = createToxiproxyContainer();
        proxyContainer.start();
        Thread.sleep(10000);
        LOGGER.info(String.valueOf(proxyContainer.getExposedPorts()));

        proxyClient = new ToxiproxyClient(proxyContainer.getHost(), proxyContainer.getControlPort());
        proxy = proxyClient.createProxy("clickhouse", "0.0.0.0:" + properties.getProperty("clickhouse.port"),
                properties.getProperty("clickhouse.host") + ":" + properties.getProperty("clickhouse.port"));
        LOGGER.info("Proxy: {}", proxy);

        Thread.sleep(10000);
        LOGGER.info("Proxy URL: {}",proxyContainer.getHost()+":"+proxyContainer.getMappedPort(Integer.parseInt(properties.getProperty("confluent.port"))));
        LOGGER.info("Proxy Upstream: {}", proxy.getUpstream());

        connectContainer = new GenericContainer<>("confluentinc/cp-kafka-connect:latest")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withNetwork(network)
                .withNetworkAliases("connect")
                .withExposedPorts(8083)
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", properties.getProperty("bootstrap.servers"))
//                .withEnv("CONNECT_BOOTSTRAP_SERVERS", proxyContainer.getHost()+":"+proxyContainer.getMappedPort(Integer.parseInt(properties.getProperty("confluent.port"))))
                .withEnv("CONNECT_SECURITY_PROTOCOL", properties.getOrDefault("security.protocol", "SASL_SSL").toString())
                .withEnv("CONNECT_CONSUMER_SECURITY_PROTOCOL", properties.getOrDefault("security.protocol", "SASL_SSL").toString())
                .withEnv("CONNECT_SASL_MECHANISM", properties.getOrDefault("sasl.mechanism", "PLAIN").toString())
                .withEnv("CONNECT_CONSUMER_SASL_MECHANISM", properties.getOrDefault("sasl.mechanism", "PLAIN").toString())
                .withEnv("CONNECT_SASL_JAAS_CONFIG", properties.getOrDefault("sasl.jaas.config", "").toString())
                .withEnv("CONNECT_CONSUMER_SASL_JAAS_CONFIG", properties.getOrDefault("sasl.jaas.config", "").toString())
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
                .withEnv("CONNECT_GROUP_ID", "connect-group" + topicCode)
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
        LOGGER.info(String.valueOf(clickhouseAPI.createTable("test_exactlyOnce_data_" + topicCode)));

        connectAPI = new ConnectAPI(properties, connectContainer);
        LOGGER.info(String.valueOf(connectAPI.createConnector("test_exactlyOnce_data_" + topicCode, false)));

        //TODO: Check programatically rather than just waiting for a while
        Thread.sleep(30000);//We need to make sure the topics exist before we start the producer
        producer = new KafkaProducer<>(properties);
    }

    @AfterAll
    public static void tearDown() throws IOException, URISyntaxException, InterruptedException {
        producer.close();
        connectContainer.stop();
        proxyContainer.stop();
        LOGGER.info(String.valueOf(confluentAPI.deleteTopic("test_exactlyOnce_configs_" + topicCode)));
        LOGGER.info(String.valueOf(confluentAPI.deleteTopic("test_exactlyOnce_offsets_" + topicCode)));
        LOGGER.info(String.valueOf(confluentAPI.deleteTopic("test_exactlyOnce_status_" + topicCode)));
        LOGGER.info(String.valueOf(confluentAPI.deleteTopic("test_exactlyOnce_data_" + topicCode)));
        LOGGER.info(String.valueOf(clickhouseAPI.dropTable("test_exactlyOnce_data_" + topicCode)));
    }

    @Test
    public void basicTest() throws InterruptedException, ExecutionException, TimeoutException {
        assertEquals(1, 1);
    }

    @Test
    public void checkTotalsEqual() throws InterruptedException, ExecutionException, TimeoutException {
        Integer count = sendDataToTopic("test_exactlyOnce_data_" + topicCode);

        Thread.sleep(60000);
        LOGGER.info("Actual Total: {}", count);
        String[] counts = clickhouseAPI.count("test_exactlyOnce_data_" + topicCode);
        if (counts != null) {
            LOGGER.info("Unique Counts: {}, Total Counts: {}, Difference: {}", Integer.parseInt(counts[0]), Integer.parseInt(counts[1]), Integer.parseInt(counts[2]));
            assertEquals(count, Integer.parseInt(counts[1]));
        } else {
            LOGGER.info("Counts are null");
            fail();
        }
    }

    @Test
    public void tryProxyConnection() {
        try {
            String proxyHost = proxyContainer.getHost();
            Integer proxyPort = proxyContainer.getMappedPort(Integer.parseInt(properties.getProperty("clickhouse.port")));

            Map<String, String> options = new HashMap<>();
            options.put("proxy_type", "HTTPS");
            options.put("proxy_host", proxyHost);
            options.put("proxy_port", Integer.toString(proxyPort));
            try (ClickHouseClient client = ClickHouseClient.builder().options(getClientOptions())
                    .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP)).build()) {
                ClickHouseNode server = createServer(proxyHost, String.valueOf(proxyPort), options);
                LOGGER.info("Server: {}", server);
                Assert.assertTrue(client.ping(server, 30000));
            }
        } catch (Exception e) {
            LOGGER.error("Error", e);
            fail();
        }
    }


    private Integer sendDataToTopic(String topicName) throws InterruptedException {
        int NUM_THREADS = Integer.parseInt(properties.getProperty("messages.threads"));
        int NUM_MESSAGES = Integer.parseInt(properties.getProperty("messages.per.thread"));
        int MESSAGE_INTERVAL_MS = Integer.parseInt(String.valueOf(properties.getOrDefault("messages.interval.ms", "1000")));
        int MIN_MESSAGES = Integer.parseInt(String.valueOf(properties.getOrDefault("messages.min", "1000")));

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
        }
        scheduler.shutdown();
        Thread.sleep(5000);
        return counter.get();
    }

    private static ToxiproxyContainer createToxiproxyContainer() {
        return new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withNetwork(network)
                .withExposedPorts(8474, Integer.parseInt(properties.getProperty("confluent.port")), Integer.parseInt(properties.getProperty("clickhouse.port")))
//                .waitingFor(Wait.forHttp("/proxies").forStatusCode(200))
                .withStartupTimeout(Duration.ofMinutes(1));
    }



    private static ClickHouseNode createServer(String hostname, String port, Map<String, String> opts) {
        String protocol = "https";
        String url = String.format("%s://%s:%s/%s", protocol, hostname, port, properties.getProperty("clickhouse.database"));
        LOGGER.info("ClickHouse URL: " + url);
        Map<String, String> options = new HashMap<>();
        options.put("user", properties.getProperty("clickhouse.username"));
        options.put("password", properties.getProperty("clickhouse.password"));
        options.put("sslmode", "none");
        options.putAll(opts);
        return ClickHouseNode.of(url, options);
    }


    private static Map<ClickHouseOption, Serializable> getClientOptions() {
        return Collections.singletonMap(ClickHouseHttpOption.CONNECTION_PROVIDER,
                HttpConnectionProvider.HTTP_URL_CONNECTION);
    }
}
