package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseCloudTest;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkTask;
import com.clickhouse.kafka.connect.sink.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ConfluentKafkaPlatform {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseCloudTest.class);

    private static final String CONFLUENT_VERSION = "7.7.x";

    private ComposeContainer platform;
    private String dbName;
    private int dbPort;
    private int connectPort;

    @BeforeAll
    public void setup() {

        platform = new ComposeContainer(new File("src/integrationTest/resources/kafka_compose.yaml"))
                .withExposedService("connect", 8083, Wait.forHttp("/connector-plugins"))
                .withExposedService("clickhouse", 8123)
                .withExposedService("connect", 9102)
        ;

        platform.start();

        dbName = "kafka_connect_int_test_" + System.currentTimeMillis();
        dbPort = platform.getServicePort("clickhouse", 8123);
        connectPort = platform.getServicePort("connect", 8083);
        int jmxPort = platform.getServicePort("connect", 9102);
        System.out.println("JMX port: " + jmxPort);
    }


    @AfterAll
    public void teardown() {
        platform.stop();
    }

    @Test
    void printConnectors() {
        Object response = queryConnect(connectPort, "/connector-plugins");
        System.out.println(response);

    }

    @Test
    void writeJson() {
        String topic = "test_json_events";

        createDatabase(dbName, "default", "");
        postDbQuery("CREATE TABLE " + dbName + "." + topic + "(id Int32, name String) ENGINE = MergeTree() ORDER BY id", "default", "");

        Map<String, String> config = createConnectorConfig();
        config.put("errors.tolerance", "all");
        config.put("exactlyOnce", "false");
        config.put("topics", topic);
        HttpResponse<String> createResponse = createConnector("test-connector", config);
        System.out.print(createResponse.body());

        Object response = queryConnect(connectPort, "/connectors");
        System.out.println(response);
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Object queryConnect(int port, String path) {
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + path))
                    .GET()
                    .build();

            HttpResponse response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return response.body();
            } else {
                throw new RuntimeException("Failed to query rest api: " + response.statusCode());
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public HttpResponse<String> createConnector(String name, Map<String, String> config) {
        ObjectNode connectorConfig = objectMapper.createObjectNode();
        for (Map.Entry<String, String> ce : config.entrySet()) {
            connectorConfig.put(ce.getKey(), ce.getValue());
        }

        ObjectNode requestObj = objectMapper.createObjectNode();
        requestObj.put("name", name);
        requestObj.set("config", connectorConfig);

        try {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + connectPort + "/connectors"))
                .header("Content-Type", "application/json;charset=UTF-8")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(requestObj)))
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        return response;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, String> createConnectorConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", ClickHouseSinkConnector.class.getName());
        config.put("connector.plugin.version", Version.ARTIFACT_VERSION);
        config.put("tasks.max", "1");
        String clientVersion = extractClientVersion();
        config.put(ClickHouseSinkConnector.CLIENT_VERSION, "V2");
        config.put(ClickHouseSinkConnector.DATABASE, dbName);
//        config.put(ClickHouseSinkConnector.DATABASE, "default");

        if (isCloud()) {
            config.put(ClickHouseSinkConnector.HOSTNAME, System.getenv("CLICKHOUSE_CLOUD_HOST"));
            config.put(ClickHouseSinkConnector.PORT, ClickHouseTestHelpers.HTTPS_PORT);
            config.put(ClickHouseSinkConnector.USERNAME, ClickHouseTestHelpers.USERNAME_DEFAULT);
            config.put(ClickHouseSinkConnector.PASSWORD, System.getenv("CLICKHOUSE_CLOUD_PASSWORD"));
            config.put(ClickHouseSinkConnector.SSL_ENABLED, "true");
            config.put(String.valueOf(ClickHouseClientOption.CONNECTION_TIMEOUT), "60000");
            config.put("clickhouseSettings", "insert_quorum=3");
        } else {
            config.put(ClickHouseSinkConnector.HOSTNAME, "clickhouse");
            config.put(ClickHouseSinkConnector.PORT, "8123");
            config.put(ClickHouseSinkConnector.USERNAME, "default");
            config.put(ClickHouseSinkConnector.PASSWORD, "");
            config.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        }
        return config;
    }

    public void createDatabase(String dbName, String user, String password) {
        postDbQuery("CREATE DATABASE " + dbName, user, password);
    }

    public void postDbQuery(String body, String user, String password) {
        String requestUrl = String.format("http://%s:%d/", "localhost" , dbPort);
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(requestUrl))
                    .header("Authorization", "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes()))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                System.out.println(response.body());
                throw new RuntimeException("Failed to create database: " + dbName + ", response code: " + response.statusCode());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String extractClientVersion() {
        String clientVersion = System.getenv("CLIENT_VERSION");
        if (clientVersion != null && clientVersion.equals("V1")) {
            return "V1";
        } else {
            return "V2";
        }
    }

    public static boolean isCloud() {
        String version = System.getenv("CLICKHOUSE_VERSION");
        LOGGER.info("ClickHouse Version: {}", version);
        return version != null && version.equalsIgnoreCase("cloud");
    }
}
