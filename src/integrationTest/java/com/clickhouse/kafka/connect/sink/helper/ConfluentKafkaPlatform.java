package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseCloudTest;
import com.clickhouse.kafka.connect.sink.Version;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class ConfluentKafkaPlatform {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseCloudTest.class);

    private ComposeContainer platform;
    private String dbName;
    private int dbPort;
    private int connectPort;
    private Client dbClient;
    private int brokerPort;

    public void setup() {

        platform = new ComposeContainer(new File("src/integrationTest/resources/kafka_compose.yaml"))
                .withExposedService("connect", 8083, Wait.forHttp("/connector-plugins"))
                .withExposedService("clickhouse", 8123)
                .withExposedService("connect", 9102)
                .withExposedService("broker", 9092)
        ;

        platform.start();

        dbName = "kafka_connect_int_test_" + System.currentTimeMillis();
        dbPort = platform.getServicePort("clickhouse", 8123);
        connectPort = platform.getServicePort("connect", 8083);
        brokerPort = platform.getServicePort("broker", 9092);

        final String dbClientEndpoint = isCloud() ? "https://" + System.getenv("CLICKHOUSE_CLOUD_HOST") + ":8433" : "http://localhost:" + dbPort;
        final String dbUserName = "default";
        final String dbUserPassword = isCloud() ? System.getenv("CLICKHOUSE_CLOUD_HOST") : "";
        dbClient = new Client.Builder()
                .addEndpoint(dbClientEndpoint)
                .setDefaultDatabase(dbName)
                .setUsername(dbUserName)
                .setPassword(dbUserPassword)
                .build();
        int jmxPort = platform.getServicePort("connect", 9102);
        System.out.println("JMX port: " + jmxPort);
    }

    public void teardown() {
        platform.stop();
    }

    public ComposeContainer getPlatform() {
        return platform;
    }

    public String getDbName() {
        return dbName;
    }

    public int getDbPort() {
        return dbPort;
    }

    public int getConnectPort() {
        return connectPort;
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Object httpQuery(int port, String path) {
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

    public void createTopic(String topicName) {
        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:" + brokerPort);

        try (AdminClient adminClient = AdminClient.create(config)) {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (Exception e) {
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
        postDbQuery("CREATE DATABASE " + dbName, user, password, null);
    }

    public void postDbQuery(String body, String user, String password, String database) {
        String requestUrl = String.format("http://%s:%d/", "localhost", dbPort);
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(requestUrl))
                    .header("Authorization", "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes()))
                    .header(ClickHouseHttpProto.HEADER_DATABASE, database == null ? "default" : database)
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

    public Map<String, Object> producerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("client.id", "client_1");
        config.put("bootstrap.servers", "localhost:" + brokerPort);
        config.put("acks", "all");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return config;
    }

    public Map<String, Object> consumerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:" + brokerPort);
        config.put("group.id", "test_group1");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return config;
    }

    public KafkaProducer<String, String> createStringProducer(Map<String, Object> config) {
        return new KafkaProducer<>(config, new StringSerializer(), new StringSerializer());
    }

    public List<ObjectNode> getTableRowsAsJson(String tableName) {
        String query = String.format("SELECT * FROM `%s`", tableName);
        QuerySettings querySettings = new QuerySettings();
        querySettings.setFormat(ClickHouseFormat.JSONEachRow);
        try (QueryResponse queryResponse = dbClient.query(query, querySettings).get()) {
            List<ObjectNode> jsonObjects = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new InputStreamReader(queryResponse.getInputStream()));
            String line;
            ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());
            while ((line = reader.readLine()) != null) {
                jsonObjects.add(jsonMapper.readValue(line, ObjectNode.class));
            }
            return jsonObjects;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void printContainerLogs(String serviceName) {
        String logs = platform.getContainerByServiceName(serviceName).get().getLogs();
        System.out.println(logs);
    }
}
