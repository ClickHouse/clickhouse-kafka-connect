package com.clickhouse.helpers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Properties;

public class ConnectAPI {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectAPI.class);

    private final Properties properties;
    private final GenericContainer container;

    public ConnectAPI(Properties properties, GenericContainer container) {
        this.properties = properties;
        this.container = container;
    }

    public String listConnectors() throws IOException, InterruptedException, URISyntaxException {
        String restURL = "http://" + container.getHost() + ":" + container.getMappedPort(8083) + "/connectors?expand=status&expand=info";
        LOGGER.info(restURL);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(restURL))
                .GET()
                .build();
        HttpResponse<String> response = HttpClient.newBuilder().proxy(ProxySelector.getDefault()).build().send(request, HttpResponse.BodyHandlers.ofString());

        LOGGER.info(String.valueOf(response.statusCode()));
        LOGGER.info(response.body());

        return response.body();
    }

    public boolean createConnector(String topicName, boolean exactlyOnce) throws IOException, InterruptedException, URISyntaxException {
        String restURL = "http://" + container.getHost() + ":" + container.getMappedPort(8083) + "/connectors";
        LOGGER.info(restURL);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(restURL))
                .header("Content-Type", "application/json;charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString("{" +
                        "\"name\": \"clickhouse-connect\"," +
                        "\"config\": {" +
                        "\"connector.class\": \"com.clickhouse.kafka.connect.ClickHouseSinkConnector\"," +
                        "\"tasks.max\": \"1\"," +
                        "\"database\": \"" + properties.getOrDefault("clickhouse.database", "default") + "\"," +
                        "\"errors.retry.timeout\": \"60\"," +
                        "\"exactlyOnce\": \"" + exactlyOnce + "\"," +
                        "\"hostname\": \"" + properties.getProperty("clickhouse.host") + "\"," +
                        "\"password\": \"" + properties.getProperty("clickhouse.password") + "\"," +
                        "\"port\": \"" + properties.getProperty("clickhouse.port") + "\"," +
                        "\"ssl\": \"true\"," +
                        "\"username\": \"" + properties.getOrDefault("clickhouse.username", "default") + "\"," +
                        "\"topics\": \"" + topicName + "\"," +
                        "\"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\"," +
                        "\"value.converter.schemas.enable\": \"false\"" +
                        "}}"))
                .build();
        HttpResponse<String> response = HttpClient.newBuilder().proxy(ProxySelector.getDefault()).build().send(request, HttpResponse.BodyHandlers.ofString());

        LOGGER.info(String.valueOf(response.statusCode()));
        LOGGER.info(response.body());

        return response.statusCode() == 201;
    }
}
