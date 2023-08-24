package com.clickhouse.helpers;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

public class ClickHouseAPI {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseAPI.class);

    private final ClickHouseHelperClient clickHouseHelperClient;
    private final Properties properties;

    public ClickHouseAPI(Properties properties) {
        this.properties = properties;
        clickHouseHelperClient = new ClickHouseHelperClient.ClickHouseClientBuilder(properties.getProperty("clickhouse.host"), Integer.parseInt(properties.getProperty("clickhouse.port")))
                .setUsername(String.valueOf(properties.getOrDefault("clickhouse.username", "default")))
                .setPassword(properties.getProperty("clickhouse.password"))
                .sslEnable(true)
                .build();
    }


    public ClickHouseResponse createTable(String tableName) {
        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "  `raw` String,"
                + "  `generationTimestamp` DateTime64(3),"
                + "  `insertTime` DateTime64(3) DEFAULT now(),"
                + ")"
                + " ORDER BY insertTime";
        LOGGER.info("Create table: " + sql);
        return clickHouseHelperClient.query(sql);
    }

    public ClickHouseResponse dropTable(String tableName) {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        LOGGER.info("Drop table: " + sql);
        return clickHouseHelperClient.query(sql);
    }

    public int[] count(String tableName) {
        String sql = "SELECT uniqExact(raw) as uniqueTotal, count(*) as total, total - uniqueTotal FROM " + tableName;
        LOGGER.info("Count table: " + sql);
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(clickHouseHelperClient.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format
                     .query(sql)
                     .executeAndWait()) {
//            ClickHouseResponseSummary summary = response.getSummary();
            String countAsString = response.firstRecord().getValue(0).asString();
            LOGGER.debug("Counts String: {}", countAsString);
            return Arrays.stream(countAsString.split("\t")).mapToInt(Integer::parseInt).toArray();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public ClickHouseResponse clearTable(String tableName) {
        String sql = "TRUNCATE TABLE " + tableName;
        LOGGER.info("Clear table: " + sql);
        return clickHouseHelperClient.query(sql);
    }


    public HttpResponse<String> stopInstance(String serviceId) throws URISyntaxException, IOException, InterruptedException {
        return updateServiceState(serviceId, "stop");
    }

    public HttpResponse<String> startInstance(String serviceId) throws URISyntaxException, IOException, InterruptedException {
        return updateServiceState(serviceId, "start");
    }

    public HttpResponse<String> updateServiceState(String serviceId, String command) throws URISyntaxException, IOException, InterruptedException {
        String restURL = "https://api.clickhouse.cloud/v1/organizations/" + properties.getProperty("clickhouse.cloud.organization") + "/services/" + serviceId + "/state";
        String basicAuthCreds = Base64.getEncoder().encodeToString((properties.getProperty("clickhouse.cloud.id") + ":" + properties.getProperty("clickhouse.cloud.secret")).getBytes());
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(restURL))
                .header("Content-Type", "application/json;charset=UTF-8")
                .header("Authorization", "Basic " + basicAuthCreds)
                .method("PATCH", HttpRequest.BodyPublishers.ofString("{\"command\": \"" + command + "\"}"))
                .build();
        HttpResponse<String> response = HttpClient.newBuilder().proxy(ProxySelector.getDefault()).build().send(request, HttpResponse.BodyHandlers.ofString());

        LOGGER.info(String.valueOf(response.statusCode()));
        LOGGER.info(response.body());

        return response;
    }

    public String getServiceState(String serviceId) throws URISyntaxException, IOException, InterruptedException {
        String restURL = "https://api.clickhouse.cloud/v1/organizations/" + properties.getProperty("clickhouse.cloud.organization") + "/services/" + serviceId;
        String basicAuthCreds = Base64.getEncoder().encodeToString((properties.getProperty("clickhouse.cloud.id") + ":" + properties.getProperty("clickhouse.cloud.secret")).getBytes());
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(restURL))
                .header("Content-Type", "application/json;charset=UTF-8")
                .header("Authorization", "Basic " + basicAuthCreds)
                .GET()
                .build();
        HttpResponse<String> response = HttpClient.newBuilder().proxy(ProxySelector.getDefault()).build().send(request, HttpResponse.BodyHandlers.ofString());

        LOGGER.info(String.valueOf(response.statusCode()));
        LOGGER.info(response.body());

        try {
            HashMap<String, Map> map = (new ObjectMapper()).readValue(response.body(), HashMap.class);
            LOGGER.info("Map: {}", String.valueOf(map.get("result")));
            return String.valueOf(map.get("result").get("state")).toUpperCase();
        } catch (Exception e) {
            LOGGER.error("Error: {}", e.getMessage());
            return "error";
        }
    }


}
