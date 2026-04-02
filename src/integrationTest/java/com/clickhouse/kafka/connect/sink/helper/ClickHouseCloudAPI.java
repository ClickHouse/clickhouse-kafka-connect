package com.clickhouse.kafka.connect.sink.helper;

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
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.fail;

public class ClickHouseCloudAPI {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseCloudAPI.class);

    private final Properties properties;
    private static final String CLICKHOUSE_CLOUD_API_HOST = "clickhouse.cloud.host";
    private static final String CLICKHOUSE_CLOUD_ORGANIZATION = "clickhouse.cloud.organization";
    private static final String CLICKHOUSE_CLOUD_API_KEY = "clickhouse.cloud.apiKey";
    private static final String CLICKHOUSE_CLOUD_API_SECRET = "clickhouse.cloud.secret";
    private static final String CLICKHOUSE_CLOUD_SERVICE_ID = "clickhouse.cloud.serviceId";

    public ClickHouseCloudAPI(Properties properties) {
        this.properties = properties;
    }

    public HttpResponse<String> stopInstance(String serviceId) throws URISyntaxException, IOException, InterruptedException {
        return updateServiceState(serviceId, "stop");
    }

    public HttpResponse<String> startInstance(String serviceId) throws URISyntaxException, IOException, InterruptedException {
        return updateServiceState(serviceId, "start");
    }

    public HttpResponse<String> updateServiceState(String serviceId, String command) throws URISyntaxException, IOException, InterruptedException {
        String restURL = "https://" + properties.getProperty(CLICKHOUSE_CLOUD_API_HOST) + "/v1/organizations/" + properties.getProperty(CLICKHOUSE_CLOUD_ORGANIZATION) + "/services/" + serviceId + "/state";
        String basicAuthCreds = Base64.getEncoder().encodeToString((properties.getProperty(CLICKHOUSE_CLOUD_API_KEY) + ":" + properties.getProperty(CLICKHOUSE_CLOUD_API_SECRET)).getBytes());
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(restURL))
                .header("Content-Type", "application/json;charset=UTF-8")
                .header("Authorization", "Basic " + basicAuthCreds)
                .method("PATCH", HttpRequest.BodyPublishers.ofString("{\"command\": \"" + command + "\"}"))
                .build();
        return HttpClient.newBuilder().proxy(ProxySelector.getDefault()).build().send(request, HttpResponse.BodyHandlers.ofString());
    }


    public String getServiceState(String serviceId) throws URISyntaxException, IOException, InterruptedException {
        String restURL = "https://" + properties.getProperty(CLICKHOUSE_CLOUD_API_HOST) + "/v1/organizations/" + properties.getProperty(CLICKHOUSE_CLOUD_ORGANIZATION) + "/services/" + serviceId;
        String basicAuthCreds = Base64.getEncoder().encodeToString((properties.getProperty(CLICKHOUSE_CLOUD_API_KEY) + ":" + properties.getProperty(CLICKHOUSE_CLOUD_API_SECRET)).getBytes());
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(restURL))
                .header("Content-Type", "application/json;charset=UTF-8")
                .header("Authorization", "Basic " + basicAuthCreds)
                .GET()
                .build();
        HttpResponse<String> response = HttpClient.newBuilder().proxy(ProxySelector.getDefault()).build().send(request, HttpResponse.BodyHandlers.ofString());
        HashMap<String, Map> map = (new ObjectMapper()).readValue(response.body(), HashMap.class);
        return String.valueOf(map.get("result").get("state")).toUpperCase();
    }


    public String restartService() throws URISyntaxException, IOException, InterruptedException {
        LOGGER.info("Restarting service...");
        String serviceId = properties.getProperty(CLICKHOUSE_CLOUD_SERVICE_ID);
        //1. Stop Instance
        stopInstance(serviceId);

        //2. Wait
        String serviceState = getServiceState(serviceId);
        int loopCount = 0;
        while(!serviceState.equals("STOPPED") && loopCount < 60) {
            LOGGER.debug("Service State: {}", serviceState);
            Thread.sleep(5 * 1000);
            serviceState = getServiceState(serviceId);
            loopCount++;
        }

        //3. Start Instance
        startInstance(serviceId);
        serviceState = getServiceState(serviceId);
        loopCount = 0;
        while(!serviceState.equals("RUNNING")) {
            LOGGER.debug("Service State: {}", serviceState);
            Thread.sleep(5 * 1000);
            serviceState = getServiceState(serviceId);

            if (loopCount >= 60) {
                LOGGER.error("Exceeded the maximum number of loops.");
                fail("Exceeded the maximum number of loops.");
            }
            loopCount++;
        }

        LOGGER.info("Service restarted");
        return serviceState;
    }
}
