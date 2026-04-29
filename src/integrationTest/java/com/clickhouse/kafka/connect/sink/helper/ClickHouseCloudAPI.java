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
        return updateServiceState(serviceId, ClickHouseCloudCommand.STOP);
    }

    public HttpResponse<String> startInstance(String serviceId) throws URISyntaxException, IOException, InterruptedException {
        return updateServiceState(serviceId, ClickHouseCloudCommand.START);
    }

    public HttpResponse<String> updateServiceState(String serviceId, ClickHouseCloudCommand command) throws URISyntaxException, IOException, InterruptedException {
        String url = getBaseServiceEndpoint(serviceId) + "/state";
        HttpRequest.BodyPublisher body = HttpRequest.BodyPublishers.ofString(command.toRequestBody());
        return executeRequest(url, "PATCH", body);
    }


    public String getServiceState(String serviceId) throws URISyntaxException, IOException, InterruptedException {
        HttpResponse<String> response = executeRequest(getBaseServiceEndpoint(serviceId), "GET", HttpRequest.BodyPublishers.noBody());
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
        final int maxRetries = 60;
        while(!serviceState.equals("STOPPED") && loopCount < maxRetries) {
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

            if (loopCount >= maxRetries) {
                fail("Failed to restart service in time.");
            }
            loopCount++;
        }

        LOGGER.info("Service restarted");
        return serviceState;
    }

    private HttpResponse<String> executeRequest(String url, String method, HttpRequest.BodyPublisher body) throws URISyntaxException, IOException, InterruptedException {
        String basicAuthCreds = Base64.getEncoder().encodeToString((properties.getProperty(CLICKHOUSE_CLOUD_API_KEY) + ":" + properties.getProperty(CLICKHOUSE_CLOUD_API_SECRET)).getBytes());
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(new URI(url))
                .header("Content-Type", "application/json;charset=UTF-8")
                .header("Authorization", "Basic " + basicAuthCreds);

        switch (method) {
            case "PATCH": {
                requestBuilder.method("PATCH", body);
                break;
            }
            case "GET": {
                requestBuilder.GET();
                break;
            }
            default: throw new RuntimeException("Unsupported request method: " + method);
        }

        return HttpClient.newBuilder().proxy(ProxySelector.getDefault()).build().send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private String getBaseServiceEndpoint(String serviceId) {
        return String.format("https://%s/v1/organizations/%s/services/%s", properties.getProperty(CLICKHOUSE_CLOUD_API_HOST), properties.getProperty(CLICKHOUSE_CLOUD_ORGANIZATION), serviceId);
    }
}
