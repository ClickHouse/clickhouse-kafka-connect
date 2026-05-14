package com.clickhouse.kafka.connect.sink.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.shaded.org.awaitility.pollinterval.FibonacciPollInterval;
import org.testcontainers.shaded.org.awaitility.pollinterval.FixedPollInterval;

import java.io.IOException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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


    public void restartService() throws URISyntaxException, IOException, InterruptedException {
        LOGGER.info("Restarting service...");
        String serviceId = properties.getProperty(CLICKHOUSE_CLOUD_SERVICE_ID);
        stopInstance(serviceId);
        Awaitility.await("service stopped")
                .atMost(Duration.ofMinutes(20))
                .pollDelay(Duration.ofSeconds(2))
                .pollInterval(FixedPollInterval.fixed(5, TimeUnit.SECONDS))
                .until(() -> "STOPPED".equals(getServiceState(serviceId)));
        LOGGER.info("Service {} stopped", serviceId);

        startInstance(serviceId);
        Awaitility.await("service started")
                .atMost(Duration.ofMinutes(20))
                .pollDelay(Duration.ofSeconds(2))
                .pollInterval(FixedPollInterval.fixed(5, TimeUnit.SECONDS))
                .until(() -> "RUNNING".equals(getServiceState(serviceId)));

        LOGGER.info("Service {} restarted", serviceId);
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
