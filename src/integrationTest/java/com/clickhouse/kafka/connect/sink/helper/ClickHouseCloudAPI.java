package com.clickhouse.kafka.connect.sink.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.org.awaitility.Awaitility;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class ClickHouseCloudAPI {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseCloudAPI.class);

    private final Properties properties;
    private static final String CLICKHOUSE_CLOUD_API_HOST = "clickhouse.cloud.host";
    private static final String CLICKHOUSE_CLOUD_ORGANIZATION = "clickhouse.cloud.organization";
    private static final String CLICKHOUSE_CLOUD_API_KEY = "clickhouse.cloud.apiKey";
    private static final String CLICKHOUSE_CLOUD_API_SECRET = "clickhouse.cloud.secret";
    private static final String CLICKHOUSE_CLOUD_SERVICE_ID = "clickhouse.cloud.serviceId";
    private static final String SERVICE_RESTART_TIMEOUT = "serviceRestartTimeout";
    // The API key is sent with every request, so only ever send it to a ClickHouse Cloud API host.
    private static final String CLOUD_API_HOST_SUFFIX = ".clickhouse.cloud";
    private static final String STATE_RUNNING = "RUNNING";
    private static final String STATE_STOPPED = "STOPPED";
    private static final String STATE_IDLE = "IDLE";
    private static final long POLL_DELAY_SECONDS = 2L;
    private static final long POLL_INTERVAL_SECONDS = 5L;
    private static final int MAX_ERROR_BODY_CHARS = 300;
    private final long serviceRestartTimeout;

    public ClickHouseCloudAPI(Properties properties) {
        this.properties = properties;

        String timeoutStr = properties.getProperty(SERVICE_RESTART_TIMEOUT);
        this.serviceRestartTimeout = timeoutStr == null ? 20L : Long.parseLong(timeoutStr);
    }

    /** True when every Cloud API property needed to read or change the service state is set. */
    public boolean isConfigured() {
        return Stream.of(CLICKHOUSE_CLOUD_API_HOST, CLICKHOUSE_CLOUD_ORGANIZATION, CLICKHOUSE_CLOUD_API_KEY,
                        CLICKHOUSE_CLOUD_API_SECRET, CLICKHOUSE_CLOUD_SERVICE_ID)
                .map(properties::getProperty)
                .allMatch(value -> value != null && !value.isBlank());
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

    /**
     * Leaves the configured service RUNNING whatever state an earlier run or idling left it in: a stopped service is
     * started (queries cannot wake it) and an idle one is woken through the data-plane ping endpoint (the API start
     * command does not wake an idle service).
     */
    public void ensureServiceRunning() {
        String serviceId = properties.getProperty(CLICKHOUSE_CLOUD_SERVICE_ID);
        AtomicBoolean startRequested = new AtomicBoolean(false);
        Awaitility.await("service running")
                .atMost(Duration.ofMinutes(serviceRestartTimeout))
                .pollDelay(Duration.ZERO)
                .pollInterval(FixedPollInterval.fixed(POLL_INTERVAL_SECONDS, TimeUnit.SECONDS))
                .until(() -> {
                    String state = getServiceState(serviceId);
                    if (STATE_STOPPED.equals(state) && !startRequested.getAndSet(true)) {
                        LOGGER.info("Service {} is stopped, starting it", serviceId);
                        startInstance(serviceId);
                    } else if (STATE_IDLE.equals(state)) {
                        LOGGER.info("Service {} is idle, waking it", serviceId);
                        pingDataPlane();
                    }
                    return STATE_RUNNING.equals(state);
                });
        LOGGER.info("Service {} is running", serviceId);
    }

    public void restartService() throws URISyntaxException, IOException, InterruptedException {
        LOGGER.info("Restarting service...");
        String serviceId = properties.getProperty(CLICKHOUSE_CLOUD_SERVICE_ID);
        try {
            stopInstance(serviceId);
            Awaitility.await("service stopped")
                    .atMost(Duration.ofMinutes(serviceRestartTimeout))
                    .pollDelay(Duration.ofSeconds(POLL_DELAY_SECONDS))
                    .pollInterval(FixedPollInterval.fixed(POLL_INTERVAL_SECONDS, TimeUnit.SECONDS))
                    .until(() -> STATE_STOPPED.equals(getServiceState(serviceId)));
            LOGGER.info("Service {} stopped", serviceId);

            startInstance(serviceId);
            Awaitility.await("service started")
                    .atMost(Duration.ofMinutes(serviceRestartTimeout))
                    .pollDelay(Duration.ofSeconds(POLL_DELAY_SECONDS))
                    .pollInterval(FixedPollInterval.fixed(POLL_INTERVAL_SECONDS, TimeUnit.SECONDS))
                    .until(() -> STATE_RUNNING.equals(getServiceState(serviceId)));
        } catch (Exception e) {
            // A failure between stop and start would leave the service stopped for every later run.
            LOGGER.warn("Restarting service {} failed; making sure it is running again", serviceId);
            try {
                ensureServiceRunning();
            } catch (Exception restoreFailure) {
                e.addSuppressed(restoreFailure);
            }
            throw e;
        }

        LOGGER.info("Service {} restarted", serviceId);
    }

    private void pingDataPlane() {
        String port = properties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PORT_SYSTEM_PROP, ClickHouseTestHelpers.HTTPS_PORT);
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(String.format("https://%s:%s/ping", properties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_HOST_SYSTEM_PROP), port)))
                    .timeout(Duration.ofSeconds(30))
                    .GET()
                    .build();
            HttpClient.newBuilder().proxy(ProxySelector.getDefault()).connectTimeout(Duration.ofSeconds(30)).build()
                    .send(request, HttpResponse.BodyHandlers.discarding());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // A waking service may refuse or time out the first pings; the state poll keeps trying.
            LOGGER.debug("Ping while waking the service failed: {}", e.getClass().getSimpleName());
        }
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

        HttpResponse<String> response = HttpClient.newBuilder().proxy(ProxySelector.getDefault()).build().send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            String responseBody = String.valueOf(response.body());
            throw new IllegalStateException(String.format("ClickHouse Cloud API %s %s returned HTTP %d: %s", method, response.uri().getPath(),
                    response.statusCode(), responseBody.length() > MAX_ERROR_BODY_CHARS ? responseBody.substring(0, MAX_ERROR_BODY_CHARS) + "..." : responseBody));
        }
        return response;
    }

    private String getBaseServiceEndpoint(String serviceId) {
        String apiHost = properties.getProperty(CLICKHOUSE_CLOUD_API_HOST);
        if (apiHost == null || !apiHost.endsWith(CLOUD_API_HOST_SUFFIX)) {
            throw new IllegalStateException(String.format("System property '%s' must be a ClickHouse Cloud API host ending in '%s' (e.g. api.clickhouse.cloud), got '%s'",
                    CLICKHOUSE_CLOUD_API_HOST, CLOUD_API_HOST_SUFFIX, apiHost));
        }
        return String.format("https://%s/v1/organizations/%s/services/%s", apiHost, properties.getProperty(CLICKHOUSE_CLOUD_ORGANIZATION), serviceId);
    }
}
