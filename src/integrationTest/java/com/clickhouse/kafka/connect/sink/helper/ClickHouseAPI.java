package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.client.*;
import com.clickhouse.data.ClickHouseRecord;
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

import static org.junit.jupiter.api.Assertions.fail;

public class ClickHouseAPI {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseAPI.class);

    private final Properties properties;

    public ClickHouseAPI(Properties properties) {
        this.properties = properties;
    }


    public static void waitWhileCounting(ClickHouseHelperClient chc, String tableName, int sleepInSeconds) throws InterruptedException {
        int count = countRows(chc, tableName);
        int lastCount = 0;
        int loopCount = 0;

        while(count != lastCount || loopCount < 5) {
            Thread.sleep(sleepInSeconds * 1000L);
            count = countRows(chc, tableName);
            if (lastCount == count) {
                loopCount++;
            } else {
                loopCount = 0;
            }

            lastCount = count;
        }
    }

    public static int countRows(ClickHouseHelperClient chc, String tableName) {
        int[] counts = getCounts(chc, tableName);
        System.out.println("Total: " + counts[0] + " Unique: " + counts[1] + " Difference: " + counts[2]);
        return counts[0];
    }



    public static void dropTable(ClickHouseHelperClient chc, String tableName) {
        String queryString = String.format("DROP TABLE IF EXISTS %s", tableName);
        try(ClickHouseResponse clickHouseResponse = chc.query(queryString)) {
            LOGGER.info("Drop: {}", clickHouseResponse.getSummary());
        }
    }

    public static void createMergeTreeTable(ClickHouseHelperClient chc, String tableName) {
        String queryString = String.format("CREATE TABLE IF NOT EXISTS %s ( `side` String, `quantity` Int32, `symbol` String, `price` Int32, `account` String, `userid` String, `insertTime` DateTime DEFAULT now() ) " +
                "Engine = MergeTree ORDER BY symbol", tableName);
        try(ClickHouseResponse clickHouseResponse = chc.query(queryString)) {
            LOGGER.info("Create: {}", clickHouseResponse.getSummary());
        }
    }

    public static void createReplicatedMergeTreeTable(ClickHouseHelperClient chc, String tableName) {
        String queryString = String.format("CREATE TABLE IF NOT EXISTS %s ( `side` String, `quantity` Int32, `symbol` String, `price` Int32, `account` String, `userid` String, `insertTime` DateTime DEFAULT now() ) " +
                "Engine = ReplicatedMergeTree ORDER BY symbol", tableName);
        try(ClickHouseResponse clickHouseResponse = chc.query(queryString)) {
            LOGGER.info("Create: {}", clickHouseResponse.getSummary());
        }
    }

    public static Iterable<ClickHouseRecord> selectDuplicates(ClickHouseHelperClient chc, String tableName) {
        String queryString = String.format("SELECT `side`, `quantity`, `symbol`, `price`, `account`, `userid`, `insertTime`, COUNT(*) " +
                "FROM %s " +
                "GROUP BY `side`, `quantity`, `symbol`, `price`, `account`, `userid`, `insertTime` " +
                "HAVING COUNT(*) > 1", tableName);
        try(ClickHouseResponse clickHouseResponse = chc.query(queryString)) {
            return clickHouseResponse.records();
        }
    }



    public static void clearTable(ClickHouseHelperClient chc, String tableName) {
        String sql = "TRUNCATE TABLE " + tableName;
        LOGGER.info("Clear table: " + sql);
        try(ClickHouseResponse clickHouseResponse = chc.query(sql)) {
            LOGGER.info("Clear table: " + clickHouseResponse.getSummary().toString());
        } catch (Exception e) {
            LOGGER.error("Error: {}", e.getMessage());
        }
    }

    public static int[] getCounts(ClickHouseHelperClient chc, String tableName) {
        String queryCount = String.format("SELECT count(*) as total, uniqExact(*) as uniqueTotal, total - uniqueTotal FROM `%s`", tableName);
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer())
                     .query(queryCount)
                     .executeAndWait()) {
            return Arrays.stream(response.firstRecord().getValue(0).asString().split("\t")).mapToInt(Integer::parseInt).toArray();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }


    public HttpResponse<String> stopInstance(String serviceId) throws URISyntaxException, IOException, InterruptedException {
        return updateServiceState(serviceId, "stop");
    }

    public HttpResponse<String> startInstance(String serviceId) throws URISyntaxException, IOException, InterruptedException {
        return updateServiceState(serviceId, "start");
    }

    public HttpResponse<String> updateServiceState(String serviceId, String command) throws URISyntaxException, IOException, InterruptedException {
        String restURL = "https://" + properties.getProperty("clickhouse.cloud.host") + "/v1/organizations/" + properties.getProperty("clickhouse.cloud.organization") + "/services/" + serviceId + "/state";
        String basicAuthCreds = Base64.getEncoder().encodeToString((properties.getProperty("clickhouse.cloud.id") + ":" + properties.getProperty("clickhouse.cloud.secret")).getBytes());
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(restURL))
                .header("Content-Type", "application/json;charset=UTF-8")
                .header("Authorization", "Basic " + basicAuthCreds)
                .method("PATCH", HttpRequest.BodyPublishers.ofString("{\"command\": \"" + command + "\"}"))
                .build();
        return HttpClient.newBuilder().proxy(ProxySelector.getDefault()).build().send(request, HttpResponse.BodyHandlers.ofString());
    }


    public String getServiceState(String serviceId) throws URISyntaxException, IOException, InterruptedException {
        String restURL = "https://" + properties.getProperty("clickhouse.cloud.host") + "/v1/organizations/" + properties.getProperty("clickhouse.cloud.organization") + "/services/" + serviceId;
        String basicAuthCreds = Base64.getEncoder().encodeToString((properties.getProperty("clickhouse.cloud.id") + ":" + properties.getProperty("clickhouse.cloud.secret")).getBytes());
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
        String serviceId = properties.getProperty("clickhouse.cloud.serviceId");
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
