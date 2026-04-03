package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.client.*;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseFieldDescriptor;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Type;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.Assumptions;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClickHouseTestHelpers {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseTestHelpers.class);
    public static final String CLICKHOUSE_VERSION_DEFAULT = "25.8.18.1"; // keep this as close to the latest LTS version
    public static final String CLICKHOUSE_DOCKER_IMAGE = String.format("clickhouse/clickhouse-server:%s", getClickhouseVersion());
    public static final String TOXIPROXY_DOCKER_IMAGE_NAME = "ghcr.io/shopify/toxiproxy:2.7.0";

    public static final String HTTPS_PORT = "8443";
    public static final String DATABASE_DEFAULT = "default";
    public static final String USERNAME_DEFAULT = "default";

    private static final int CLOUD_TIMEOUT_VALUE = 900;
    private static final TimeUnit CLOUD_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final String MISSING_CLOUD_PROP_MESSAGE_FORMAT = "%s system property is required to connect to cloud, skipping tests";

    // env vars
    public static final String CLIENT_VERSION = "CLIENT_VERSION";
    public static final String CLICKHOUSE_CLOUD_HOST = "CLICKHOUSE_CLOUD_HOST";
    public static final String CLICKHOUSE_CLOUD_PASSWORD = "CLICKHOUSE_CLOUD_PASSWORD";
    private static final String CLICKHOUSE_VERSION = "CLICKHOUSE_VERSION";

    public static final String CLICKHOUSE_DB_NETWORK_ALIAS = "clickhouse";
    public static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";

    // cloud integration test system props
    public static final String CLICKHOUSE_CLOUD_HOST_SYSTEM_PROP = "clickhouse.host";
    public static final String CLICKHOUSE_CLOUD_PORT_SYSTEM_PROP = "clickhouse.port";
    public static final String CLICKHOUSE_CLOUD_PASSWORD_SYSTEM_PROP = "clickhouse.password";

    public static String getClickhouseVersion() {
        String clickHouseVersion = System.getenv(CLICKHOUSE_VERSION);
        if (clickHouseVersion == null) {
            clickHouseVersion = CLICKHOUSE_VERSION_DEFAULT;
        }
        return clickHouseVersion;
    }

    public static boolean isCloud() {
        String version = System.getenv(CLICKHOUSE_VERSION);
        LOGGER.info("Version: {}", version);
        return version != null && version.equalsIgnoreCase("cloud");
    }

    public static void query(ClickHouseHelperClient chc, String query) {
        try (Records ignored = chc.queryV2(query)) {
            // success
        } catch (Exception e) {
            LOGGER.info("Failed to query ", e);
            throw new RuntimeException(e);
        }

    }

    public static OperationMetrics dropTable(ClickHouseHelperClient chc, String tableName) {
        for (int i = 0; i < 5; i++) {
            try {
                OperationMetrics operationMetrics = dropTableLoop(chc, tableName);
                if (operationMetrics != null) {
                    return operationMetrics;
                }
            } catch (Exception e) {
                LOGGER.error("Error while sleeping", e);
            }

            try {
                Thread.sleep(30000);//Sleep for 30 seconds
            } catch (InterruptedException e) {
                LOGGER.error("Error while sleeping", e);
            }
        }

        return null;
    }

    private static OperationMetrics dropTableLoop(ClickHouseHelperClient chc, String tableName) {
        String dropTable = String.format("DROP TABLE IF EXISTS `%s`", tableName);
        try {
            return chc.getClient().queryRecords(dropTable).get(CLOUD_TIMEOUT_VALUE, CLOUD_TIMEOUT_UNIT).getMetrics();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static List<JSONObject> getAllRowsAsJson(ClickHouseHelperClient chc, String tableName) {
        String query = String.format("SELECT * FROM `%s`", tableName);
        QuerySettings querySettings = new QuerySettings();
        querySettings.setFormat(ClickHouseFormat.JSONEachRow);
        try {
            QueryResponse queryResponse = chc.getClient().query(query, querySettings).get();
            List<JSONObject> jsonObjects = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new InputStreamReader(queryResponse.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                JSONObject jsonObject = new JSONObject(line);
                jsonObjects.add(jsonObject);
            }
            return jsonObjects;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<JSONObject> getAllRowsAsJsonCloud(ClickHouseHelperClient chc, String tableName) {
        String query = getClusterAllReplicasQuery(chc, tableName);
        QuerySettings querySettings = new QuerySettings();
        querySettings.setFormat(ClickHouseFormat.JSONEachRow);
        try {
            QueryResponse queryResponse = chc.getClient().query(query, querySettings).get();
            List<JSONObject> jsonObjects = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new InputStreamReader(queryResponse.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                JSONObject jsonObject = new JSONObject(line);
                jsonObjects.add(jsonObject);
            }
            return jsonObjects;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getClusterAllReplicasQuery(ClickHouseHelperClient chc, String tableName) {
        if (isVersionAtLeast(chc.version(), 26, 2)) {
            String escapedDatabase = escapeSingleQuotes(chc.getDatabase());
            String escapedTableName = escapeSingleQuotes(tableName);
            return String.format("SELECT * FROM clusterAllReplicas('default', '%s', '%s')", escapedDatabase, escapedTableName);
        }

        return String.format("SELECT * FROM clusterAllReplicas('default', `%s`)", tableName);
    }

    private static String escapeSingleQuotes(String input) {
        return input == null ? "" : input.replace("'", "''");
    }

    private static boolean isVersionAtLeast(String version, int requiredMajor, int requiredMinor) {
        if (version == null || version.isBlank()) {
            return false;
        }

        String[] parts = version.split("\\.");
        if (parts.length < 2) {
            return false;
        }

        try {
            int major = Integer.parseInt(parts[0]);
            int minor = Integer.parseInt(parts[1]);
            if (major != requiredMajor) {
                return major > requiredMajor;
            }
            return minor >= requiredMinor;
        } catch (NumberFormatException e) {
            LOGGER.warn("Failed to parse ClickHouse version '{}'", version, e);
            return false;
        }
    }


    public static OperationMetrics optimizeTable(ClickHouseHelperClient chc, String tableName) {
        String queryCount = String.format("OPTIMIZE TABLE `%s`", tableName);

        try {
            Records records = chc.getClient().queryRecords(queryCount).get(CLOUD_TIMEOUT_VALUE, CLOUD_TIMEOUT_UNIT);
            return records.getMetrics();
        } catch (Exception e) {
            return null;
        }
    }

    public static int countRows(ClickHouseHelperClient chc, String database, String topic) {
        String queryCount = String.format("select count(*) from `%s.%s`", database, topic);
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.read(chc.getServer())
                     .query(queryCount)
                     .executeAndWait()) {
            return response.firstRecord().getValue(0).asInteger();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public static int countRows(ClickHouseHelperClient chc, String tableName) {
        String queryCount = String.format("SELECT COUNT(*) FROM `%s` SETTINGS select_sequential_consistency = 1", tableName);

        try {
            optimizeTable(chc, tableName);
            Records records = chc.getClient().queryRecords(queryCount).get(CLOUD_TIMEOUT_VALUE, CLOUD_TIMEOUT_UNIT);
            // Note we probrbly need asInteger() here
            String value = records.iterator().next().getString(1);
            return Integer.parseInt(value);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            LOGGER.error("Error while counting rows. Query was " + queryCount, e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOGGER.error("Error while counting rows. Query was " + queryCount, e);
            throw new RuntimeException(e);
        }
    }

    public static int sumRows(ClickHouseHelperClient chc, String tableName, String column) {
        String queryCount = String.format("SELECT SUM(`%s`) FROM `%s`", column, tableName);
        try {
            Records records = chc.getClient().queryRecords(queryCount).get(CLOUD_TIMEOUT_VALUE, CLOUD_TIMEOUT_UNIT);
            String value = records.iterator().next().getString(1);
            return (int) (Float.parseFloat(value));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int countRowsWithEmojis(ClickHouseHelperClient chc, String tableName) {
        String queryCount = "SELECT COUNT(*) FROM `" + tableName + "` WHERE str LIKE '%\uD83D\uDE00%' SETTINGS select_sequential_consistency = 1";
        try {
            Records records = chc.getClient().queryRecords(queryCount).get(CLOUD_TIMEOUT_VALUE, CLOUD_TIMEOUT_UNIT);
            String value = records.iterator().next().getString(1);
            return (int) (Float.parseFloat(value));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean validateRows(ClickHouseHelperClient chc, String topic, Collection<SinkRecord> sinkRecords) {
        boolean match = false;
        try {
            QuerySettings querySettings = new QuerySettings();
            querySettings.setFormat(ClickHouseFormat.JSONStringsEachRow);
            QueryResponse queryResponse = chc.getClient().query(String.format("SELECT * FROM `%s`", topic), querySettings).get(CLOUD_TIMEOUT_VALUE, CLOUD_TIMEOUT_UNIT);
            Gson gson = new Gson();

            List<String> records = new ArrayList<>();
            for (SinkRecord record : sinkRecords) {
                Map<String, String> recordMap = new TreeMap<>();
                if (record.value() instanceof HashMap) {
                    for (Map.Entry<String, Object> entry : ((HashMap<String, Object>) record.value()).entrySet()) {
                        recordMap.put(entry.getKey(), entry.getValue().toString());
                    }
                } else if (record.value() instanceof Struct) {
                    ((Struct) record.value()).schema().fields().forEach(f -> {
                        recordMap.put(f.name(), ((Struct) record.value()).get(f).toString());
                    });
                }

                String gsonString = gson.toJson(recordMap);
                records.add(gsonString.replace(".0", "").replace(" ", "").replace("'", "").replace("\\u003d", ":"));
            }
            List<String> results = new ArrayList<>();
            LOGGER.info("read rows [%d]", queryResponse.getReadRows());
            BufferedReader reader = new BufferedReader(new InputStreamReader(queryResponse.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                String gsonString = line.replace("'", "").replace(" ", "").replace("\\u003d", ":");
                Map<String, String> resultMap = new TreeMap<>((Map<String, String>) gson.fromJson(gsonString, new TypeToken<Map<String, String>>() {
                }.getType()));
                results.add(gson.toJson(resultMap));
            }
            for (String record : records) {
                if (results.get(0).equals(record)) {
                    match = true;
                    LOGGER.info("Matched record: {}", record);
                    LOGGER.info("Matched result: {}", results.get(0));
                    break;
                }
            }

            LOGGER.info("Match? {}", match);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return match;
    }

    public static int countInsertQueries(ClickHouseHelperClient chc, String topic) {
        try (Client client = chc.getClient()) {
            String sql = String.format("SELECT COUNT(*) " +
                    "FROM system.query_log " +
                    "WHERE type = 'QueryFinish' " +
                    "AND query_kind = 'Insert' " +
                    "AND query ILIKE '%%%s%%'", topic);
            GenericRecord result = client.queryAll(sql).get(0);
            return result.getInteger(1);
        }
    }


    @Deprecated(since = "for debug purposes only")
    public static void showRows(ClickHouseHelperClient chc, String topic) {
        String queryCount = String.format("select * from `%s`", topic);
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.read(chc.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format
                     .query(queryCount)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();
            response.records().forEach(r -> {
                //int colsCount = r.size();
                System.out.println(r.getValue(0));
            });
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public static ClickHouseFieldDescriptor newDescriptor(String name, String valueType) {
        return ClickHouseFieldDescriptor
                .builder()
                .name(name)
                .type(valueType)
                .isSubcolumn(name.contains("."))
                .build();
    }

    public static ClickHouseFieldDescriptor newDescriptor(String valueType) {
        return ClickHouseFieldDescriptor
                .builder()
                .name("columnName")
                .type(valueType)
                .build();
    }

    public static Column col(Type type) {
        return Column.builder().type(type).build();
    }

    public static Column col(Type type, int precision, int scale) {
        return Column.builder().type(type).precision(precision).scale(scale).build();
    }

    public static boolean checkSequentialRows(ClickHouseHelperClient chc, String tableName, int totalRecords) {
        String queryCount = String.format("SELECT DISTINCT `off16` FROM `%s` ORDER BY `off16` ASC", tableName);
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer())
                     .query(queryCount)
                     .executeAndWait()) {

            int expectedIndexCount = 0;
            for (ClickHouseRecord record : response.records()) {
                int currentIndexCount = record.getValue(0).asInteger();
                if (currentIndexCount != expectedIndexCount) {
                    LOGGER.error("currentIndexCount: {}, expectedIndexCount: {}", currentIndexCount, expectedIndexCount);
                    return false;
                }
                expectedIndexCount++;
            }

            LOGGER.info("Total Records: {}, expectedIndexCount: {}", totalRecords, expectedIndexCount);
            return totalRecords == expectedIndexCount;
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public static void logAndThrowIfCloudPropNotExists(Logger logger, Properties properties, String property) throws TestAbortedException {
        try {
            Assumptions.assumeTrue(properties.get(property) != null);
        } catch (TestAbortedException e) {
            final String warning = String.format(MISSING_CLOUD_PROP_MESSAGE_FORMAT, property);
            logger.warn(warning);
            throw e;
        }
    }

    public static void runQuery(ClickHouseHelperClient chc, String query) {
        try (Records ignored = chc.queryV2(query)) {
            // success
        } catch (Exception e) {
            LOGGER.info("Failed to create table ", e);
            throw new RuntimeException(e);
        }
    }

    public static void createDatabase(String database, ClickHouseHelperClient chc) {
        String createDatabaseQuery = "CREATE DATABASE IF NOT EXISTS `" + database + "`";
        try (Records ignored = chc.queryV2(createDatabaseQuery)) {
            // success
        } catch (Exception e) {
            LOGGER.info("Failed to create database ", e);
            throw new RuntimeException(e);
        }
    }

    public static void dropDatabase(ClickHouseHelperClient chc, String database) {
        String dropDatabaseQuery = "DROP DATABASE IF EXISTS `" + database + "`";
        try (Records ignored = chc.queryV2(dropDatabaseQuery)) {
            // success
        } catch (Exception e) {
            LOGGER.info("Failed to drop database ", e);
            throw new RuntimeException(e);
        }
    }

    public static void doPing(ClickHouseHelperClient chc) {
        boolean ping;
        int retry = 0;

        do {
            LOGGER.info("Pinging ClickHouse server to warm up for tests...");
            ping = chc.ping();
            if (!ping) {
                LOGGER.info("Unable to ping ClickHouse server, retrying... {}", retry);
            }
        } while (!ping && retry++ < 10);

        if (!ping) {
            throw new RuntimeException("Unable to ping ClickHouse server...");
        }
    }

    public static ClickHouseHelperClient createClient(Map<String, String> props) {
        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(props);

        String hostname = csc.getHostname();
        int port = csc.getPort();
        String database = csc.getDatabase();
        String username = csc.getUsername();
        String password = csc.getPassword();
        boolean sslEnabled = csc.isSslEnabled();
        int timeout = csc.getTimeout();
        String clientVersion = csc.getClientVersion();

        return new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port, csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
                .setDatabase(csc.getDatabase())
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .useClientV2("V2".equals(clientVersion))
                .setSslSocketSni(csc.getSslSocketSni())
                .build();
    }

    public static void waitWhileCounting(ClickHouseHelperClient chc, String tableName, int sleepInSeconds) throws InterruptedException {
        int count = countRows(chc, tableName);
        int lastCount = 0;
        int loopCount = 0;

        while (count != lastCount || loopCount < 5) {
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

    public static Records selectDuplicates(ClickHouseHelperClient chc, String tableName) {
        String queryString = String.format("SELECT `side`, `quantity`, `symbol`, `price`, `account`, `userid`, `insertTime`, COUNT(*) " +
                "FROM %s " +
                "GROUP BY `side`, `quantity`, `symbol`, `price`, `account`, `userid`, `insertTime` " +
                "HAVING COUNT(*) > 1", tableName);
        try {
            Records records = chc.getClient().queryRecords(queryString).get(10, TimeUnit.SECONDS);
            return records;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public static void clearTable(ClickHouseHelperClient chc, String tableName) {
        String sql = "TRUNCATE TABLE " + tableName;
        LOGGER.info("Clear table: " + sql);
        try (Records records = chc.getClient().queryRecords(sql).get(10, TimeUnit.SECONDS)) {
            LOGGER.info("Create: {}", records.getMetrics());
        } catch (Exception e) {
            throw new RuntimeException(e);
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
}
