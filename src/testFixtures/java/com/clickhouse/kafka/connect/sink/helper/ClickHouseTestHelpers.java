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
import java.util.stream.Stream;

public class ClickHouseTestHelpers {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseTestHelpers.class);
    public static final String CLICKHOUSE_VERSION_DEFAULT = "25.8.18.1"; // keep this as close to the latest LTS version
    public static final String CLICKHOUSE_DOCKER_IMAGE = String.format("clickhouse/clickhouse-server:%s", getClickhouseVersion());
    public static final String TOXIPROXY_DOCKER_IMAGE_NAME = "ghcr.io/shopify/toxiproxy:2.7.0";

    public static final String HTTPS_PORT = "8443";
    public static final String DATABASE_DEFAULT = "default";
    public static final String USERNAME_DEFAULT = "default";

    private static final int QUERY_TIMEOUT = 900;
    private static final TimeUnit QUERY_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final String MISSING_CLOUD_PROP_MESSAGE_FORMAT = "%s system property is required to connect to cloud, skipping tests";

    // env vars
    public static final String CLIENT_VERSION = "CLIENT_VERSION";
    public static final String CLICKHOUSE_CLOUD_HOST = "CLICKHOUSE_CLOUD_HOST";
    public static final String CLICKHOUSE_CLOUD_PASSWORD = "CLICKHOUSE_CLOUD_PASSWORD";
    private static final String CLICKHOUSE_VERSION = "CLICKHOUSE_VERSION";
    private static final String CLICKHOUSE_CLUSTER_MODE = "CLICKHOUSE_CLUSTER_MODE";

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

    public static boolean isCluster() {
        String isClusterMode = System.getenv(CLICKHOUSE_CLUSTER_MODE);
        return isClusterMode != null && isClusterMode.equalsIgnoreCase("true");
    }

    /**
     * Returns the set of {@link ClickHouseDeploymentType} values to test against.
     * Used as a JUnit {@code @MethodSource} for {@code @ParameterizedTest}.
     *
     * <ul>
     *   <li>Non-cluster mode: {@code [STANDALONE]}</li>
     *   <li>Cluster mode: {@code [THREE_SHARDS_ONE_REPLICA_EACH, ONE_SHARD_THREE_REPLICAS]}</li>
     * </ul>
     */
    public static Stream<ClickHouseDeploymentType> clusterConfigs() {
        if (isCluster()) {
            return Stream.of(ClickHouseDeploymentType.THREE_SHARDS_ONE_REPLICA_EACH, ClickHouseDeploymentType.ONE_SHARD_THREE_REPLICAS);
        }
        return Stream.of(ClickHouseDeploymentType.STANDALONE);
    }

    public static void query(ClickHouseHelperClient chc, String query) {
        if (chc.isUseClientV2()) {
            try {
                chc.queryV2(query).close();
            } catch (Exception e) {
                LOGGER.info("Failed to query ", e);
                throw new RuntimeException(e);
            }
        } else {
            chc.queryV1(query);
        }
    }

    public static OperationMetrics dropTable(ClickHouseHelperClient chc, String tableName, ClickHouseDeploymentType cfg) {
        for (int i = 0; i < 5; i++) {
            try {
                OperationMetrics operationMetrics = dropTableLoop(chc, tableName, cfg);
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

    private static OperationMetrics dropTableLoop(ClickHouseHelperClient chc, String tableName, ClickHouseDeploymentType cfg) {
        String clusterClause = (cfg.isLocalCluster())
                ? " ON CLUSTER '" + cfg.clusterName + "' SYNC" : "";
        String dropTable = String.format("DROP TABLE IF EXISTS `%s`%s", tableName, clusterClause);
        try (Records records = chc.getClient().queryRecords(dropTable).get(QUERY_TIMEOUT, QUERY_TIMEOUT_UNIT)) {
            return records.getMetrics();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<JSONObject> getAllRowsAsJson(ClickHouseHelperClient chc, String tableName, ClickHouseDeploymentType cfg) {
        String from = buildFromClause(chc, tableName, cfg);
        String query = "SELECT * FROM " + from;

        QuerySettings querySettings = new QuerySettings();
        querySettings.setFormat(ClickHouseFormat.JSONEachRow);
        try (QueryResponse queryResponse = chc.getClient().query(query, querySettings).get()) {
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    public static OperationMetrics optimizeTable(ClickHouseHelperClient chc, String tableName, ClickHouseDeploymentType cfg) {
        String clusterClause = (cfg.isLocalCluster())
                ? " ON CLUSTER '" + cfg.clusterName + "'" : "";
        String queryCount = String.format("OPTIMIZE TABLE `%s`%s", tableName, clusterClause);

        try (Records records = chc.getClient().queryRecords(queryCount).get(QUERY_TIMEOUT, QUERY_TIMEOUT_UNIT)) {
            return records.getMetrics();
        } catch (Exception e) {
            return null;
        }
    }

    public static int countRows(ClickHouseHelperClient chc, String tableName, ClickHouseDeploymentType cfg) {
        optimizeTable(chc, tableName, cfg);
        String from = buildFromClause(chc, tableName, cfg);
        String queryCount = "SELECT COUNT(*) FROM " + from + " SETTINGS select_sequential_consistency = 1";

        try (Records records = chc.getClient().queryRecords(queryCount).get(QUERY_TIMEOUT, QUERY_TIMEOUT_UNIT)) {
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

    public static int sumRows(ClickHouseHelperClient chc, String tableName, String column, ClickHouseDeploymentType cfg) {
        String from = buildFromClause(chc, tableName, cfg);
        String queryCount = "SELECT SUM(`" + column + "`) FROM " + from;
        try (Records records = chc.getClient().queryRecords(queryCount).get(QUERY_TIMEOUT, QUERY_TIMEOUT_UNIT)) {
            String value = records.iterator().next().getString(1);
            return (int) (Float.parseFloat(value));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int countRowsWithEmojis(ClickHouseHelperClient chc, String tableName, ClickHouseDeploymentType cfg) {
        String from = buildFromClause(chc, tableName, cfg);
        String queryCount = "SELECT COUNT(*) FROM " + from + " WHERE str LIKE '%\uD83D\uDE00%' SETTINGS select_sequential_consistency = 1";
        try (Records records = chc.getClient().queryRecords(queryCount).get(QUERY_TIMEOUT, QUERY_TIMEOUT_UNIT)) {
            String value = records.iterator().next().getString(1);
            return (int) (Float.parseFloat(value));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String buildFromClause(ClickHouseHelperClient chc, String tableName, ClickHouseDeploymentType cfg) {
        if (cfg.isLocalCluster()) {
            String escapedDatabase = escapeSingleQuotes(chc.getDatabase());
            String escapedTableName = escapeSingleQuotes(tableName);
            return String.format("cluster('%s', '%s', '%s')",
                    cfg.clusterName, escapedDatabase, escapedTableName);
        }
        return "`" + tableName + "`";
    }

    public static boolean validateRows(ClickHouseHelperClient chc, String topic, Collection<SinkRecord> sinkRecords, ClickHouseDeploymentType cfg) {
        boolean match = false;
        try {
            QuerySettings querySettings = new QuerySettings();
            querySettings.setFormat(ClickHouseFormat.JSONStringsEachRow);
            String from = buildFromClause(chc, topic, cfg);
            QueryResponse queryResponse = chc.getClient().query(String.format("SELECT * FROM %s", from), querySettings).get(QUERY_TIMEOUT, QUERY_TIMEOUT_UNIT);
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

    public static int countInsertQueries(ClickHouseHelperClient chc, String topic, ClickHouseDeploymentType cfg) {
        try (Client client = chc.getClient()) {
            String from;
            if (cfg.isLocalCluster()) {
                from = String.format("clusterAllReplicas('%s', 'system', 'query_log', rand())", cfg.clusterName);
            } else {
                from = "system.query_log";
            }
            String sql = String.format("SELECT COUNT(*) " +
                    "FROM %s " +
                    "WHERE type = 'QueryFinish' " +
                    "AND query_kind = 'Insert' " +
                    "AND query ILIKE '%%%s%%'", from, topic);
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

    public static boolean checkSequentialRows(ClickHouseHelperClient chc, String tableName, int totalRecords, ClickHouseDeploymentType cfg) {
        String from = buildFromClause(chc, tableName, cfg);
        String queryCount = "SELECT DISTINCT `off16` FROM " + from + " ORDER BY `off16` ASC";
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
}
