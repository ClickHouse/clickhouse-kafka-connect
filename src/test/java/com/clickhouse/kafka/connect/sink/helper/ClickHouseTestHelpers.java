package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseFieldDescriptor;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Type;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ClickHouseTestHelpers {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseTestHelpers.class);
    public static final String CLICKHOUSE_VERSION_DEFAULT = "24.3";
    public static final String CLICKHOUSE_PROXY_VERSION_DEFAULT = "23.8";
    public static final String CLICKHOUSE_DOCKER_IMAGE = String.format("clickhouse/clickhouse-server:%s", getClickhouseVersion());
    public static final String CLICKHOUSE_FOR_PROXY_DOCKER_IMAGE = String.format("clickhouse/clickhouse-server:%s", CLICKHOUSE_PROXY_VERSION_DEFAULT);

    public static final String HTTPS_PORT = "8443";
    public static final String DATABASE_DEFAULT = "default";
    public static final String USERNAME_DEFAULT = "default";

    private static final int CLOUD_TIMEOUT_VALUE = 900;
    private static final TimeUnit CLOUD_TIMEOUT_UNIT = TimeUnit.SECONDS;

    public static String getClickhouseVersion() {
        String clickHouseVersion = System.getenv("CLICKHOUSE_VERSION");
        if (clickHouseVersion == null) {
            clickHouseVersion = CLICKHOUSE_VERSION_DEFAULT;
        }
        return clickHouseVersion;
    }
    public static boolean isCloud() {
        String version = System.getenv("CLICKHOUSE_VERSION");
        LOGGER.info("Version: {}", version);
        return version != null && version.equalsIgnoreCase("cloud");
    }

    public static void query(ClickHouseHelperClient chc, String query) {
        if (chc.isUseClientV2()) {
            chc.queryV2(query);
        } else {
            chc.queryV1(query);
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



    public static OperationMetrics createTable(ClickHouseHelperClient chc, String tableName, String createTableQuery) {
        LOGGER.info("Creating table: {}, Query: {}", tableName, createTableQuery);
        OperationMetrics operationMetrics = createTable(chc, tableName, createTableQuery, new HashMap<>());
        if (isCloud()) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                LOGGER.error("Error while sleeping", e);
            }
        }
        return operationMetrics;
    }

    public static OperationMetrics createTable(ClickHouseHelperClient chc, String tableName, String createTableQuery, Map<String, Serializable> clientSettings) {
        for (int i = 0; i < 5; i++) {
            try {
                OperationMetrics operationMetrics = createTableLoop(chc, tableName, createTableQuery, clientSettings);
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
    private static OperationMetrics createTableLoop(ClickHouseHelperClient chc, String tableName, String createTableQuery, Map<String, Serializable> clientSettings) {
        final String createTableQueryTmp = String.format(createTableQuery, tableName);
        QuerySettings settings = new QuerySettings();
        for (Map.Entry<String, Serializable> entry : clientSettings.entrySet()) {
            settings.setOption(entry.getKey(), entry.getValue());
        }
        try {
            return chc.getClient().queryRecords(createTableQueryTmp, settings).get(CLOUD_TIMEOUT_VALUE, CLOUD_TIMEOUT_UNIT).getMetrics();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<JSONObject> getAllRowsAsJson(ClickHouseHelperClient chc, String tableName)  {
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


    public static OperationMetrics optimizeTable(ClickHouseHelperClient chc, String tableName) {
        String queryCount = String.format("OPTIMIZE TABLE `%s`", tableName);

        try {
            Records records = chc.getClient().queryRecords(queryCount).get(CLOUD_TIMEOUT_VALUE, CLOUD_TIMEOUT_UNIT);
            return records.getMetrics();
        } catch (Exception e) {
            return null;
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
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int sumRows(ClickHouseHelperClient chc, String tableName, String column) {
        String queryCount = String.format("SELECT SUM(`%s`) FROM `%s`", column, tableName);
        try {
            Records records = chc.getClient().queryRecords(queryCount).get(CLOUD_TIMEOUT_VALUE, CLOUD_TIMEOUT_UNIT);
            String value = records.iterator().next().getString(1);
            return (int)(Float.parseFloat(value));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int countRowsWithEmojis(ClickHouseHelperClient chc, String tableName) {
        String queryCount = "SELECT COUNT(*) FROM `" + tableName + "` WHERE str LIKE '%\uD83D\uDE00%' SETTINGS select_sequential_consistency = 1";
        try {
            Records records = chc.getClient().queryRecords(queryCount).get(CLOUD_TIMEOUT_VALUE, CLOUD_TIMEOUT_UNIT);
            String value = records.iterator().next().getString(1);
            return (int)(Float.parseFloat(value));
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
                records.add(gsonString.replace(".0", "").replace(" ","").replace("'","").replace("\\u003d",":"));
            }
            List<String> results = new ArrayList<>();
            LOGGER.info("read rows [%d]", queryResponse.getReadRows());
            BufferedReader reader = new BufferedReader(new InputStreamReader(queryResponse.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                String gsonString = line.replace("'","").replace(" ","").replace("\\u003d",":");
                Map<String, String> resultMap = new TreeMap<>((Map<String, String>) gson.fromJson(gsonString, new TypeToken<Map<String, String>>() {}.getType()));
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
}
