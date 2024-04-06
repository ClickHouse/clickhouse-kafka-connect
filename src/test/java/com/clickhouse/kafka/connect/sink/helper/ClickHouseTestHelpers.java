package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.client.*;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseFieldDescriptor;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Type;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ClickHouseTestHelpers {

    public static final String CLICKHOUSE_VERSION_DEFAULT = "24.3";
    public static final String CLICKHOUSE_PROXY_VERSION_DEFAULT = "23.8";
    public static final String CLICKHOUSE_DOCKER_IMAGE = String.format("clickhouse/clickhouse-server:%s", getClickhouseVersion());
    public static final String CLICKHOUSE_FOR_PROXY_DOCKER_IMAGE = String.format("clickhouse/clickhouse-server:%s", CLICKHOUSE_PROXY_VERSION_DEFAULT);

    public static final String HTTPS_PORT = "8443";
    public static final String DATABASE_DEFAULT = "default";
    public static final String USERNAME_DEFAULT = "default";
    public static final String getClickhouseVersion() {
        String clickHouseVersion = System.getenv("CLICKHOUSE_VERSION");
        if (clickHouseVersion == null) {
            clickHouseVersion = CLICKHOUSE_VERSION_DEFAULT;
        }
        return clickHouseVersion;
    }
    public static boolean isCloud() {
        String version = System.getenv("CLICKHOUSE_VERSION");
        System.out.println("version: " + version);
        if ( version != null && version.equalsIgnoreCase("cloud")) {
            return true;
        }
        return false;
    }
    public static ClickHouseResponseSummary dropTable(ClickHouseHelperClient chc, String tableName) {
        String dropTable = String.format("DROP TABLE IF EXISTS `%s`", tableName);
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer())
                     .query(dropTable)
                     .executeAndWait()) {
            return response.getSummary();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public static ClickHouseResponseSummary createTable(ClickHouseHelperClient chc, String tableName, String createTableQuery) {
        return createTable(chc, tableName, createTableQuery, new HashMap<>());
    }

    public static ClickHouseResponseSummary createTable(ClickHouseHelperClient chc, String tableName, String createTableQuery, Map<String, Serializable> clientSettings) {
        String createTableQueryTmp = String.format(createTableQuery, tableName);

        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer())
                     .settings(clientSettings)
                     .query(createTableQueryTmp)
                     .executeAndWait()) {
            return response.getSummary();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<JSONObject> getAllRowsAsJson(ClickHouseHelperClient chc, String tableName) {
        String query = String.format("SELECT * FROM `%s`", tableName);
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer())
                     .query(query)
                     .format(ClickHouseFormat.JSONEachRow)
                     .executeAndWait()) {

            return StreamSupport.stream(response.records().spliterator(), false)
                    .map(record -> record.getValue(0).asString())
                    .map(JSONObject::new)
                    .collect(Collectors.toList());
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public static int countRows(ClickHouseHelperClient chc, String tableName) {
        String queryCount = String.format("SELECT COUNT(*) FROM `%s`", tableName);
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer())
                     .query(queryCount)
                     .executeAndWait()) {
            return response.firstRecord().getValue(0).asInteger();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public static int sumRows(ClickHouseHelperClient chc, String tableName, String column) {
        String queryCount = String.format("SELECT SUM(`%s`) FROM `%s`", column, tableName);
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer())
                     .query(queryCount)
                     .executeAndWait()) {
            return response.firstRecord().getValue(0).asInteger();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public static int countRowsWithEmojis(ClickHouseHelperClient chc, String tableName) {
        String queryCount = "SELECT COUNT(*) FROM `" + tableName + "` WHERE str LIKE '%\uD83D\uDE00%'";

        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer()) // or client.connect(endpoints)
                     .query(queryCount)
                     .executeAndWait()) {
            return response.firstRecord().getValue(0).asInteger();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
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
