package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;

public class ClickHouseTestHelpers {
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
        String createTableQueryTmp = String.format(createTableQuery, tableName);

        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer())
                     .query(createTableQueryTmp)
                     .executeAndWait()) {
            return response.getSummary();
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
}
