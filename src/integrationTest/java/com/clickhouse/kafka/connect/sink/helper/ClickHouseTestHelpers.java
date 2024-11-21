package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickHouseTestHelpers {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseTestHelpers.class);
    public static final String CLICKHOUSE_DOCKER_IMAGE = "clickhouse/clickhouse-server:23.8";
    public static void dropTable(ClickHouseHelperClient chc, String tableName) {
        String dropTable = String.format("DROP TABLE IF EXISTS `%s`", tableName);
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer())
                     .query(dropTable)
                     .executeAndWait()) {
            return;
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createTable(ClickHouseHelperClient chc, String tableName, String createTableQuery) {
        String createTableQueryTmp = String.format(createTableQuery, tableName);

        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer())
                     .query(createTableQueryTmp)
                     .executeAndWait()) {
            return;
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public static int countRows(ClickHouseHelperClient chc, String tableName) {
        String queryCount = String.format("SELECT COUNT(*) FROM `%s`", tableName);
        return runQuery(chc, queryCount);
    }

    public static int sumRows(ClickHouseHelperClient chc, String tableName, String column) {
        String queryCount = String.format("SELECT SUM(`%s`) FROM `%s`", column, tableName);
        return runQuery(chc, queryCount);
    }

    public static int countRowsWithEmojis(ClickHouseHelperClient chc, String tableName) {
        String queryCount = "SELECT COUNT(*) FROM `" + tableName + "` WHERE str LIKE '%\uD83D\uDE00%'";
        return runQuery(chc, queryCount);
    }

    private static int runQuery(ClickHouseHelperClient chc, String query) {
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer())
                     .query(query)
                     .executeAndWait()) {
            return response.firstRecord().getValue(0).asInteger();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean checkSequentialRows(ClickHouseHelperClient chc, String tableName, int totalRecords) {
        String queryCount = String.format("SELECT DISTINCT `indexCount` FROM `%s` ORDER BY `indexCount` ASC", tableName);
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

    public static void waitWhileCounting(ClickHouseHelperClient chc, String tableName, int sleepInSeconds) {
        int databaseCount = countRows(chc, tableName);
        int lastCount = 0;
        int loopCount = 0;

        while(databaseCount != lastCount || loopCount < 6) {
            try {
                Thread.sleep(sleepInSeconds * 1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            databaseCount = countRows(chc, tableName);
            if (lastCount == databaseCount) {
                loopCount++;
            } else {
                loopCount = 0;
            }

            lastCount = databaseCount;
        }
    }
}
