package com.clickhouse.helpers;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ClickHouseAPI {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseAPI.class);

    private final ClickHouseHelperClient clickHouseHelperClient;
    private final Properties properties;

    public ClickHouseAPI(Properties properties) {
        this.properties = properties;
        clickHouseHelperClient = new ClickHouseHelperClient.ClickHouseClientBuilder(properties.getProperty("clickhouse.host"), Integer.parseInt(properties.getProperty("clickhouse.port")))
                .setUsername(String.valueOf(properties.getOrDefault("clickhouse.username", "default")))
                .setPassword(properties.getProperty("clickhouse.password"))
                .sslEnable(true)
                .build();
    }


    public ClickHouseResponse createTable(String tableName) {
        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "  `raw` String,"
                + "  `generationTimestamp` DateTime64(3),"
                + "  `insertTime` DateTime64(3) DEFAULT now(),"
                + ")"
                + " ORDER BY insertTime";
        LOGGER.info("Create table: " + sql);
        return clickHouseHelperClient.query(sql);
    }

    public ClickHouseResponse dropTable(String tableName) {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        LOGGER.info("Drop table: " + sql);
        return clickHouseHelperClient.query(sql);
    }

    public String[] count(String tableName) {
        String sql = "SELECT uniqExact(raw) as uniqueTotal, count(*) as total, total - uniqueTotal FROM " + tableName;
        LOGGER.info("Count table: " + sql);
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(clickHouseHelperClient.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format
                     .query(sql)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();
            String countAsString = response.firstRecord().getValue(0).asString();
            LOGGER.info("Counts: {}", countAsString);
            return countAsString.split("\t");
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }


}
