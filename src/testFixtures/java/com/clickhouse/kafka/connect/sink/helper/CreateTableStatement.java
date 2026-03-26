package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class CreateTableStatement {
    private final ClickHouseHelperClient chc;
    private String tableName;
    private LinkedHashMap<String, String> schema;
    private String engine;
    private String orderByColumn;
    private Map<String, Serializable> settings;
    private boolean ifNotExists = false;

    public CreateTableStatement(ClickHouseHelperClient chc) {
        this.chc = chc;
    }

    public CreateTableStatement setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public CreateTableStatement setSchema(LinkedHashMap<String, String> schema) {
        this.schema = schema;
        return this;
    }

    public CreateTableStatement setEngine(String engine) {
        this.engine = engine;
        return this;
    }

    public CreateTableStatement setOrderByColumn(String orderByColumn) {
        this.orderByColumn = orderByColumn;
        return this;
    }

    public CreateTableStatement setSettings(Map<String, Serializable> settings) {
        this.settings = settings;
        return this;
    }

    public CreateTableStatement setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
        return this;
    }

    public void execute() {
        StringBuilder columns = new StringBuilder();
        for (Map.Entry<String, String> entry : schema.entrySet()) {
            if (columns.length() > 0) columns.append(", ");
            columns.append("`").append(entry.getKey()).append("` ").append(entry.getValue());
        }
        String sql = String.format("CREATE TABLE %s`%s` (%s) Engine = %s ORDER BY %s",
                ifNotExists ? "IF NOT EXISTS " : "", tableName, columns, engine, orderByColumn);
        try {
            if (settings != null && !settings.isEmpty()) {
                QuerySettings querySettings = new QuerySettings();
                settings.forEach(querySettings::setOption);
                try (Records ignored = chc.queryV2(sql, querySettings)) { /* success */ }
            } else {
                try (Records ignored = chc.queryV2(sql)) { /* success */ }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
