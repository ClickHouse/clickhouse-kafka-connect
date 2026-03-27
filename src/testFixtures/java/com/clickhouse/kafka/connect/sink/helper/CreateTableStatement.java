package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class CreateTableStatement {
    private String tableName;
    private LinkedHashMap<String, String> schema = new LinkedHashMap<>();
    private String engine;
    private String orderByColumn;
    private Map<String, Serializable> settings;
    private boolean ifNotExists = false;

    public CreateTableStatement() {}

    public CreateTableStatement(CreateTableStatement template) {
        this.tableName = template.tableName;
        this.schema = new LinkedHashMap<>(template.schema);
        this.engine = template.engine;
        this.orderByColumn = template.orderByColumn;
        this.settings = template.settings;
        this.ifNotExists = template.ifNotExists;
    }

    public CreateTableStatement setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public CreateTableStatement setColumn(String name, String type) {
        this.schema.put(name, type);
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

    public void execute(ClickHouseHelperClient chc) {
        var columns = new StringBuilder();
        for (Map.Entry<String, String> entry : schema.entrySet()) {
            if (columns.length() > 0)
                columns.append(", ");
            columns.append("`").append(entry.getKey()).append("` ").append(entry.getValue());
        }
        var sql = new StringBuilder();
        sql.append("CREATE TABLE ")
                .append(ifNotExists ? "IF NOT EXISTS " : "")
                .append("`").append(tableName).append("`").append(" ")
                .append("(").append(columns).append(")").append(" ")
                .append("Engine = ").append(engine);
        if (orderByColumn != null) {
            sql.append(" ORDER BY ").append(orderByColumn);
        }

        try {
            if (settings != null && !settings.isEmpty()) {
                QuerySettings querySettings = new QuerySettings();
                settings.forEach(querySettings::setOption);
                try (Records ignored = chc.queryV2(sql.toString(), querySettings)) { /* success */ }
            } else {
                try (Records ignored = chc.queryV2(sql.toString())) { /* success */ }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
