package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@SuppressWarnings({"OptionalUsedAsFieldOrParameterType"})
public class CreateTableStatement {
    private String tableName;
    private LinkedHashMap<String, String> schema = new LinkedHashMap<>();
    private Optional<String> engineOpt = Optional.empty(); // defaults to MergeTree/ReplicatedMergeTree if unset
    private Optional<String> orderByColumnOpt =  Optional.empty();
    private Map<String, Serializable> settings;
    private boolean ifNotExists = false;

    public CreateTableStatement() {}

    public CreateTableStatement(CreateTableStatement template) {
        this.tableName = template.tableName;
        this.schema = new LinkedHashMap<>(template.schema);
        this.engineOpt = template.engineOpt;
        this.orderByColumnOpt = template.orderByColumnOpt;
        this.settings = template.settings;
        this.ifNotExists = template.ifNotExists;
    }

    public CreateTableStatement tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public CreateTableStatement column(String name, String type) {
        this.schema.put(name, type);
        return this;
    }

    public CreateTableStatement engine(String engine) {
        this.engineOpt = Optional.of(engine);
        return this;
    }

    public CreateTableStatement orderByColumn(String orderByColumn) {
        this.orderByColumnOpt = Optional.of(orderByColumn);
        return this;
    }

    public CreateTableStatement settings(Map<String, Serializable> settings) {
        this.settings = settings;
        return this;
    }

    public CreateTableStatement ifNotExists(boolean ifNotExists) {
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
                .append("`").append(tableName).append("`");
        if (ClickHouseTestHelpers.isCluster()) {
            var cluster = ClickHouseCluster.getClusterFromEnvVarOrThrow();
            sql.append(" ON CLUSTER '").append(cluster.getName()).append("'");
        }
        sql.append(" ")
                .append("(").append(columns).append(")").append(" ");

        if (engineOpt.isPresent()) {
            sql.append("Engine = ").append(engineOpt.get());
        } else {
            var engine = ClickHouseTestHelpers.isCluster() ? ClickHouseCluster.getClusterFromEnvVarOrThrow().getMergeTreeEngine() : "MergeTree";
            sql.append("Engine = ").append(engine);
        }

        orderByColumnOpt.ifPresent(s -> sql.append(" ORDER BY ").append(s));

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
