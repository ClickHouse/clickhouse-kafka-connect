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
    private Optional<String> databaseOpt = Optional.empty();
    private LinkedHashMap<String, String> schema = new LinkedHashMap<>();
    private String engine;
    private Optional<String> orderByColumnOpt = Optional.empty();
    private Map<String, Serializable> settings;
    private boolean ifNotExists = false;
    private Optional<String> clusterClauseOpt = Optional.empty();

    public CreateTableStatement() {
        if (ClickHouseTestHelpers.isCluster()) {
            this.clusterClauseOpt = Optional.of("ON CLUSTER '" + ClickHouseCluster.getClusterFromEnvVarOrThrow().getName() + "'");
        }
    }

    /**
     * @deprecated Copying a shared mutable template is easy to get wrong. Declare the template as a
     * {@code Supplier<CreateTableStatement>} and call {@code get()} instead, see {@link #create()}.
     */
    @Deprecated
    public CreateTableStatement(CreateTableStatement template) {
        this.tableName = template.tableName;
        this.databaseOpt = template.databaseOpt;
        this.schema = new LinkedHashMap<>(template.schema);
        this.engine = template.engine;
        this.orderByColumnOpt = template.orderByColumnOpt;
        this.settings = template.settings;
        this.ifNotExists = template.ifNotExists;
        this.clusterClauseOpt = template.clusterClauseOpt;
    }

    /** Fluent entry point for new tests;
     * the setters already chain, so a separate Builder class would only duplicate them.
     *
     * <pre>{@code
     * CreateTableStatement.create()
     *         .tableName("table_test")
     *         .column("id", "Int16")
     *         .engine(ClickHouseTableEngine.MERGE_TREE)
     *         .orderByColumn("id")
     *         .execute(chc);
     * }</pre>
     * <p>For reusable templates, declare a {@code Supplier} so every {@code get()} builds a fresh statement:
     * <pre>{@code
     * private static final Supplier<CreateTableStatement> STOCK_TABLE = () -> CreateTableStatement.create()
     *         .column("symbol", "String")
     *         .engine(ClickHouseTableEngine.MERGE_TREE)
     *         .orderByColumn("symbol");
     *
     * STOCK_TABLE.get().tableName(topic).execute(chc);
     * }</pre>
     * */
    public static CreateTableStatement create() {
        return new CreateTableStatement();
    }

    public CreateTableStatement tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public CreateTableStatement database(String database) {
        this.databaseOpt = Optional.ofNullable(database);
        return this;
    }

    public CreateTableStatement column(String name, String type) {
        this.schema.put(name, type);
        return this;
    }

    public CreateTableStatement engine(String engine) {
        if (ClickHouseTestHelpers.isCluster()
                && ClickHouseTableEngine.MERGE_TREE.getSqlName().equals(engine)) {
            engine = ClickHouseCluster.getClusterFromEnvVarOrThrow().getMergeTreeEngine();
        }
        this.engine = engine;
        return this;
    }

    public CreateTableStatement engine(ClickHouseTableEngine engine) {
        return engine(engine.getSqlName());
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
                .append(ClickHouseTestHelpers.quoteTableName(databaseOpt.orElse(null), tableName));
        clusterClauseOpt.ifPresent(s -> sql.append(" ").append(s));
        sql.append(" ").append("(").append(columns).append(")").append(" ");
        sql.append("Engine = ").append(engine);
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
