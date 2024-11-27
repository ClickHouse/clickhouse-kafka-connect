package com.clickhouse.kafka.connect.sink.db.helper;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.ProxyType;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.util.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ClickHouseHelperClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseHelperClient.class);

    private final String hostname;
    private final int port;
    private final String username;
    @Getter
    private final String database;
    private final String password;
    private final boolean sslEnabled;
    private final String jdbcConnectionProperties;
    private final int timeout;
    @Getter
    private ClickHouseNode server = null;
    @Getter
    private Client client = null;
    private final int retry;
    private ClickHouseProxyType proxyType = null;
    private String proxyHost = null;
    private int proxyPort = -1;
    @Getter
    private boolean useClientV2 = false;

    public ClickHouseHelperClient(ClickHouseClientBuilder builder) {
        this.hostname = builder.hostname;
        this.port = builder.port;
        this.username = builder.username;
        this.password = builder.password;
        this.database = builder.database;
        this.sslEnabled = builder.sslEnabled;
        this.jdbcConnectionProperties = builder.jdbcConnectionProperties;
        this.timeout = builder.timeout;
        this.retry = builder.retry;
        this.proxyType = builder.proxyType;
        this.proxyHost = builder.proxyHost;
        this.proxyPort = builder.proxyPort;
        this.useClientV2 = builder.useClientV2;
        // We are creating two clients, one for V1 and one for V2
        this.client = createClientV2();
        this.server = createClientV1();
    }

    public Map<ClickHouseOption, Serializable> getDefaultClientOptions() {
        Map<ClickHouseOption, Serializable> options = new HashMap<>();
        options.put(ClickHouseClientOption.PRODUCT_NAME, "clickhouse-kafka-connect/"+ClickHouseClientOption.class.getPackage().getImplementationVersion());
        if (proxyType != null && !proxyType.equals(ClickHouseProxyType.IGNORE)) {
            options.put(ClickHouseClientOption.PROXY_TYPE, proxyType);
            options.put(ClickHouseClientOption.PROXY_HOST, proxyHost);
            options.put(ClickHouseClientOption.PROXY_PORT, proxyPort);
        }
        return options;
    }

    private ClickHouseNode createClientV1() {
        String protocol = "http";
        if (this.sslEnabled)
            protocol += "s";

        String tmpJdbcConnectionProperties = jdbcConnectionProperties;
        if (tmpJdbcConnectionProperties != null && !tmpJdbcConnectionProperties.startsWith("?")) {
            tmpJdbcConnectionProperties = "?" + tmpJdbcConnectionProperties;
        }

        String url = String.format("%s://%s:%d/%s%s", 
                protocol, 
                hostname, 
                port, 
                database,
                tmpJdbcConnectionProperties
        );

        LOGGER.info("ClickHouse URL: {}", url);

        if (username != null && password != null) {
            LOGGER.debug(String.format("Adding username [%s]", username));
            Map<String, String> options = new HashMap<>();
            options.put("user", username);
            options.put("password", password);
            server = ClickHouseNode.of(url, options);
        } else {
            server = ClickHouseNode.of(url);
        }
        return server;
    }

    private Client createClientV2() {
        String protocol = "http";
        if (this.sslEnabled)
            protocol += "s";

        String tmpJdbcConnectionProperties = jdbcConnectionProperties;
        if (tmpJdbcConnectionProperties != null && !tmpJdbcConnectionProperties.startsWith("?")) {
            tmpJdbcConnectionProperties = "?" + tmpJdbcConnectionProperties;
        }

        String url = String.format("%s://%s:%d/%s%s",
                protocol,
                hostname,
                port,
                database,
                tmpJdbcConnectionProperties
        );

        LOGGER.info("ClickHouse URL: {}", url);



        Client.Builder clientBuilder = new Client.Builder()
                .addEndpoint(url)
                .setUsername(this.username)
                .setPassword(this.password)
                .setDefaultDatabase(this.database);

        if (proxyType != null && !proxyType.equals(ClickHouseProxyType.IGNORE)) {
            clientBuilder.addProxy(ProxyType.HTTP, proxyHost, proxyPort);
        }
        client = clientBuilder.build();
        return client;
    }

    public boolean ping() {
        if (useClientV2) {
            return pingV2();
        } else {
            return pingV1();
        }
    }
    private boolean pingV1() {
        ClickHouseClient clientPing = ClickHouseClient.builder()
                .options(getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
        LOGGER.debug(String.format("Server [%s] , Timeout [%d]", server, timeout));
        int retryCount = 0;

        while (retryCount < retry) {
            if (clientPing.ping(server, timeout)) {
                LOGGER.info("Ping was successful.");
                clientPing.close();
                return true;
            }
            retryCount++;
            LOGGER.warn(String.format("Ping retry %d out of %d", retryCount, retry));
        }
        LOGGER.error("Unable to ping ClickHouse instance.");
        clientPing.close();
        return false;
    }
    private boolean pingV2() {
        int retryCount = 0;
        while (retryCount < retry) {
            if (client.ping(timeout)) {
                LOGGER.info("Ping was successful.");
                return true;
            }
            retryCount++;
            LOGGER.warn(String.format("Ping retry %d out of %d", retryCount, retry));
        }
        LOGGER.error("Unable to ping ClickHouse instance.");
        return false;
    }

    public String version() {
        if (useClientV2) {
            return versionV2();
        } else {
            return versionV1();
        }
    }
    public String versionV1() {
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(server)
                     .query("SELECT VERSION()")
                     .executeAndWait()) {
            return response.firstRecord().getValue(0).asString();
        } catch (ClickHouseException e) {
            LOGGER.error("Exception when trying to retrieve VERSION()", e);
            return null;
        }
    }

    public String versionV2() {
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        CompletableFuture<Records> futureRecords = client.queryRecords("SELECT VERSION()", settings);
        try {
            Records records = futureRecords.get();
            for (GenericRecord record : records) {
                return record.getString(1); // string column col3
            }
        } catch (InterruptedException e) {
            LOGGER.error("Exception when trying to retrieve VERSION()", e);
            return null;
        } catch (ExecutionException e) {
            LOGGER.error("Exception when trying to retrieve VERSION()", e);
            return null;
        }
        return null;
    }

    public ClickHouseResponse queryV1(String query) {
        return queryV1(query, null);
    }

    public Records queryV2(String query) {
        return queryV2(query, null);
    }

    public ClickHouseResponse queryV1(String query, ClickHouseFormat clickHouseFormat) {
        int retryCount = 0;
        Exception ce = null;
        while (retryCount < retry) {
            try (ClickHouseClient client = ClickHouseClient.builder()
                    .options(getDefaultClientOptions())
                    .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                    .build();
                 ClickHouseResponse response = client.read(server)
                         .format(clickHouseFormat)
                         .query(query)
                         .executeAndWait()) {
                return response;
            } catch (ClickHouseException e) {
                retryCount++;
                LOGGER.warn(String.format("Query retry %d out of %d", retryCount, retry), e);
                ce = e;
            }
        }
        throw new RuntimeException(ce);
    }
    public Records queryV2(String query, ClickHouseFormat clickHouseFormat) {
        int retryCount = 0;
        Exception ce = null;
        QuerySettings settings = new QuerySettings();
        if (clickHouseFormat != null)
            settings.setFormat(clickHouseFormat);
        while (retryCount < retry) {
            System.out.println("query " + query + " retry " + retryCount + " out of " + retry);
            CompletableFuture<Records> futureRecords = client.queryRecords(query, settings);
            try {
                Records records = futureRecords.get();
                return records;
            } catch (ExecutionException e) {
                retryCount++;
                LOGGER.warn(String.format("Query retry %d out of %d", retryCount, retry), e);
                ce = e;
            } catch (InterruptedException e) {
                retryCount++;
                LOGGER.warn(String.format("Query retry %d out of %d", retryCount, retry), e);
                ce = e;
            }
        }
        throw new RuntimeException(ce);
    }

    public List<Table> showTables(String database) {
        if (useClientV2) {
            return showTablesV2(database);
        } else {
            return showTablesV1(database);
        }
    }
    public List<Table> showTablesV1(String database) {
        List<Table> tables = new ArrayList<>();
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(server)
                     .query(String.format("select database, table, count(*) as col_count from system.columns where database = '%s' group by database, table", database))
                     .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                     .executeAndWait()) {
            for (ClickHouseRecord r : response.records()) {
                String databaseName = r.getValue(0).asString();
                String tableName = r.getValue(1).asString();
                int colCount =  r.getValue(2).asInteger();
                tables.add(new Table(databaseName, tableName, colCount));
            }
        } catch (ClickHouseException e) {
            LOGGER.error("Failed in show tables", e);
        }
        return tables;
    }

    public List<Table> showTablesV2(String database) {
        List<Table> tablesList = new ArrayList<>();
        Records records = queryV2(String.format("select database, table, count(*) as col_count from system.columns where database = '%s' group by database, table", database));
        for (GenericRecord record : records) {
            String databaseName = record.getString(1);
            String tableName = record.getString(2);
            int colCount =  record.getInteger(3);
            LOGGER.debug("table name: {}", tableName);
            tablesList.add(new Table(databaseName, tableName, colCount));
        }
        return tablesList;
    }

    public Table describeTable(String database, String tableName) {
        if (useClientV2) {
            return describeTableV2(database, tableName);
        } else {
            return describeTableV1(database, tableName);
        }
    }
    public Table describeTableV1(String database, String tableName) {
        if (tableName.startsWith(".inner"))
            return null;
        String describeQuery = String.format("DESCRIBE TABLE `%s`.`%s`", database, tableName);
        LOGGER.debug(describeQuery);

        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(server)
                     .set("describe_include_subcolumns", true)
                     .format(ClickHouseFormat.JSONEachRow)
                     .query(describeQuery)
                     .executeAndWait()) {

            Table table = new Table(database, tableName);
            for (ClickHouseRecord r : response.records()) {
                ClickHouseValue v = r.getValue(0);

                ClickHouseFieldDescriptor fieldDescriptor = ClickHouseFieldDescriptor.fromJsonRow(v.asString());
                if (fieldDescriptor.isAlias() || fieldDescriptor.isMaterialized() || fieldDescriptor.isEphemeral()) {
                    LOGGER.debug("Skipping column {} as it is an alias or materialized view or ephemeral", fieldDescriptor.getName());
                    continue;
                }

                if (fieldDescriptor.hasDefault()) {
                    table.hasDefaults(true);
                }

                Column column = Column.extractColumn(fieldDescriptor);
                //If we run into a rare column we can't handle, just ignore the table and warn the user
                if (column == null) {
                    LOGGER.warn("Unable to handle column: {}", fieldDescriptor.getName());
                    return null;
                }
                table.addColumn(column);
            }
            return table;
        } catch (ClickHouseException | JsonProcessingException e) {
            LOGGER.error(String.format("Exception when running describeTable %s", describeQuery), e);
            return null;
        }
    }

    public Table describeTableV2(String database, String tableName)  {
        if (tableName.startsWith(".inner"))
            return null;
        String describeQuery = String.format("DESCRIBE TABLE `%s`.`%s`", this.database, tableName);
        LOGGER.debug(describeQuery);

        Table table = new Table(database, tableName);
        try {
            QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.JSONEachRow);
            settings.serverSetting("describe_include_subcolumns", "1");
            settings.setDatabase(database);
            QueryResponse queryResponse = client.query(describeQuery, settings).get();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(queryResponse.getInputStream()))) {
                String line = null;
                while ((line = br.readLine()) != null) {
                    ClickHouseFieldDescriptor fieldDescriptor = ClickHouseFieldDescriptor.fromJsonRow(line);
                    if (fieldDescriptor.isAlias() || fieldDescriptor.isMaterialized() || fieldDescriptor.isEphemeral()) {
                        LOGGER.debug("Skipping column {} as it is an alias or materialized view or ephemeral", fieldDescriptor.getName());
                        continue;
                    }

                    if (fieldDescriptor.hasDefault()) {
                        table.hasDefaults(true);
                    }

                    Column column = Column.extractColumn(fieldDescriptor);
                    //If we run into a rare column we can't handle, just ignore the table and warn the user
                    if (column == null) {
                        LOGGER.warn("Unable to handle column: {}", fieldDescriptor.getName());
                        return null;
                    }
                    table.addColumn(column);                }
            }
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
        return table;
    }
    public List<Table> extractTablesMapping(String database, Map<String, Table> cache) {
        List<Table> tableList =  new ArrayList<>();
        for (Table table : showTables(database) ) {
            // (Full) Table names are escaped in the cache
            String escapedTableName = Utils.escapeTableName(database, table.getCleanName());

            // Read from cache if we already described this table before
            // This means we won't pick up edited table configs until the connector is restarted
            if (cache.containsKey(escapedTableName)) {
                // 2 -> 3
                if (cache.get(escapedTableName).getNumColumns() < table.getNumColumns()) {
                    LOGGER.info("Table {} has been updated, re-describing", table.getCleanName());
                } else {
                    // No need to re-describe since no columns have been added
                    continue;
                }
            }
            Table tableDescribed = describeTable(this.database, table.getCleanName());
            if (tableDescribed != null) {
                tableDescribed.setNumColumns(table.getNumColumns());
                tableList.add(tableDescribed);
            }
        }
        return tableList;
    }

    public static class ClickHouseClientBuilder {
        private ClickHouseSinkConfig config = null;
        private String hostname = null;
        private int port = -1;
        private String username = "default";
        private String database = "default";
        private String password = "";
        private boolean sslEnabled = false;
        private String jdbcConnectionProperties = "";
        private int timeout = ClickHouseSinkConfig.timeoutSecondsDefault * ClickHouseSinkConfig.MILLI_IN_A_SEC;
        private int retry = ClickHouseSinkConfig.retryCountDefault;

        private ClickHouseProxyType proxyType = null;
        private String proxyHost = null;
        private int proxyPort = -1;
        private boolean useClientV2 = true;

        public ClickHouseClientBuilder(String hostname, int port, ClickHouseProxyType proxyType, String proxyHost, int proxyPort) {
            this.hostname = hostname;
            this.port = port;
            this.proxyType = proxyType;
            this.proxyHost = proxyHost;
            this.proxyPort = proxyPort;
        }


        public ClickHouseClientBuilder setUsername(String username) {
            this.username = username;
            return this;
        }

        public ClickHouseClientBuilder setPassword(String password) {
            this.password = password;
            return this;
        }

        public ClickHouseClientBuilder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public ClickHouseClientBuilder sslEnable(boolean sslEnabled) {
            this.sslEnabled = sslEnabled;
            return this;
        }

        public ClickHouseClientBuilder setJdbcConnectionProperties(String jdbcConnectionProperties) {
            this.jdbcConnectionProperties = jdbcConnectionProperties;
            return this;
        }

        public ClickHouseClientBuilder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public ClickHouseClientBuilder setRetry(int retry) {
            this.retry = retry;
            return this;
        }
        public ClickHouseClientBuilder useClientV2(boolean useClientV2) {
            this.useClientV2 = useClientV2;
            return this;
        }
        public ClickHouseHelperClient build(){
            return new ClickHouseHelperClient(this);
        }

    }
}
