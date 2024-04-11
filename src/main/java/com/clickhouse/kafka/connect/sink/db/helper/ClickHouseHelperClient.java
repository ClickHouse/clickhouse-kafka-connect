package com.clickhouse.kafka.connect.sink.db.helper;

import com.clickhouse.client.*;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClickHouseHelperClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseHelperClient.class);

    private final String hostname;
    private final int port;
    private final String username;
    private final String database;
    private final String password;
    private final boolean sslEnabled;
    private final String jdbcConnectionProperties;
    private final int timeout;
    private ClickHouseNode server = null;
    private final int retry;
    private ClickHouseProxyType proxyType = null;
    private String proxyHost = null;
    private int proxyPort = -1;

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
        this.server = create();
    }

    public String getDatabase() {
        return database;
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

    private ClickHouseNode create() {
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

        LOGGER.info("ClickHouse URL: " + url);

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

    public boolean ping() {
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

    public String version() {
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

    public ClickHouseNode getServer() {
        return this.server;
    }

    public ClickHouseResponse query(String query) {
        return query(query, null);
    }

    public ClickHouseResponse query(String query, ClickHouseFormat clickHouseFormat) {
        int retryCount = 0;
        ClickHouseException ce = null;
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


    public List<String> showTables() {
        List<String> tablesNames = new ArrayList<>();
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(server)
                     .query("SHOW TABLES")
                     .executeAndWait()) {
            for (ClickHouseRecord r : response.records()) {
                ClickHouseValue v = r.getValue(0);
                String tableName = v.asString();
                tablesNames.add(tableName);
            }

        } catch (ClickHouseException e) {
            LOGGER.error("Failed in show tables", e);
        }
        return tablesNames;
    }

    public Table describeTable(String tableName) {
        if (tableName.startsWith(".inner"))
            return null;
        String describeQuery = String.format("DESCRIBE TABLE `%s`.`%s`", this.database, tableName);
        LOGGER.debug(describeQuery);

        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(server)
                     .query(describeQuery)
                     .executeAndWait()) {
            Table table = new Table(tableName);
            for (ClickHouseRecord r : response.records()) {
                boolean hasDefault = false;
                ClickHouseValue v = r.getValue(0);
                String value = v.asString();
                String[] cols = value.split("\t");
                if (cols.length > 2) {
                    String defaultKind = cols[2];
                    if ("ALIAS".equals(defaultKind) || "MATERIALIZED".equals(defaultKind)) {
                        LOGGER.debug("Skipping column {} as it is an alias or materialized view", cols[0]);
                        // Only insert into "real" columns
                        continue;
                    } else if("DEFAULT".equals(defaultKind)) {
                        table.setHasDefaults(true);
                        hasDefault = true;
                    }
                }
                String name = cols[0];
                String type = cols[1];
                Column column = Column.extractColumn(name, type, false, hasDefault);
                table.addColumn(column);
            }
            return table;
        } catch (ClickHouseException e) {
            LOGGER.error(String.format("Exception when running describeTable %s", describeQuery), e);
            return null;
        }
    }
    
    public List<Table> extractTablesMapping() {
        HashMap<String, Table> cache = new HashMap<String, Table>();
        return extractTablesMapping(cache);
    }

    public List<Table> extractTablesMapping(Map<String, Table> cache) {
        List<Table> tableList =  new ArrayList<>();
        for (String tableName : showTables() ) {
            // Table names are escaped in the cache
            String escapedTableName = Utils.escapeTopicName(tableName);

            // Read from cache if we already described this table before
            // This means we won't pick up edited table configs until the connector is restarted
            if (cache.containsKey(escapedTableName)) {
                tableList.add(cache.get(escapedTableName));
                continue;
            }

            Table table = describeTable(tableName);
            if (table != null )
                tableList.add(table);
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

        public ClickHouseHelperClient build(){
            return new ClickHouseHelperClient(this);
        }

    }
}
