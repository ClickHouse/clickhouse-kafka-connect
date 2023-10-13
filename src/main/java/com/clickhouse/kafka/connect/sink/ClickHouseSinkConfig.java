package com.clickhouse.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ClickHouseSinkConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkConfig.class);

    public static final String VERSION = "1.0.2";

    //Configuration Names
    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";
    public static final String DATABASE = "database";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String SSL_ENABLED = "ssl";
    public static final String TIMEOUT_SECONDS = "timeoutSeconds";
    public static final String RETRY_COUNT = "retryCount";
    public static final String EXACTLY_ONCE = "exactlyOnce";
    public static final String SUPPRESS_TABLE_EXISTENCE_EXCEPTION = "suppressTableExistenceException";
    public static final String CLICKHOUSE_SETTINGS = "clickhouseSettings";
    public static final String TABLE_MAPPING = "topic2TableMap";
    public static final String ERRORS_TOLERANCE = "errors.tolerance";
    public static final String TABLE_REFRESH_INTERVAL = "tableRefreshInterval";




    
    public static final int MILLI_IN_A_SEC = 1000;
    private static final String databaseDefault = "default";
    public static final int portDefault = 8443;
    public static final String usernameDefault = "default";
    public static final String passwordDefault = "";
    public static final Boolean sslDefault = Boolean.TRUE;
    public static final Integer timeoutSecondsDefault = 30;
    public static final Integer retryCountDefault = 3;
    public static final Integer tableRefreshIntervalDefault = 0;
    public static final Boolean exactlyOnceDefault = Boolean.FALSE;
    public enum StateStores {
        NONE,
        IN_MEMORY,
        REDIS,
        KEEPER_MAP
    }

    private final String hostname;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final boolean sslEnabled;
    private final boolean exactlyOnce;
    private final int timeout;
    private final int retry;
    private final long tableRefreshInterval;
    private final boolean suppressTableExistenceException;
    private final boolean errorsTolerance;

    private final Map<String, String> clickhouseSettings;
    private final Map<String, String> topicToTableMap;

    public static class UTF8String implements ConfigDef.Validator {

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (s != null ) {
                byte[] tmpBytes = s.getBytes(StandardCharsets.UTF_8);
            }
        }

        @Override
        public String toString() {
            return "utf-8 string";
        }
    }

    public ClickHouseSinkConfig(Map<String, String> props) {
        // Extracting configuration
        hostname = props.get(HOSTNAME);
        port = Integer.parseInt(props.getOrDefault(PORT, String.valueOf(portDefault)));
        database = props.getOrDefault(DATABASE, databaseDefault);
        username = props.getOrDefault(USERNAME, usernameDefault);
        password = props.getOrDefault(PASSWORD, passwordDefault).trim();
        sslEnabled = Boolean.parseBoolean(props.getOrDefault(SSL_ENABLED,"false"));
        timeout = Integer.parseInt(props.getOrDefault(TIMEOUT_SECONDS, timeoutSecondsDefault.toString())) * MILLI_IN_A_SEC; // multiple in 1000 milli
        retry = Integer.parseInt(props.getOrDefault(RETRY_COUNT, retryCountDefault.toString()));
        tableRefreshInterval = Long.parseLong(props.getOrDefault(TABLE_REFRESH_INTERVAL, tableRefreshIntervalDefault.toString())) * MILLI_IN_A_SEC; // multiple in 1000 milli
        exactlyOnce = Boolean.parseBoolean(props.getOrDefault(EXACTLY_ONCE,"false"));
        suppressTableExistenceException = Boolean.parseBoolean(props.getOrDefault("suppressTableExistenceException","false"));

        String errorsToleranceString = props.getOrDefault("errors.tolerance", "none").trim();
        errorsTolerance = errorsToleranceString.equalsIgnoreCase("all");

        Map<String, String> clickhouseSettings = new HashMap<>();
        String clickhouseSettingsString = props.getOrDefault("clickhouseSettings", "").trim();
        if (!clickhouseSettingsString.isBlank()) {
            String [] stringSplit = clickhouseSettingsString.split(",");
            for (String clickProp: stringSplit) {
                String [] propSplit = clickProp.trim().split("=");
                if ( propSplit.length == 2 ) {
                    clickhouseSettings.put(propSplit[0].trim(), propSplit[1].trim());
                }
            }
        }
        this.clickhouseSettings = clickhouseSettings;
        this.addClickHouseSetting("insert_quorum", "2", false);
        this.addClickHouseSetting("input_format_skip_unknown_fields", "1", false);
        this.addClickHouseSetting("send_progress_in_http_headers", "1", false);

        topicToTableMap = new HashMap<>();
        String topicToTableMapString = props.getOrDefault(TABLE_MAPPING, "").trim();
        if (!topicToTableMapString.isBlank()) {
            String [] stringSplit = topicToTableMapString.split(",");
            for (String topicToTable: stringSplit) {
                String [] propSplit = topicToTable.trim().split("=");
                if ( propSplit.length == 2 ) {
                    topicToTableMap.put(propSplit[0].trim(), propSplit[1].trim());
                }
            }
        }

        LOGGER.debug("ClickHouseSinkConfig: hostname: {}, port: {}, database: {}, username: {}, sslEnabled: {}, timeout: {}, retry: {}, exactlyOnce: {}",
                hostname, port, database, username, sslEnabled, timeout, retry, exactlyOnce);
        LOGGER.debug("ClickHouseSinkConfig: clickhouseSettings: {}", clickhouseSettings);
        LOGGER.debug("ClickHouseSinkConfig: topicToTableMap: {}", topicToTableMap);
    }

    public void addClickHouseSetting(String key, String value, boolean override) {
        if (clickhouseSettings.containsKey(key)) {
            if (override) {
                clickhouseSettings.put(key, value);
            }
        } else {
            clickhouseSettings.put(key, value);
        }
    }

    public static final ConfigDef CONFIG = createConfigDef();

    private static ConfigDef createConfigDef() {
        ConfigDef configDef = new ConfigDef();

        String group = "Connection";
        int orderInGroup = 0;
        configDef.define(HOSTNAME,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                "hostname",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "ClickHouse Hostname.");
        configDef.define(PORT,
                ConfigDef.Type.INT,
                portDefault,
                ConfigDef.Range.between(1, 65535),
                ConfigDef.Importance.HIGH,
                "port",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "ClickHouse Port.");
        configDef.define(DATABASE,
                ConfigDef.Type.STRING,
                databaseDefault,
                new UTF8String(),
                ConfigDef.Importance.LOW,
                "database",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "Clickhouse database name.");
        configDef.define(USERNAME,
                ConfigDef.Type.STRING,
                passwordDefault,
                ConfigDef.Importance.LOW,
                "username",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "Clickhouse username.");
        configDef.define(PASSWORD,
                ConfigDef.Type.PASSWORD,
                passwordDefault,
                ConfigDef.Importance.LOW,
                "password",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "Password for authentication.");
        configDef.define(SSL_ENABLED,
                ConfigDef.Type.BOOLEAN,
                sslDefault,
                ConfigDef.Importance.LOW,
                "enabled SSL. default: false",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "enable SSL.");
        configDef.define(TIMEOUT_SECONDS,
                ConfigDef.Type.INT,
                timeoutSecondsDefault,
                ConfigDef.Range.between(0, 60 * 10),
                ConfigDef.Importance.LOW,
                "clickhouse driver timeout in sec",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "ClickHouse driver timeout");
        configDef.define(RETRY_COUNT,
                ConfigDef.Type.INT,
                retryCountDefault,
                ConfigDef.Range.between(3, 10),
                ConfigDef.Importance.LOW,
                "clickhouse driver retry ",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "ClickHouse driver retry");
        configDef.define(TABLE_REFRESH_INTERVAL,
                ConfigDef.Type.LONG,
                tableRefreshIntervalDefault,
                ConfigDef.Range.between(0, 60 * 10),
                ConfigDef.Importance.LOW,
                "table refresh interval in sec, default: 0",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "table refresh interval");
        configDef.define(EXACTLY_ONCE,
                ConfigDef.Type.BOOLEAN,
                exactlyOnceDefault,
                ConfigDef.Importance.LOW,
                "enable exactly once semantics. default: false",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "enable exactly once semantics.");
        configDef.define(SUPPRESS_TABLE_EXISTENCE_EXCEPTION,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                "suppress table existence exception. default: false",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "suppress table existence exception.");
        configDef.define(CLICKHOUSE_SETTINGS,
                ConfigDef.Type.LIST,
                "",
                ConfigDef.Importance.LOW,
                "A comma-separated list of ClickHouse settings to be appended to the JDBC URL",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "ClickHouse settings.");
        configDef.define(TABLE_MAPPING,
                ConfigDef.Type.LIST,
                "",
                ConfigDef.Importance.LOW,
                "A comma-separated list of topic=table mappings",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "Table mapping.");
        configDef.define(ERRORS_TOLERANCE,
                ConfigDef.Type.STRING,
                "none",
                ConfigDef.Importance.LOW,
                "Should we tolerate exceptions? default: none",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Tolerate errors.");

        return configDef;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }
    public int getTimeout() {
        return timeout;
    }
    public int getRetry() { return retry; }
    public long getTableRefreshInterval() { 
        return tableRefreshInterval;
    }
    public boolean getExactlyOnce() { return exactlyOnce; }
    public boolean getSuppressTableExistenceException() {
        return suppressTableExistenceException;
    }
    public Map<String, String> getClickhouseSettings() {return clickhouseSettings;}
    public Map<String, String> getTopicToTableMap() {return topicToTableMap;}
    public boolean getErrorsTolerance() { return errorsTolerance; }

}
