package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.config.ClickHouseProxyType;
import lombok.Getter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.clickhouse.kafka.connect.ClickHouseSinkConnector.CLIENT_VERSION;

@Getter
public class ClickHouseSinkConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkConfig.class);

    //Configuration Names
    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";
    public static final String DATABASE = "database";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String SSL_ENABLED = "ssl";
    public static final String JDBC_CONNECTION_PROPERTIES = "jdbcConnectionProperties";
    public static final String TIMEOUT_SECONDS = "timeoutSeconds";
    public static final String RETRY_COUNT = "retryCount";
    public static final String EXACTLY_ONCE = "exactlyOnce";
    public static final String SUPPRESS_TABLE_EXISTENCE_EXCEPTION = "suppressTableExistenceException";
    public static final String CLICKHOUSE_SETTINGS = "clickhouseSettings";
    public static final String TABLE_MAPPING = "topic2TableMap";
    public static final String ERRORS_TOLERANCE = "errors.tolerance";
    public static final String TABLE_REFRESH_INTERVAL = "tableRefreshInterval";
    public static final String CUSTOM_INSERT_FORMAT_ENABLE = "customInsertFormat";
    public static final String INSERT_FORMAT = "insertFormat";
    public static final String PROXY_TYPE = "proxyType";
    public static final String PROXY_HOST = "proxyHost";
    public static final String PROXY_PORT = "proxyPort";
    public static final String ZK_PATH = "zkPath";
    public static final String ZK_DATABASE = "zkDatabase";
    public static final String ENABLE_DB_TOPIC_SPLIT = "enableDbTopicSplit";
    public static final String DB_TOPIC_SPLIT_CHAR = "dbTopicSplitChar";
    public static final String KEEPER_ON_CLUSTER = "keeperOnCluster";
    public static final String DATE_TIME_FORMAT = "dateTimeFormats";
    public static final String TOLERATE_STATE_MISMATCH = "tolerateStateMismatch";
    public static final String BYPASS_SCHEMA_VALIDATION = "bypassSchemaValidation";
    public static final String BYPASS_FIELD_CLEANUP = "bypassFieldCleanup";
    public static final String IGNORE_PARTITIONS_WHEN_BATCHING = "ignorePartitionsWhenBatching";

    public static final int MILLI_IN_A_SEC = 1000;
    private static final String databaseDefault = "default";
    public static final int portDefault = 8443;
    public static final String usernameDefault = "default";
    public static final String passwordDefault = "";
    public static final Boolean sslDefault = Boolean.TRUE;
    public static final String jdbcConnectionPropertiesDefault = "";
    public static final Integer timeoutSecondsDefault = 30;
    public static final Integer retryCountDefault = 3;
    public static final Integer tableRefreshIntervalDefault = 0;
    public static final Boolean exactlyOnceDefault = Boolean.FALSE;
    public static final Boolean customInsertFormatDefault = Boolean.FALSE;

    private final String hostname;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final boolean sslEnabled;
    private final String jdbcConnectionProperties;
    private final boolean exactlyOnce;
    private final int timeout;
    private final int retry;
    private final long tableRefreshInterval;
    private final boolean suppressTableExistenceException;
    private final boolean errorsTolerance;
    private final Map<String, String> clickhouseSettings;
    private final Map<String, String> topicToTableMap;
    private final ClickHouseProxyType proxyType;
    private final String proxyHost;
    private final int proxyPort;
    private final String zkPath;
    private final String zkDatabase;
    private final boolean enableDbTopicSplit;
    private final String dbTopicSplitChar;
    private final String keeperOnCluster;
    private final Map<String, DateTimeFormatter> dateTimeFormats;
    private final String clientVersion;
    private final boolean tolerateStateMismatch;
    private final boolean bypassSchemaValidation;
    private final boolean bypassFieldCleanup;
    private final boolean ignorePartitionsWhenBatching;
    private final boolean binaryFormatWrtiteJsonAsString;

    public enum InsertFormats {
        NONE,
        CSV,
        TSV,
        JSON
    }

    private boolean bypassRowBinary = false;

    private InsertFormats insertFormat = InsertFormats.NONE;
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
    public static final class ZKPathValidator implements ConfigDef.Validator {

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (s == null || s.isBlank() || !s.startsWith("/")) {
                throw new ConfigException("zkPath cannot be empty and must begin with a forward slash");
            }
        }
    }

    public static final class InsertFormatValidatorAndRecommender implements ConfigDef.Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            Boolean enableCustom = (Boolean) parsedConfig.get(ClickHouseSinkConfig.CUSTOM_INSERT_FORMAT_ENABLE);
            if (enableCustom)
                return Arrays.asList("CSV", "TSV", "JSON");
            else
                return List.of("NONE");
        }
        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    public static final class ProxyTypeValidatorAndRecommender implements ConfigDef.Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            return List.of((Object[]) ClickHouseProxyType.values());
        }
        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    public static final class DbTopicSplitCharValidatorAndRecommender implements ConfigDef.Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            return List.of("_", "-", ".");
        }
        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
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
        jdbcConnectionProperties = props.getOrDefault(JDBC_CONNECTION_PROPERTIES,jdbcConnectionPropertiesDefault).trim();
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
        this.addClickHouseSetting("input_format_skip_unknown_fields", "1", false);
        this.addClickHouseSetting("wait_end_of_query", "1", false);
        this.addClickHouseSetting("async_insert", "0", false);
        //We set this so our ResponseSummary has actual data in it
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

        String insertFormatTmp = props.getOrDefault(INSERT_FORMAT, "node").toLowerCase();
        switch (insertFormatTmp) {
            case "none":
                this.insertFormat = InsertFormats.NONE;
                break;
            case "csv":
                this.insertFormat = InsertFormats.CSV;
                break;
            case "tsv":
                this.insertFormat = InsertFormats.TSV;
                break;
            default:
                this.insertFormat = InsertFormats.JSON;
        }

        String proxyTypeTmp = props.getOrDefault(PROXY_TYPE, "none").toLowerCase();
        switch (proxyTypeTmp) {
            case "http":
                this.proxyType = ClickHouseProxyType.HTTP;
                break;
            case "socks":
                this.proxyType = ClickHouseProxyType.SOCKS;
                break;
            case "direct":
                this.proxyType = ClickHouseProxyType.DIRECT;
                break;
            default:
                this.proxyType = ClickHouseProxyType.IGNORE;
        }
        this.proxyHost = props.getOrDefault(PROXY_HOST, "");
        this.proxyPort = Integer.parseInt(props.getOrDefault(PROXY_PORT, "-1"));
        this.zkPath = props.getOrDefault(ZK_PATH, "/kafka-connect");
        this.zkDatabase = props.getOrDefault(ZK_DATABASE, "connect_state");
        this.enableDbTopicSplit = Boolean.parseBoolean(props.getOrDefault(ENABLE_DB_TOPIC_SPLIT, "false"));
        this.dbTopicSplitChar = props.getOrDefault(DB_TOPIC_SPLIT_CHAR, "");
        this.keeperOnCluster = props.getOrDefault(KEEPER_ON_CLUSTER, "");
        this.bypassRowBinary = Boolean.parseBoolean(props.getOrDefault("bypassRowBinary", "false"));
        this.dateTimeFormats = new HashMap<>();
        String dateTimeFormatsString = props.getOrDefault(DATE_TIME_FORMAT, "").trim();
        if (!dateTimeFormatsString.isBlank()) {
            String [] stringSplit = dateTimeFormatsString.split(";");
            for (String topicToDateTimeFormat: stringSplit) {
                String [] propSplit = topicToDateTimeFormat.trim().split("=");
                if ( propSplit.length == 2 ) {
                    dateTimeFormats.put(propSplit[0].trim(), DateTimeFormatter.ofPattern(propSplit[1].trim()));
                }
            }
        }
        this.clientVersion = props.getOrDefault(CLIENT_VERSION, "V1");
        this.tolerateStateMismatch = Boolean.parseBoolean(props.getOrDefault(TOLERATE_STATE_MISMATCH, "false"));
        this.bypassSchemaValidation = Boolean.parseBoolean(props.getOrDefault(BYPASS_SCHEMA_VALIDATION, "false"));
        this.bypassFieldCleanup = Boolean.parseBoolean(props.getOrDefault(BYPASS_FIELD_CLEANUP, "false"));
        this.ignorePartitionsWhenBatching = Boolean.parseBoolean(props.getOrDefault(IGNORE_PARTITIONS_WHEN_BATCHING, "false"));


        String jsonAsString = getClickhouseSettings().get("input_format_binary_read_json_as_string");
        this.binaryFormatWrtiteJsonAsString = jsonAsString != null && (jsonAsString.equalsIgnoreCase("true") || jsonAsString.equals("1"));

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

        //TODO: At some point we should group these more clearly
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
        configDef.define(JDBC_CONNECTION_PROPERTIES,
                ConfigDef.Type.STRING,
                jdbcConnectionPropertiesDefault,
                ConfigDef.Importance.LOW,
                "Clickhouse JDBC connection properties. default: empty",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "JDBC properties for Clickhouse connection.");
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
        configDef.define(CUSTOM_INSERT_FORMAT_ENABLE,
                ConfigDef.Type.BOOLEAN,
                customInsertFormatDefault,
                ConfigDef.Importance.LOW,
                "enable custom insert format only for org.apache.kafka.connect.storage.StringConverter. default: false",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "enable custom insert format.");
        configDef.define(INSERT_FORMAT,
                ConfigDef.Type.STRING,
                "none",
                ConfigDef.Importance.LOW,
                "Set insert format when using org.apache.kafka.connect.storage.StringConverter",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "Insert format.",
                new InsertFormatValidatorAndRecommender()
                );
        configDef.define(PROXY_TYPE,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.LOW,
                "What type of proxy to use. Default: none",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Proxy type",
                new ProxyTypeValidatorAndRecommender()
        );
        configDef.define(PROXY_HOST,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.LOW,
                "If we're using a proxy, what is the host?",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Proxy host"
        );
        configDef.define(PROXY_PORT,
                ConfigDef.Type.INT,
                -1,
                ConfigDef.Importance.LOW,
                "If we're using a proxy, what is the port?",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Proxy port"
        );
        configDef.define(ZK_PATH,
                ConfigDef.Type.STRING,
                "/kafka-connect",
                new ZKPathValidator(),
                ConfigDef.Importance.LOW,
                "This is the zookeeper path for the state store",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "Zookeeper path"
        );
        configDef.define(ZK_DATABASE,
                ConfigDef.Type.STRING,
                "connect_state",
                ConfigDef.Importance.LOW,
                "This is the database for the state store",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "State store database"
        );
        configDef.define(ENABLE_DB_TOPIC_SPLIT,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                "enable database topic split from topic name. default: false",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "enable database topic split from topic name.");
        configDef.define(DB_TOPIC_SPLIT_CHAR,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.LOW,
                "Database topic split character. Default: none",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Database topic split character",
                new DbTopicSplitCharValidatorAndRecommender()
        );
        configDef.define(KEEPER_ON_CLUSTER,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.LOW,
                "Which cluster keeper is on. Only needed for self-hosted clusters using exactly-once. Default: ''",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Keeper on cluster"
        );
        configDef.define("bypassRowBinary",
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                "Bypass RowBinary format, sometimes needed with data gaps. default: false",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Bypass RowBinary format.");
        configDef.define(DATE_TIME_FORMAT,
                ConfigDef.Type.LIST,
                "",
                ConfigDef.Importance.LOW,
                "Date time formats for parsing date time fields (e.g. 'someDateField=yyyy-MM-dd HH:mm:ss.SSSSSSSSS'). default: ''",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Date time formats.");
        configDef.define(CLIENT_VERSION,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.LOW,
                "Client version",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Client version"
        );
        configDef.define(TOLERATE_STATE_MISMATCH,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                "Tolerate state mismatch. default: false",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Tolerate state mismatch."
        );
        configDef.define(BYPASS_SCHEMA_VALIDATION,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                "Bypass schema validation. default: false",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Bypass schema validation."
        );
        configDef.define(BYPASS_FIELD_CLEANUP,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                "Bypass field cleanup. default: false",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Bypass field cleanup."
        );
        configDef.define(IGNORE_PARTITIONS_WHEN_BATCHING,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                "Ignore partitions when batching. Flag ignored when exactlyOnce=true. default: false",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Ignore partitions when batching."
        );
        return configDef;
    }
}
