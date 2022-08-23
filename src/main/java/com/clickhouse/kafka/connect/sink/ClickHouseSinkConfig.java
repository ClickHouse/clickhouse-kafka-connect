package com.clickhouse.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class ClickHouseSinkConfig {

    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";
    public static final String DATABASE = "database";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String SSL_ENABLED = "ssl";
    public static final String TIMEOUT = "timeout";



    private static final String databaseDefault = "default";
    public static final Integer portDefault = Integer.valueOf(8443);
    public static final String usernameDefault = "default";
    public static final String passwordDefault = "";
    public static final Boolean sslDefault = Boolean.TRUE;
    public static final Integer timeoutDefault = Integer.valueOf(30);
    public enum StateStores {
        NONE,
        IN_MEMORY,
        REDIS,
        KEEPER_MAP
    }

    private Map<String, String> settings = null;
    private String hostname;
    private int port;
    private String database;
    private String username;
    private String password;
    private boolean sslEnabled;

    private int timeout;

    public static class UTF8String implements ConfigDef.Validator {

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            try {
                if (s != null ) {
                    byte[] tmpBytes = s.getBytes("UTF-8");
                }
            } catch (UnsupportedEncodingException e) {
                throw new ConfigException(name, o, "String must be non-empty");
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
        port = Integer.valueOf(props.get(PORT)).intValue();
        database = props.getOrDefault(DATABASE, databaseDefault);
        username = props.getOrDefault(USERNAME, usernameDefault);
        password = props.getOrDefault(PASSWORD, passwordDefault).trim();
        sslEnabled = Boolean.valueOf(props.getOrDefault(SSL_ENABLED,"false")).booleanValue();
        timeout = Integer.valueOf(props.getOrDefault(TIMEOUT, "30")).intValue() * 1000; // multiple in 1000 milli
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
                ConfigDef.Range.between(1024,Integer.MAX_VALUE),
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
                "Clickhouse username password.");
        configDef.define(SSL_ENABLED,
                ConfigDef.Type.BOOLEAN,
                sslDefault,
                ConfigDef.Importance.LOW,
                "ssl enabled default is false",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "if using ssl please use true.");
        configDef.define(TIMEOUT,
                ConfigDef.Type.INT,
                timeoutDefault,
                ConfigDef.Range.between(0, 60 * 10),
                ConfigDef.Importance.LOW,
                "clickhouse driver timeout in sec",
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "ClickHouse driver timeout");

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
}
