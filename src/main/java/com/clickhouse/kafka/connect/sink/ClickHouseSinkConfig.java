package com.clickhouse.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ClickHouseSinkConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkConfig.class);
    public static final String ENDPOINTS = "endpoints";
    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";
    public static final String DATABASE = "database";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String SSL_ENABLED = "ssl";
    public static final String TIMEOUT_SECONDS = "timeoutSeconds";
    public static final String RETRY_COUNT = "retryCount";
    public static final String EXACTLY_ONCE = "exactlyOnce";
    public static final String HASH_FUNCTION_NAME = "hashFunctionName";

    public static final int MILLI_IN_A_SEC = 1000;
    private static final String databaseDefault = "default";
    public static final String endpointsDefault = "endpoints";
    public static final String hostnameDefault = "hostname";
    public static final int portDefault = 8443;
    public static final String usernameDefault = "default";
    public static final String passwordDefault = "";
    public static final Boolean sslDefault = Boolean.TRUE;
    public static final Integer timeoutSecondsDefault = 30;
    public static final Integer retryCountDefault = 3;
    public static final Boolean exactlyOnceDefault = Boolean.FALSE;
    public static final String hashFunctionNameDefault = "default";
    public enum StateStores {
        NONE,
        IN_MEMORY,
        REDIS,
        KEEPER_MAP
    }

    private Map<String, String> settings = null;
    private String endpoints;
    private String hostname;
    private int port;
    private String database;
    private String username;
    private String password;
    private boolean sslEnabled;
    private boolean exactlyOnce;
    private String hashFunctionName;

    private int timeout;

    private int retry;

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
        endpoints = props.getOrDefault(ENDPOINTS, endpointsDefault);
        if (!endpoints.equals(endpointsDefault)){ // endpoints takes precedence over hostname and ports.
            hostname = props.getOrDefault(HOSTNAME, hostnameDefault);
            LOGGER.info("using all endpoints: " + endpoints);
            if (!hostname.equals(hostnameDefault)) {LOGGER.info("ignoring hostname: "+hostname);}
        } else {
            hostname = props.get(HOSTNAME);
            LOGGER.info("using single host: " + hostname);
        }
        port = Integer.parseInt(props.getOrDefault(PORT, String.valueOf(portDefault)));
        database = props.getOrDefault(DATABASE, databaseDefault);
        username = props.getOrDefault(USERNAME, usernameDefault);
        password = props.getOrDefault(PASSWORD, passwordDefault).trim();
        sslEnabled = Boolean.parseBoolean(props.getOrDefault(SSL_ENABLED,"false"));
        timeout = Integer.parseInt(props.getOrDefault(TIMEOUT_SECONDS, timeoutSecondsDefault.toString())) * MILLI_IN_A_SEC; // multiple in 1000 milli
        retry = Integer.parseInt(props.getOrDefault(RETRY_COUNT, retryCountDefault.toString()));
        exactlyOnce = Boolean.parseBoolean(props.getOrDefault(EXACTLY_ONCE,"false"));
        hashFunctionName = props.getOrDefault(HASH_FUNCTION_NAME, hashFunctionNameDefault);
        LOGGER.info("exactlyOnce: " + exactlyOnce);
        LOGGER.info("props: " + props);
    }

    public static final ConfigDef CONFIG = createConfigDef();

    private static ConfigDef createConfigDef() {
        ConfigDef configDef = new ConfigDef();

        String group = "Connection";
        int orderInGroup = 0;
        configDef.define(ENDPOINTS,
                ConfigDef.Type.STRING,
                endpointsDefault,
                ConfigDef.Importance.HIGH,
                "endpoints",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "Endpoints of ClickHouse Nodes.");
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
        configDef.define(HASH_FUNCTION_NAME,
                ConfigDef.Type.STRING,
                hashFunctionNameDefault,
                ConfigDef.Importance.LOW,
                "hash function name",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "hash function name for distributing messages among endpoints.");
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
        configDef.define(EXACTLY_ONCE,
                ConfigDef.Type.BOOLEAN,
                exactlyOnceDefault,
                ConfigDef.Importance.LOW,
                "enable exactly once semantics. default: false",
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                "enable exactly once semantics.");

        return configDef;
    }
    public String getEndpoints() { return endpoints; }
    public List<String> getEndpoints_array() {
        String[] eps = endpoints.split(",");
        for (int i=0; i<eps.length; i++) {
            String ep = eps[i].strip();
            if (!ep.isEmpty()) {
                eps[i] = ep;
            }
        }
        Arrays.sort(eps); // Consistently sort endpoints.
        return Arrays.asList(eps);
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
    public String getHashFunctionName() {return hashFunctionName; }

    public int getTimeout() {
        return timeout;
    }
    public int getRetry() { return retry; }
    public boolean getExactlyOnce() { return exactlyOnce; }

    public void updateHostNameAndPort(String hostnameAndPort) {
        if (hostnameAndPort.contains(":")) {
            String[] hp = hostnameAndPort.split(":");
            if (hp.length == 2) {
                hostname = hp[0];
                port = Integer.parseInt(hp[1]);
                return;
            }
        }
        LOGGER.error("failed to parse endpoint " + hostnameAndPort);
    }
}
