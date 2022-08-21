package com.clickhouse.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;

public class ClickHouseSinkConfig {

    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";
    public static final String DATABASE = "database";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    public static final String SSL_ENABLED = "ssl";


    public enum StateStores {
        NONE,
        IN_MEMORY,
        REDIS,
        KEEPER_MAP
    }

    public static final ConfigDef CONFIG = createConfigDef();

    private static ConfigDef createConfigDef() {
        ConfigDef configDef = new ConfigDef();

        String group = "Connection";
        int orderInGroup = 0;
        configDef.define(HOSTNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The hostname of ClickHouse Cluster hostname", group, ++orderInGroup, ConfigDef.Width.MEDIUM, "ClickHouse Hostname.");
        configDef.define(PORT, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "port", group, ++orderInGroup, ConfigDef.Width.SHORT, "ClickHouse Port.");
        configDef.define(DATABASE, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "database", group, ++orderInGroup, ConfigDef.Width.MEDIUM, "database name if no value use default.");
        configDef.define(USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "username", group, ++orderInGroup, ConfigDef.Width.MEDIUM, "username if no value use default.");
        configDef.define(PASSWORD, ConfigDef.Type.PASSWORD, ConfigDef.Importance.LOW, "password", group, ++orderInGroup, ConfigDef.Width.MEDIUM, "password if no value use empty.");
        configDef.define(SSL_ENABLED, ConfigDef.Type.BOOLEAN, ConfigDef.Importance.LOW, "ssl enabled default is false", group, ++orderInGroup, ConfigDef.Width.MEDIUM, "if using ssl please use true.");
        return configDef;
    }
}
