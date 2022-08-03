package com.clickhouse.kafka.connect;

import com.clickhouse.kafka.connect.sink.ClickHouseSinkTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import org.apache.kafka.connect.sink.SinkConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClickHouseSinkConnector extends SinkConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkConnector.class);

    private String hostname;
    private String port;
    private String database;

    private String username;

    private String password;

    private String sslEnabled;


    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";
    public static final String DATABASE = "database";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    public static final String SSL_ENABLED = "ssl";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HOSTNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "hostname")
            .define(PORT, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "port")
            .define(DATABASE, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "database")
            .define(USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "username")
            .define(PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "password")
            .define(SSL_ENABLED, ConfigDef.Type.BOOLEAN, ConfigDef.Importance.LOW, "ssl enabled default is false")

            ;

    private String convertWithStream(Map<String, String> map) {
        String mapAsString = map.keySet().stream()
                .map(key -> key + "=" + map.get(key))
                .collect(Collectors.joining(", ", "{", "}"));
        return mapAsString;
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("start SinkConnect: ");
        hostname = props.get(HOSTNAME);
        port = props.get(PORT);
        database = props.get(DATABASE);
        username = props.get(USERNAME);
        password = props.get(PASSWORD);
        sslEnabled = props.getOrDefault(SSL_ENABLED,"false");
        // topics contains the name of the topics

    }

    @Override
    public Class<? extends Task> taskClass() {
        return ClickHouseSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            if (hostname != null)
                config.put(HOSTNAME, hostname);
            if (port != null )
                config.put(PORT, port);
            else
                config.put(PORT, "8433");
            if (database != null)
                config.put(DATABASE, database);
            else
                config.put(DATABASE, "default");
            if (username != null)
                config.put(USERNAME, username);
            else
                config.put(USERNAME, "default");
            if (password != null)
                config.put(PASSWORD, password);
            else
                config.put(PASSWORD, "");

            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        LOGGER.info("stop SinkConnect");
    }

    @Override
    protected SinkConnectorContext context() {
        return super.context();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return "0.0.1";
    }
}