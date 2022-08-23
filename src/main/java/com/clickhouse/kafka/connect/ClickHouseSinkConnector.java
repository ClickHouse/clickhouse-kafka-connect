package com.clickhouse.kafka.connect;

import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkTask;
import org.apache.kafka.common.config.Config;
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

    private int timeout;


    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";
    public static final String DATABASE = "database";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    public static final String SSL_ENABLED = "ssl";
    private static final ConfigDef CONFIG_DEF = ClickHouseSinkConfig.CONFIG;


    private Map<String, String> settings;

    private String convertWithStream(Map<String, String> map) {
        String mapAsString = map.keySet().stream()
                .map(key -> key + "=" + map.get(key))
                .collect(Collectors.joining(", ", "{", "}"));
        return mapAsString;
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("start SinkConnect: ");
        settings = props;

        hostname = props.get(HOSTNAME);
        port = props.get(PORT);
        database = props.get(DATABASE);
        username = props.get(USERNAME);
        password = props.get(PASSWORD).trim(); // TODO: We need to check how we make some filed not mandatory
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

            config.put(SSL_ENABLED, sslEnabled);
            configs.add(config);

            //configs.add(settings);
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

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);
        ClickHouseSinkConfig sinkConfig;
        try {
            sinkConfig = new ClickHouseSinkConfig(connectorConfigs);
        } catch (Exception e) {
            return config;
        }
        // TODO: run validation here

        return config;
    }
}