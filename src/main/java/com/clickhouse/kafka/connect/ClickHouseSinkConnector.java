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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClickHouseSinkConnector extends SinkConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkConnector.class);
    private String endpoints;
    private String hostname;
    private String port;
    private String database;

    private String username;

    private String password;

    private String sslEnabled;

    private String hashFunctionName;

    private int timeout;

    public static final String ENDPOINTS = "endpoints";
    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";
    public static final String DATABASE = "database";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    public static final String SSL_ENABLED = "ssl";
    public static final String HASH_FUNCTION_NAME = "hashFunctionName";
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
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ClickHouseSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(settings);
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
        return config;
    }
}