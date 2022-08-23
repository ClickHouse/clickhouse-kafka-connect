package com.clickhouse.kafka.connect.sink.db.helper;

import com.clickhouse.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ClickHouseHelperClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseHelperClient.class);

    private String hostname = null;
    private int port = -1;
    private String username = "default";
    private String database = "default";
    private String password = "";
    private boolean sslEnabled = false;
    private int timeout = 30*1000;

    private ClickHouseNode server = null;

    public ClickHouseHelperClient(ClickHouseClientBuilder builder) {
        this.hostname = builder.hostname;
        this.port = builder.port;
        this.username = builder.username;
        this.password = builder.password;
        this.database = builder.database;
        this.sslEnabled = builder.sslEnabled;
        this.timeout = builder.timeout;
        this.server = create();
    }

    private ClickHouseNode create() {
        String protocol = "http";
        if (this.sslEnabled)
            protocol += "s";

        String url = String.format("%s://%s:%d/%s", protocol, hostname, port, database);

        LOGGER.info("url: " + url);

        if (username != null && password != null) {
            LOGGER.info(String.format("Adding username [%s] password [%s]  ", username, password));
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
        ClickHouseClient clientPing = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
        LOGGER.info(String.format("server [%s] , timeout [%d]", server, timeout));
        if (clientPing.ping(server, timeout)) {
            LOGGER.info("Ping is successful.");
            return true;
        }
        LOGGER.info("unable to ping to clickhouse server. ");
        return false;
    }

    public ClickHouseNode getServer() {
        return this.server;
    }

    public ClickHouseResponse query(String query) {
        return query(query, null);
    }

    public ClickHouseResponse query(String query, ClickHouseFormat clickHouseFormat) {
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(server) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format

                     .format(clickHouseFormat)
                     .query(query)
                     .executeAndWait()) {
            response.getSummary();
            return response;
        } catch (ClickHouseException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }


    public static class ClickHouseClientBuilder{
        private String hostname = null;
        private int port = -1;
        private String username = "default";
        private String database = "default";
        private String password = "";
        private boolean sslEnabled = false;

        private int timeout = 30*1000;

        public ClickHouseClientBuilder(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
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

        public ClickHouseClientBuilder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public ClickHouseHelperClient build(){
            return new ClickHouseHelperClient(this);
        }

    }
}
