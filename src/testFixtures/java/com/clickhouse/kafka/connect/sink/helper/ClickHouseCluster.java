package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;


/**
 * This class represents a CH cluster that runs locally as defined by src/testFixtures/docker/cluster/docker-compose.yml.
 */
public class ClickHouseCluster {
    // fixed port mapped by docker-compose.yml: "10726:8123" for clickhouse-nginx
    // requests to the cluster are round-robin'ed
    private static final String CLUSTER_HOST = "localhost";
    private static final int CLUSTER_PORT = 10726;
    private static final File composeFile = new File("src/testFixtures/docker/clickhouse/cluster/docker-compose.yml");
    private final ComposeContainer container;

    public static final String THREE_SHARDS_ONE_REPLICA_EACH = "three_shards_one_replica_each";
    public static final String ONE_SHARD_THREE_REPLICAS = "one_shard_three_replicas";

    public ClickHouseCluster() {
        this.container = new ComposeContainer(composeFile);
    }

    public static Integer getPort() {
        return CLUSTER_PORT;
    }

    public static Map<String, String> getClusterProps(String database) {
        return Map.of(
                ClickHouseSinkConnector.HOSTNAME, CLUSTER_HOST,
                ClickHouseSinkConnector.PORT, getPort().toString(),
                ClickHouseSinkConnector.DATABASE, database,
                ClickHouseSinkConnector.USERNAME, ClickHouseTestHelpers.USERNAME_DEFAULT,
                ClickHouseSinkConnector.PASSWORD, "",
                ClickHouseSinkConnector.SSL_ENABLED, "false"
        );
    }

    public void start() {
        container.start();
    }

    public void stop() {
        container.stop();
    }
}
