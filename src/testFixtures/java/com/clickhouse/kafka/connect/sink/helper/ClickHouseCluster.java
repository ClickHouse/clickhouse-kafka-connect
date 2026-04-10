package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;


/**
 * This class represents a CH cluster that runs locally as defined by src/testFixtures/docker/cluster/docker-compose.yml.
 * For JUnit tests, the cluster lifecycle is managed by Gradle.
 */
public class ClickHouseCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseCluster.class);

    // Fixed port mapped by docker-compose.yml: "10723:8123" for ch0
    private static final String CLUSTER_HOST = "localhost";
    private static final int CLUSTER_PORT = 10723;

    public static final String THREE_SHARDS_ONE_REPLICA_EACH = "three_shards_one_replica_each";
    public static final String ONE_SHARD_THREE_REPLICAS = "one_shard_three_replicas";


    private static volatile boolean started = false;

    /**
     * NOTE: this should only be called from SetupListenerForTests
     */
    public static void markStarted() {
        LOGGER.info("ClickHouseCluster marked as started (Gradle-managed compose)");
        started = true;
    }

    public static boolean isStarted() {
        return started;
    }

    public static String getHost() {
        return CLUSTER_HOST;
    }

    public static Integer getPort() {
        return CLUSTER_PORT;
    }

    public static Map<String, String> getClusterProps(String database) {
        return Map.of(
                ClickHouseSinkConnector.HOSTNAME, ClickHouseCluster.getHost(),
                ClickHouseSinkConnector.PORT, ClickHouseCluster.getPort().toString(),
                ClickHouseSinkConnector.DATABASE, database,
                ClickHouseSinkConnector.USERNAME, ClickHouseTestHelpers.USERNAME_DEFAULT,
                ClickHouseSinkConnector.PASSWORD, "",
                ClickHouseSinkConnector.SSL_ENABLED, "false"
        );
    }

    /**
     * This class will be instantiated before any JUnit tests run and only after the cluster has been created by Gradle
     */
    public static class SetupListenerForTests implements LauncherSessionListener {
        public SetupListenerForTests() {}

        @Override
        public void launcherSessionOpened(LauncherSession session) {
            if (ClickHouseTestHelpers.isCluster()) {
                // Cluster should already be started, so verify connectivity before allowing tests to proceed.
                try (var client = ClickHouseTestHelpers.createClient(getClusterProps("default"))) {
                    client.ping();
                }
                markStarted();
                LOGGER.info("Cluster mode: Gradle-managed cluster at {}:{}", CLUSTER_HOST, CLUSTER_PORT);
            }
        }

        @Override
        public void launcherSessionClosed(LauncherSession session) {
            LOGGER.info("Cluster mode: cluster teardown delegated to Gradle");
        }
    }
}
