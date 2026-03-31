package com.clickhouse.kafka.connect.sink.helper;

import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;



/**
 * This class represents a CH cluster that runs locally as defined by src/testFixtures/docker/cluster/docker-compose.yml.
 * For JUnit tests, the cluster lifecycle is managed by Gradle.
 */
public class ClickHouseCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseCluster.class);

    // Fixed port mapped by docker-compose.yml: "10723:8123" for ch0
    private static final String CLUSTER_HOST = "localhost";
    private static final int CLUSTER_PORT = 10723;

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

    /**
     * Pings the cluster HTTP endpoint with retries. Throws RuntimeException if
     * not reachable after all attempts.
     */
    private static void verifyConnectivity() {
        int maxAttempts = 10;
        int delayMs = 5000;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                URL url = new URL("http://" + CLUSTER_HOST + ":" + CLUSTER_PORT + "/ping");
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setConnectTimeout(2000);
                conn.setReadTimeout(2000);
                if (conn.getResponseCode() == 200) {
                    LOGGER.info("Cluster is reachable at {}:{}", CLUSTER_HOST, CLUSTER_PORT);
                    return;
                }
            } catch (IOException e) {
                LOGGER.info("Cluster not ready yet (attempt {}/{}): {}", attempt, maxAttempts, e.getMessage());
            }
            if (attempt < maxAttempts) {
                try { Thread.sleep(delayMs); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        throw new RuntimeException(
            "ClickHouse cluster not reachable at " + CLUSTER_HOST + ":" + CLUSTER_PORT +
            " after " + maxAttempts + " attempts. Ensure dockerComposeUp completed successfully."
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
                verifyConnectivity();
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
