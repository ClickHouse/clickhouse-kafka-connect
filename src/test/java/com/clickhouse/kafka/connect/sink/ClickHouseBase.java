package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClusterConfig;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseCluster;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.google.crypto.tink.internal.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ClickHouseBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseBase.class);
    protected ClickHouseContainer db;
    protected boolean isCloud = ClickHouseTestHelpers.isCloud();
    protected boolean isCluster = ClickHouseTestHelpers.isCluster();
    protected String database;
    /**
     * ClusterConfig used for database-level DDL (CREATE/DROP DATABASE) in @BeforeAll/@AfterAll.
     * Null when not running in cluster mode.
     * Per-test ClusterConfig comes from the @ParameterizedTest @MethodSource("clusterConfigs") parameter.
     */
    protected ClusterConfig setupClusterConfig;

    /**
     * Returns cluster configurations for @ParameterizedTest @MethodSource("clusterConfigs").
     * Delegates to {@link ClickHouseTestHelpers#clusterConfigs()}.
     */
    public static Stream<ClusterConfig> clusterConfigs() {
        return ClickHouseTestHelpers.clusterConfigs();
    }

    @BeforeAll
    public void setup() throws IOException  {
        if (isCluster) {
            // cluster lifecycle is managed by Gradle and must be started before tests run
            if (!ClickHouseCluster.isStarted()) {
                throw new IOException("cluster is not running - aborting tests");
            }
            // Use THREE_SHARDS for database-level DDL — it covers all 3 nodes.
            // Per-test cluster config comes from the @ParameterizedTest parameter.
            this.setupClusterConfig = ClusterConfig.THREE_SHARDS_ONE_REPLICA_EACH;
        } else if (!isCloud) {
            setupContainer(ClickHouseTestHelpers.CLICKHOUSE_DOCKER_IMAGE);
        }

        setDatabase(String.format("kafka_connect_test_%d_%s", Math.abs(Random.randInt()), System.currentTimeMillis()));
        initialPing();
    }

    public void setupContainer(String clickhouseDockerImage) {
        Network network = Network.newNetwork();

        this.db = new ClickHouseContainer(clickhouseDockerImage)
                .withNetwork(network)
                .withNetworkAliases(ClickHouseTestHelpers.CLICKHOUSE_DB_NETWORK_ALIAS)
                .withPassword("test_password")
                .withCreateContainerCmdModifier(cmd -> {
                    cmd.getHostConfig().withMemory(1024 * 1024 * 1024 * 2L);
                })
                .withExposedPorts(8123)
                .withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");
        db.start();
    }

    @AfterAll
    protected void tearDown() {
        // disable dropping database for debug
        if (isCloud) { // We need to clean up databases in the cloud, we can ignore the local database
            if (database != null) {
                try {
                    dropDatabase(getDatabase());
                } catch (Exception e) {
                    LOGGER.error("Error dropping database", e);
                }
            }
        } else if (isCluster) {
            // do nothing - managed by Gradle
        } else {
            ClickHouseContainer ch = getDb();
            if (ch != null) {
                LOGGER.info("Stopping db container: id={}, port={}", ch.getContainerId(), ch.getMappedPort(8123));
                ch.copyFileFromContainer("/var/log/clickhouse-server/clickhouse-server.log",
                        "./build/reports/tests/server_" + +db.getMappedPort(8123) + ".log");
                ch.copyFileFromContainer("/var/log/clickhouse-server/clickhouse-server.err.log",
                        "./build/reports/tests/server-err_" + db.getMappedPort(8123) + ".log");
                ch.stop();
            }
        }
    }

    public  ClickHouseContainer getDb() {
        return db;
    }

    public  void setDb(ClickHouseContainer db) {
        this.db = db;
    }

    public  String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public static String extractClientVersion() {
        String clientVersion = System.getenv(ClickHouseTestHelpers.CLIENT_VERSION);
        if (clientVersion != null && clientVersion.equals("V1")) {
            return "V1";
        } else {
            return "V2";
        }
    }

    protected ClickHouseHelperClient createClient(Map<String,String> props) {
        return createClient(props, true);
    }
    protected ClickHouseHelperClient createClient(Map<String,String> props, boolean withDatabase) {
        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(props);

        String hostname = csc.getHostname();
        int port = csc.getPort();
        String database = csc.getDatabase();
        String username = csc.getUsername();
        String password = csc.getPassword();
        boolean sslEnabled = csc.isSslEnabled();
        int timeout = csc.getTimeout();
        String clientVersion = csc.getClientVersion();
        boolean useClientV2 = clientVersion.equals("V1") ? false : true;
        ClickHouseHelperClient tmpChc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port, csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
                .setDatabase("default")
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .useClientV2(useClientV2)
                .setSslSocketSni(csc.getSslSocketSni())
                .build();

        if (withDatabase) {
            try (ClickHouseHelperClient setupChc = tmpChc) {
                createDatabase(getDatabase(), setupChc);
            }
            props.put(ClickHouseSinkConnector.DATABASE, getDatabase());
            tmpChc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port, csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
                    .setDatabase(getDatabase())
                    .setUsername(username)
                    .setPassword(password)
                    .sslEnable(sslEnabled)
                    .setTimeout(timeout)
                    .setRetry(csc.getRetry())
                    .useClientV2(useClientV2)
                    .setSslSocketSni(csc.getSslSocketSni())
                    .build();
        }

        return tmpChc;
    }

    protected void createDatabase(String database, ClickHouseHelperClient chc) {
        String clusterClause = (setupClusterConfig != null)
                ? " ON CLUSTER '" + setupClusterConfig.clusterName + "'" : "";
        String createDatabaseQuery = String.format("CREATE DATABASE IF NOT EXISTS `%s`%s", database, clusterClause);
        if (chc.isUseClientV2()) {
            try {
                chc.queryV2(createDatabaseQuery).close();
            } catch (Exception e) {
                LOGGER.info("Failed to create database ", e);
                throw new RuntimeException(e);
            }
        } else {
            chc.queryV1(createDatabaseQuery);
        }
    }

    protected void initialPing() {
        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(getBaseProps());

        String hostname = csc.getHostname();
        int port = csc.getPort();
        String username = csc.getUsername();
        String password = csc.getPassword();
        boolean sslEnabled = csc.isSslEnabled();
        int timeout = csc.getTimeout();

        boolean ping;
        int retry = 0;
        try (ClickHouseHelperClient tmpChc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port, csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
                .setDatabase("default")
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .setSslSocketSni(csc.getSslSocketSni())
                .build()) {
            do {
                LOGGER.info("Pinging ClickHouse server to warm up for tests...");
                ping = tmpChc.ping();
                if (!ping) {
                    LOGGER.info("Unable to ping ClickHouse server, retrying... {}", retry);
                }
            } while (!ping && retry++ < 10);
        }

        if (!ping) {
            throw new RuntimeException("Unable to ping ClickHouse server...");
        }
    }

    protected  void dropDatabase(String database) {
        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(getBaseProps());

        String hostname = csc.getHostname();
        int port = csc.getPort();
        String username = csc.getUsername();
        String password = csc.getPassword();
        boolean sslEnabled = csc.isSslEnabled();
        int timeout = csc.getTimeout();

        try (ClickHouseHelperClient tmpChc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port, csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
                .setDatabase(database)
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .setSslSocketSni(csc.getSslSocketSni())
                .build()) {
            dropDatabase(database, tmpChc);
        }
    }
    protected void dropDatabase(String database, ClickHouseHelperClient chc) {
        String clusterClause = (setupClusterConfig != null)
                ? " ON CLUSTER '" + setupClusterConfig.clusterName + "'" : "";
        String dropDatabaseQuery = String.format("DROP DATABASE IF EXISTS `%s`%s", database, clusterClause);
        try {
            chc.queryV2(dropDatabaseQuery).close();
        } catch (Exception e) {
            LOGGER.info("Failed to drop database ", e);
            throw new RuntimeException(e);
        }
    }

    protected int countRows(ClickHouseHelperClient chc, String database, String topic) {
        String queryCount = String.format("select count(*) from `%s.%s`", database, topic);
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.read(chc.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format


                     .query(queryCount)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();
            return response.firstRecord().getValue(0).asInteger();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    protected Map<String,String> getBaseProps() {
        Map<String, String> props = new HashMap<>();
        String clientVersion = extractClientVersion();
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, clientVersion);
        if (isCloud) {
            props.put(ClickHouseSinkConnector.HOSTNAME, System.getenv(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_HOST));
            props.put(ClickHouseSinkConnector.PORT, ClickHouseTestHelpers.HTTPS_PORT);
            props.put(ClickHouseSinkConnector.DATABASE, database);
            props.put(ClickHouseSinkConnector.USERNAME, ClickHouseTestHelpers.USERNAME_DEFAULT);
            props.put(ClickHouseSinkConnector.PASSWORD, System.getenv(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PASSWORD));
            props.put(ClickHouseSinkConnector.SSL_ENABLED, "true");
            props.put(String.valueOf(ClickHouseClientOption.CONNECTION_TIMEOUT), "60000");
            props.put("clickhouseSettings", "insert_quorum=3");
        } else if (isCluster) {
            props.put(ClickHouseSinkConnector.HOSTNAME, ClickHouseCluster.getHost());
            props.put(ClickHouseSinkConnector.PORT, ClickHouseCluster.getPort().toString());
            props.put(ClickHouseSinkConnector.DATABASE, database);
            props.put(ClickHouseSinkConnector.USERNAME, ClickHouseTestHelpers.USERNAME_DEFAULT);
            props.put(ClickHouseSinkConnector.PASSWORD, "");
            props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        } else {
            props.put(ClickHouseSinkConnector.HOSTNAME, getDb().getHost());
            props.put(ClickHouseSinkConnector.PORT, getDb().getMappedPort(8123).toString());
            props.put(ClickHouseSinkConnector.DATABASE, database);
            props.put(ClickHouseSinkConnector.USERNAME, getDb().getUsername());
            props.put(ClickHouseSinkConnector.PASSWORD, getDb().getPassword());
            props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        }
        return props;
    }

    protected String createTopicName(String name) {
        return String.format("%s_%d", name, System.currentTimeMillis());
    }
}
