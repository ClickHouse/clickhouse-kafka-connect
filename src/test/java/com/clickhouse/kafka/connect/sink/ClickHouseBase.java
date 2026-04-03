package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ClickHouseBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseBase.class);
    protected ClickHouseContainer db;
    protected boolean isCloud = ClickHouseTestHelpers.isCloud();
    protected String database = ClickHouseTestHelpers.DATABASE_DEFAULT;

    @BeforeAll
    public void setup() throws IOException {
        if (!isCloud) {
            setupContainer(ClickHouseTestHelpers.CLICKHOUSE_DOCKER_IMAGE);
        }

        try (var tmpClient = ClickHouseTestHelpers.createClient(getBaseProps())) {
            setDatabase(String.format("kafka_connect_test_%d_%s", Math.abs(Random.randInt()), System.currentTimeMillis()));
            ClickHouseTestHelpers.createDatabase(database, tmpClient);
            tmpClient.ping();
        }
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
        // TODO: disable dropping database for debug
        if (isCloud) { // We need to clean up databases in the cloud, we can ignore the local database
            if (!ClickHouseTestHelpers.DATABASE_DEFAULT.equals(database)) {
                try (var tmpClient = ClickHouseTestHelpers.createClient(getBaseProps())) {
                    ClickHouseTestHelpers.dropDatabase(tmpClient, database);
                } catch (Exception e) {
                    LOGGER.error("Error dropping database", e);
                }
            }
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

    protected Map<String, String> getBaseProps() {
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

    protected String createTestUsername(String baseName) {
        return String.format("%s_%d", baseName, System.currentTimeMillis());
    }
}
