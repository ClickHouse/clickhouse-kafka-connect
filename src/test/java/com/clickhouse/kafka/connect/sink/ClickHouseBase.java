package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ClickHouseBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseBase.class);
    protected static ClickHouseHelperClient chc = null;
    protected static ClickHouseContainer db = null;
    protected static boolean isCloud = ClickHouseTestHelpers.isCloud();
    protected static String database = null;
    @BeforeAll
    public static void setup() throws IOException  {
        if (database == null) {
            database = String.format("kafka_connect_test_%s", System.currentTimeMillis());
        }
        if (isCloud) {
            initialPing();
            return;
        }
        db = new ClickHouseContainer(ClickHouseTestHelpers.CLICKHOUSE_DOCKER_IMAGE).withPassword("test_password").withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");
        db.start();
    }

    @AfterAll
    protected static void tearDown() {
        if (isCloud) {//We need to clean up databases in the cloud, we can ignore the local database
            if (database != null) {
                try {
                    dropDatabase(database);
                } catch (Exception e) {
                    LOGGER.error("Error dropping database", e);
                }
            }
        }
        if (db != null)
            db.stop();
    }

    public String extractClientVersion() {
        String clientVersion = System.getenv("CLIENT_VERSION");
        if (clientVersion != null && clientVersion.equals("V1")) {
            return "V1";
        } else {
            return "V2";
        }
    }
    protected ClickHouseSinkConfig createConfig() {
        return new ClickHouseSinkConfig(createProps());
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
                .setDatabase(database)
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .useClientV2(useClientV2)
                .build();

        if (withDatabase) {
            createDatabase(ClickHouseBase.database, tmpChc);
            props.put(ClickHouseSinkConnector.DATABASE, ClickHouseBase.database);
            tmpChc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port, csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
                    .setDatabase(ClickHouseBase.database)
                    .setUsername(username)
                    .setPassword(password)
                    .sslEnable(sslEnabled)
                    .setTimeout(timeout)
                    .setRetry(csc.getRetry())
                    .useClientV2(useClientV2)
                    .build();
            }

        chc = tmpChc;
        return chc;
    }

    protected void createDatabase(String database) {
        createDatabase(database, chc);
    }
    protected void createDatabase(String database, ClickHouseHelperClient chc) {
        String createDatabaseQuery = String.format("CREATE DATABASE IF NOT EXISTS `%s`", database);
        if (chc.isUseClientV2()) {
            chc.queryV2(createDatabaseQuery);
        } else {
            chc.queryV1(createDatabaseQuery);
        }
    }

    protected static void initialPing() {
        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(new ClickHouseBase().createProps());

        String hostname = csc.getHostname();
        int port = csc.getPort();
        String username = csc.getUsername();
        String password = csc.getPassword();
        boolean sslEnabled = csc.isSslEnabled();
        int timeout = csc.getTimeout();

        ClickHouseHelperClient tmpChc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port, csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
                .setDatabase("default")
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .build();

        boolean ping;
        int retry = 0;
        do {
            LOGGER.info("Pinging ClickHouse server to warm up for tests...");
            ping = tmpChc.ping();
            if (!ping) {
                LOGGER.info("Unable to ping ClickHouse server, retrying... {}", retry);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } while (!ping && retry++ < 10);

        if (!ping) {
            throw new RuntimeException("Unable to ping ClickHouse server...");
        }
    }

    protected static void dropDatabase(String database) {
        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(new ClickHouseBase().createProps());

        String hostname = csc.getHostname();
        int port = csc.getPort();
        String username = csc.getUsername();
        String password = csc.getPassword();
        boolean sslEnabled = csc.isSslEnabled();
        int timeout = csc.getTimeout();

        ClickHouseHelperClient tmpChc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port, csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
                .setDatabase(database)
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .build();

        dropDatabase(database, tmpChc);
    }
    protected static void dropDatabase(String database, ClickHouseHelperClient chc) {
        String dropDatabaseQuery = String.format("DROP DATABASE IF EXISTS `%s`", database);
        chc.queryV2(dropDatabaseQuery);
    }

    protected void createTable(ClickHouseHelperClient chc, String topic, String createTableQuery) {
        String createTableQueryTmp = String.format(createTableQuery, topic);
        chc.queryV2(createTableQueryTmp);

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

    protected Map<String,String> createProps() {
        Map<String, String> props = new HashMap<>();
        String clientVersion = extractClientVersion();
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, clientVersion);
        if (isCloud) {
            props.put(ClickHouseSinkConnector.HOSTNAME, System.getenv("CLICKHOUSE_CLOUD_HOST"));
            props.put(ClickHouseSinkConnector.PORT, ClickHouseTestHelpers.HTTPS_PORT);
            props.put(ClickHouseSinkConnector.DATABASE, ClickHouseTestHelpers.DATABASE_DEFAULT);
            props.put(ClickHouseSinkConnector.USERNAME, ClickHouseTestHelpers.USERNAME_DEFAULT);
            props.put(ClickHouseSinkConnector.PASSWORD, System.getenv("CLICKHOUSE_CLOUD_PASSWORD"));
            props.put(ClickHouseSinkConnector.SSL_ENABLED, "true");
            props.put(String.valueOf(ClickHouseClientOption.CONNECTION_TIMEOUT), "60000");
            props.put("clickhouseSettings", "insert_quorum=3");
        } else {
            props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
            props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
            props.put(ClickHouseSinkConnector.DATABASE, ClickHouseTestHelpers.DATABASE_DEFAULT);
            props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
            props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
            props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        }
        return props;
    }

    protected String createTopicName(String name) {
        return String.format("%s_%d", name, System.currentTimeMillis());
    }
}
