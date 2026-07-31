package kafka_connector;

import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.ClickHouseWriter;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.util.QueryIdentifier;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.testcontainers.clickhouse.ClickHouseContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@State(Scope.Benchmark)
public class InsertClientBenchmark {

    private static final String BENCHMARK_TOPIC = "jmh_insert_benchmark";
    private static final String CLICKHOUSE_IMAGE_DEFAULT = "clickhouse/clickhouse-server:25.8.18.1";
    private static final String CLICKHOUSE_PASSWORD_DEFAULT = "test_password";
    private static final String CLICKHOUSE_DATABASE_DEFAULT = "default";
    private static final String CLICKHOUSE_USER_DEFAULT = "default";
    private static final int CLICKHOUSE_PORT_DEFAULT = 8123;

    private static final Object CONTAINER_LOCK = new Object();
    private static ClickHouseContainer sharedContainer;
    private static int containerUsers;

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"V1", "V2"})
        String clientVersion;

        @Param({"string", "rowbinary", "json"})
        String insertMode;

        @Param({"10000", "50000", "100000"})
        int rows;

        ClickHouseWriter writer;
        ClickHouseHelperClient adminClient;
        ClickHouseEndpoint endpoint;
        String databaseName;
        String tableName;
        List<Record> records;
        AtomicLong queryCounter;

        @Setup(Level.Trial)
        public void setupTrial() {
            endpoint = resolveEndpoint();
            adminClient = createClient(endpoint, CLICKHOUSE_DATABASE_DEFAULT, true);

            databaseName = "jmh_benchmark_" + UUID.randomUUID().toString().replace("-", "");
            tableName = "insert_" + insertMode + "_" + clientVersion.toLowerCase() + "_" + rows;

            executeSql(String.format("CREATE DATABASE IF NOT EXISTS `%s`", databaseName), null);
            executeSql(String.format(
                    "CREATE TABLE IF NOT EXISTS `%s`.`%s` (`off16` Int16, `str` String) Engine=MergeTree ORDER BY off16",
                    databaseName, tableName), databaseName);

            writer = new ClickHouseWriter(new SinkTaskStatistics(0));
            if (!writer.start(new ClickHouseSinkConfig(writerProps()))) {
                throw new IllegalStateException("Failed to start ClickHouseWriter");
            }

            switch (insertMode) {
                case "rowbinary":
                    records = createRowBinaryRecords(tableName, rows, databaseName);
                    break;
                case "json":
                    records = createJsonRecords(tableName, rows, databaseName);
                    break;
                case "string":
                    records = createStringRecords(tableName, rows, databaseName);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported insertMode: " + insertMode);
            }

            queryCounter = new AtomicLong(0);
        }

        @Setup(Level.Invocation)
        public void setupInvocation() {
            executeSql(String.format("TRUNCATE TABLE `%s`.`%s`", databaseName, tableName), databaseName);
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() {
            if (writer != null) {
                writer.stop();
            }
            if (adminClient != null) {
                try {
                    executeSql(String.format("DROP TABLE IF EXISTS `%s`.`%s`", databaseName, tableName), databaseName);
                    executeSql(String.format("DROP DATABASE IF EXISTS `%s`", databaseName), null);
                } catch (Exception ignored) {
                    // Best-effort cleanup for benchmark resources.
                }
                adminClient.close();
            }
            releaseEndpoint(endpoint);
        }

        private Map<String, String> writerProps() {
            return Map.of(
                    ClickHouseSinkConnector.HOSTNAME, endpoint.host(),
                    ClickHouseSinkConnector.PORT, String.valueOf(endpoint.port()),
                    ClickHouseSinkConnector.USERNAME, endpoint.username(),
                    ClickHouseSinkConnector.PASSWORD, endpoint.password(),
                    ClickHouseSinkConnector.DATABASE, databaseName,
                    ClickHouseSinkConnector.SSL_ENABLED, String.valueOf(endpoint.ssl()),
                    ClickHouseSinkConnector.CLIENT_VERSION, clientVersion,
                    ClickHouseSinkConfig.INSERT_FORMAT, "json"
            );
        }

        private void executeSql(String sql, String databaseForRequest) {
            QuerySettings settings = new QuerySettings();
            if (databaseForRequest != null && !databaseForRequest.isBlank()) {
                settings.setDatabase(databaseForRequest);
            }
            try (QueryResponse ignored = adminClient.getClient().query(sql, settings).get()) {
                // no-op: execute without helper-level per-query stdout logging
            } catch (Exception e) {
                throw new RuntimeException("Failed SQL: " + sql, e);
            }
        }
    }

    @Benchmark
    public void insert(BenchmarkState state) throws Exception {
        String queryId = "jmh-" + state.insertMode + "-" + state.clientVersion + "-" + state.queryCounter.incrementAndGet();
        state.writer.doInsert(state.records, new QueryIdentifier(BENCHMARK_TOPIC, queryId));
    }

    private static List<Record> createRowBinaryRecords(String topic, int totalRows, String database) {
        Schema schema = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("str", Schema.STRING_SCHEMA)
                .build();

        List<Record> result = new ArrayList<>(totalRows);
        for (int n = 0; n < totalRows; n++) {
            Struct value = new Struct(schema)
                    .put("off16", (short) n)
                    .put("str", "rowbinary_value_" + n);
            SinkRecord sinkRecord = new SinkRecord(
                    topic,
                    0,
                    null,
                    null,
                    schema,
                    value,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME);
            result.add(Record.convert(sinkRecord, false, ".", database, false));
        }
        return result;
    }

    private static List<Record> createJsonRecords(String topic, int totalRows, String database) {
        List<Record> result = new ArrayList<>(totalRows);
        for (int n = 0; n < totalRows; n++) {
            Map<String, Object> value = Map.of(
                    "off16", (short) n,
                    "str", "json_value_" + n
            );
            SinkRecord sinkRecord = new SinkRecord(
                    topic,
                    0,
                    null,
                    null,
                    null,
                    value,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME);
            result.add(Record.convert(sinkRecord, false, ".", database, false));
        }
        return result;
    }

    private static List<Record> createStringRecords(String topic, int totalRows, String database) {
        List<Record> result = new ArrayList<>(totalRows);
        for (int n = 0; n < totalRows; n++) {
            String jsonEachRowLine = String.format("{\"off16\":%d,\"str\":\"string_value_%d\"}%n", (short) n, n);
            SinkRecord sinkRecord = new SinkRecord(
                    topic,
                    0,
                    null,
                    null,
                    null,
                    jsonEachRowLine,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME);
            result.add(Record.convert(sinkRecord, false, ".", database, false));
        }
        return result;
    }

    private static ClickHouseHelperClient createClient(ClickHouseEndpoint endpoint, String database, boolean useClientV2) {
        return new ClickHouseHelperClient.ClickHouseClientBuilder(endpoint.host(), endpoint.port(), null, null, -1)
                .setDatabase(database)
                .setUsername(endpoint.username())
                .setPassword(endpoint.password())
                .sslEnable(endpoint.ssl())
                .useClientV2(useClientV2)
                .build();
    }

    private static ClickHouseEndpoint resolveEndpoint() {
        String externalHost = System.getenv("CLICKHOUSE_HOST");
        if (externalHost != null && !externalHost.isBlank()) {
            int port = Integer.parseInt(System.getenv().getOrDefault("CLICKHOUSE_PORT", String.valueOf(CLICKHOUSE_PORT_DEFAULT)));
            String username = System.getenv().getOrDefault("CLICKHOUSE_USER", CLICKHOUSE_USER_DEFAULT);
            String password = System.getenv().getOrDefault("CLICKHOUSE_PASSWORD", "");
            boolean ssl = Boolean.parseBoolean(System.getenv().getOrDefault("CLICKHOUSE_SSL", "false"));
            return new ClickHouseEndpoint(externalHost, port, username, password, ssl, false);
        }

        synchronized (CONTAINER_LOCK) {
            if (sharedContainer == null) {
                String image = System.getenv().getOrDefault("CLICKHOUSE_IMAGE", CLICKHOUSE_IMAGE_DEFAULT);
                sharedContainer = new ClickHouseContainer(image)
                        .withPassword(CLICKHOUSE_PASSWORD_DEFAULT);
                sharedContainer.start();
            }
            containerUsers++;
            return new ClickHouseEndpoint(
                    sharedContainer.getHost(),
                    sharedContainer.getMappedPort(CLICKHOUSE_PORT_DEFAULT),
                    sharedContainer.getUsername(),
                    sharedContainer.getPassword(),
                    false,
                    true
            );
        }
    }

    private static void releaseEndpoint(ClickHouseEndpoint endpoint) {
        if (endpoint == null || !endpoint.fromContainer()) {
            return;
        }
        synchronized (CONTAINER_LOCK) {
            containerUsers--;
            if (containerUsers <= 0 && sharedContainer != null) {
                sharedContainer.stop();
                sharedContainer = null;
                containerUsers = 0;
            }
        }
    }

    private record ClickHouseEndpoint(
            String host,
            int port,
            String username,
            String password,
            boolean ssl,
            boolean fromContainer
    ) {
    }
}
