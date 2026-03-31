package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.*;
import com.clickhouse.kafka.connect.test.junit.extension.FromVersionConditionExtension;
import com.clickhouse.kafka.connect.test.junit.extension.SinceClickHouseVersion;
import com.clickhouse.kafka.connect.util.Utils;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NOTE: this test explicitly connects to the proxy endpoint and avoids setting PROXY_HOST/PROXY_PORT because the client makes requests with absolute URI's to the server when the proxy config is set.
 * TODO: Once <a href="https://github.com/ClickHouse/ClickHouse/issues/58828">this issue</a> is fixed, we can revert this test to use the client proxy config.
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(FromVersionConditionExtension.class)
public class ClickHouseSinkTaskWithSchemaProxyTest extends ClickHouseBase {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseSinkTaskWithSchemaProxyTest.class);

    private static ToxiproxyContainer toxiproxy = null;
    private static Proxy proxy = null;

    private static final int PROXY_PORT = 8667;

    private static final CreateTableStatement SINGLE_INT16_TABLE = new CreateTableStatement()
            .column("off16", "Int16")
            .engine("MergeTree").orderByColumn("off16");

    private static final CreateTableStatement MAP_TYPES_TABLE = new CreateTableStatement()
            .column("off16", "Int16")
            .column("map_string_string", "Map(String, String)")
            .column("map_string_int64", "Map(String, Int64)")
            .column("map_int64_string", "Map(Int64, String)")
            .column("map_string_map", "Map(String, Map(String, Int64))")
            .column("map_string_array", "Map(String, Array(String))")
            .column("map_map_map", "Map(String, Map(String, Map(String, String)))")
            .engine("MergeTree").orderByColumn("off16");

    private static final CreateTableStatement ARRAY_TYPES_TABLE = new CreateTableStatement()
            .column("off16", "Int16")
            .column("arr", "Array(String)")
            .column("arr_empty", "Array(String)")
            .column("arr_int8", "Array(Int8)")
            .column("arr_int16", "Array(Int16)")
            .column("arr_int32", "Array(Int32)")
            .column("arr_int64", "Array(Int64)")
            .column("arr_float32", "Array(Float32)")
            .column("arr_float64", "Array(Float64)")
            .column("arr_bool", "Array(Bool)")
            .engine("MergeTree").orderByColumn("off16");

    @BeforeAll
    public void setup() throws IOException {
        super.setup();

        toxiproxy = new ToxiproxyContainer(ClickHouseTestHelpers.TOXIPROXY_DOCKER_IMAGE_NAME)
                .withNetwork(isCluster || isCloud ? Network.newNetwork() : db.getNetwork())
                .withNetworkAliases(ClickHouseTestHelpers.TOXIPROXY_NETWORK_ALIAS);
        if (isCluster) {
            toxiproxy = toxiproxy.withExtraHost("host.docker.internal", "host-gateway");
        }
        toxiproxy.start();

        log.info("Started proxy container: {}", toxiproxy.getControlPort());
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());

        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(getBaseProps());
        String upstream;
        if (isCloud) {
            upstream = String.format("%s:%d", csc.getHostname(), csc.getPort());
        } else if (isCluster) {
            upstream = String.format("host.docker.internal:%d", ClickHouseCluster.getPort());
        } else {
            upstream = String.format("%s:%d", ClickHouseTestHelpers.CLICKHOUSE_DB_NETWORK_ALIAS, ClickHouseProtocol.HTTP.getDefaultPort());
        }
        proxy = toxiproxyClient.createProxy("clickhouse-proxy", "0.0.0.0:" + PROXY_PORT, upstream);
        log.info("Proxy configured {}", proxy.getListen());
    }

    @AfterAll
    public void tearDown() {
        super.tearDown();

        try {
            toxiproxy.stop();
        } catch (Exception e) {
            // ignore
        }
    }

    private Map<String, String> getTestProperties() {
        Map<String, String> props = getBaseProps();
        if (isCloud) {
            // Set the actual cloud hostname as SNI before overriding with ToxiProxy host.
            // When SSL=true (cloud), ToxiProxy acts as a transparent TCP relay; the TLS
            // ClientHello must carry the real cloud hostname as SNI so the server presents
            // its certificate - otherwise server will reject the client handshake because
            // the proxy hostname and server hostname are different.
            // In client v2, setting sslSocketSNI also disables client-side hostname
            // verification (see HttpAPIClientHelper.createHttpClient in client-v2), allowing the cert to
            // be accepted even though the TCP connection target is the proxy.
            props.put(ClickHouseSinkConfig.SSL_SOCKET_SNI, props.get(ClickHouseSinkConfig.HOSTNAME));
        }
        props.put(ClickHouseSinkConfig.HOSTNAME, toxiproxy.getHost());
        props.put(ClickHouseSinkConfig.PORT, String.valueOf(toxiproxy.getMappedPort(PROXY_PORT)));
        return props;
    }


    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void proxyPingTest(ClusterConfig clusterConfig) throws IOException {
        ClickHouseHelperClient chc = createClient(getTestProperties(), false);
        assertTrue(chc.ping());
        proxy.disable();
        assertFalse(chc.ping());
        proxy.enable();
        assertTrue(chc.ping());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void arrayTypesTest(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("array_string_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement(ARRAY_TYPES_TABLE).tableName(topic).clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void mapTypesTest(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("map_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement(MAP_TYPES_TABLE).tableName(topic).clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/33
    public void materializedViewsBug(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("m_array_string_table_test");
        // Drop the MV and its target table before the source table to avoid orphaned dependencies
        ClickHouseTestHelpers.runQuery(chc, String.format("DROP VIEW IF EXISTS `%s_mv`", topic));
        ClickHouseTestHelpers.dropTable(chc, topic + "_mate", clusterConfig);
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement(ARRAY_TYPES_TABLE).tableName(topic).clusterConfig(clusterConfig).execute(chc);
        new CreateTableStatement()
                .tableName(topic + "_mate")
                .column("off16", "Int16")
                .engine("Null")
                .clusterConfig(clusterConfig)
                .execute(chc);
        ClickHouseTestHelpers.runQuery(chc, String.format("CREATE MATERIALIZED VIEW `%s_mv` TO `%s_mate` AS SELECT off16 FROM `%s`", topic, topic, topic));
        Collection<SinkRecord> sr = SchemaTestData.createArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/38
    public void specialCharTableNameTest(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("special-char-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement(MAP_TYPES_TABLE).tableName(topic).clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/62
    public void nullValueDataTest(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("null-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .column("off16", "Int16").column("null_value_data", "Nullable(DateTime64(6, 'UTC'))")
                .engine("MergeTree").orderByColumn("off16").tableName(topic).clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createNullValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));

    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/57
    public void supportDatesTest(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("support-dates-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .column("off16", "Int16").column("date_number", "Nullable(Date)").column("date32_number", "Nullable(Date32)")
                .column("datetime_number", "DateTime").column("datetime64_number", "DateTime64")
                .column("timestamp_int64", "Int64").column("timestamp_date", "DateTime64")
                .column("time_int32", "Int32").column("time_date32", "Date32")
                .column("date_date", "Date").column("datetime_date", "DateTime")
                .engine("MergeTree").orderByColumn("off16").tableName(topic).clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createDateType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void detectUnsupportedDataConversions(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("support-unsupported-dates-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .column("off16", "Int16").column("date_number", "Date").column("date32_number", "Date32")
                .column("datetime_number", "DateTime").column("datetime64_number", "DateTime64")
                .engine("MergeTree").orderByColumn("off16").tableName(topic).clusterConfig(clusterConfig).execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createUnsupportedDataConversions(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        try {
            chst.put(sr);
        } catch (RuntimeException e) {
            assertTrue(Utils.getRootCause(e) instanceof DataException, "Did not detect wrong date conversion ");
        }
        chst.stop();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void withEmptyDataRecordsTest(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schema_empty_records_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .column("off16", "Int16").column("p_int64", "Int64")
                .engine("MergeTree").orderByColumn("off16").tableName(topic).clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createWithEmptyDataRecords(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void withLowCardinalityTest(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props, true);

        String topic = createTopicName("schema_empty_records_lc_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .column("off16", "Int16").column("p_int64", "Int64")
                .column("lc_string", "LowCardinality(String)").column("nullable_lc_string", "LowCardinality(Nullable(String))")
                .engine("MergeTree").orderByColumn("off16").tableName(topic).clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createWithLowCardinality(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void withUUIDTest(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props, true);

        String topic = createTopicName("schema_empty_records_lc_uuid_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .column("off16", "Int16").column("uuid", "UUID")
                .engine("MergeTree").orderByColumn("off16").tableName(topic).clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createWithUUID(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void schemaWithDefaultsTest(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props, true);

        String topic = createTopicName("default-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .column("off16", "Int16").column("default_value_data", "DateTime DEFAULT now()")
                .engine("MergeTree").orderByColumn("off16").tableName(topic).clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createNullValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void schemaWithDecimalTest(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props, true);

        String topic = createTopicName("decimal-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .column("off16", "Int16").column("decimal_14_2", "Decimal(14, 2)")
                .engine("MergeTree").orderByColumn("off16").tableName(topic).clusterConfig(clusterConfig).execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createDecimalValueData(topic, 1);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
        assertEquals(499700, ClickHouseTestHelpers.sumRows(chc, topic, "decimal_14_2", clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void schemaWithBytesTest(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props, true);
        String topic = createTopicName("bytes-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .column("string", "String")
                .engine("MergeTree").orderByColumn("string").tableName(topic).clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createBytesValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    @SinceClickHouseVersion("24.1")
    public void schemaWithTupleLikeInfluxTest(ClusterConfig clusterConfig) {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("tuple-like-influx-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .column("off16", "Int16")
                .column("payload", "Tuple(fields Map(String, Variant(Float64, Int64, String)), tags Map(String, String))")
                .engine("MergeTree").orderByColumn("off16")
                .settings(Map.of("allow_experimental_variant_type", 1))
                .tableName(topic).clusterConfig(clusterConfig).execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createTupleLikeInfluxValueData(topic, 1);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, clusterConfig);

        LongStream.range(0, sr.size()).forEachOrdered(n -> {
            JSONObject row = rows.get((int) n);

            assertEquals(n, row.getInt("off16"));

            JSONObject payload = row.getJSONObject("payload");
            JSONObject fields = payload.getJSONObject("fields");

            assertEquals(1 / (float) (n + 1), fields.getFloat("field1"));
            assertEquals(n, fields.getBigInteger("field2").longValue());
            assertEquals(String.format("Value '%d'", n), fields.getString("field3"));

            JSONObject tags = payload.getJSONObject("tags");
            assertEquals("tag1", tags.getString("tag1"));
            assertEquals("tag2", tags.getString("tag2"));
        });
    }
}
