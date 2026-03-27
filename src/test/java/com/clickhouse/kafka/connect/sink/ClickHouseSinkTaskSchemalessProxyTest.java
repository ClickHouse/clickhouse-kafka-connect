package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NOTE: this test explicitly connects to the proxy endpoint and avoids setting PROXY_HOST/PROXY_PORT because the client makes requests with absolute URI's to the server when the proxy config is set.
 * TODO: Once <a href="https://github.com/ClickHouse/ClickHouse/issues/58828">this issue</a> is fixed, we can revert this test to use the client proxy config.
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ClickHouseSinkTaskSchemalessProxyTest extends ClickHouseBase {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseSinkTaskSchemalessProxyTest.class);
    private static ToxiproxyContainer toxiproxy = null;
    private static Proxy proxy = null;

    private static final int PROXY_PORT = 8666;

    private static final CreateTableStatement PRIMITIVE_TYPES_TABLE = new CreateTableStatement()
            .column("off16", "Int16").column("str", "String")
            .column("p_int8", "Int8").column("p_int16", "Int16").column("p_int32", "Int32")
            .column("p_int64", "Int64").column("p_float32", "Float32")
            .column("p_float64", "Float64").column("p_bool", "Bool")
            .engine("MergeTree").orderByColumn("off16");

    private static final CreateTableStatement MAP_TYPES_TABLE = new CreateTableStatement()
            .column("off16", "Int16").column("map_string_string", "Map(String, String)")
            .column("map_string_int64", "Map(String, Int64)").column("map_int64_string", "Map(Int64, String)")
            .column("map_string_map", "Map(String, Map(String, Int64))")
            .column("map_string_array", "Map(String, Array(String))")
            .column("map_map_map", "Map(String, Map(String, Map(String, String)))")
            .engine("MergeTree").orderByColumn("off16");

    @BeforeAll
    public void setup() throws IOException {
        super.setup();

        toxiproxy = new ToxiproxyContainer(TOXIPROXY_DOCKER_IMAGE_NAME).withNetwork(isCloud ? Network.newNetwork() : db.getNetwork()).withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);
        toxiproxy.start();

        log.info("Started proxy container: {}", toxiproxy.getControlPort());
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());

        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(createProps());
        proxy = toxiproxyClient.createProxy("clickhouse-proxy", "0.0.0.0:" + PROXY_PORT, isCloud ? String.format("%s:%d", csc.getHostname(), csc.getPort()) : String.format("%s:%d", CLICKHOUSE_DB_NETWORK_ALIAS, ClickHouseProtocol.HTTP.getDefaultPort()));
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
        Map<String, String> props = createProps();
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

    @Test
    public void proxyPingTest() throws IOException {
        ClickHouseHelperClient chc = createClient(getTestProperties(), false);
        assertTrue(chc.ping());
        proxy.disable();
        assertFalse(chc.ping());
        proxy.enable();
        assertTrue(chc.ping());
    }

    @Test
    public void primitiveTypesTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);
        String topic = "schemaless_primitive_types_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void withEmptyDataRecordsTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);
        String topic = "schemaless_empty_records_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createWithEmptyDataRecords(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void NullableValuesTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);
        String topic = "schemaless_nullable_values_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16").column("str", "String").column("null_str", "Nullable(String)")
                .column("p_int8", "Int8").column("p_int16", "Int16").column("p_int32", "Int32")
                .column("p_int64", "Int64").column("p_float32", "Float32").column("p_float64", "Float64").column("p_bool", "Bool")
                .engine("MergeTree").orderByColumn("off16").execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypesWithNulls(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void arrayTypesTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "schemaless_array_string_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16").column("arr", "Array(String)").column("arr_empty", "Array(String)")
                .column("arr_int8", "Array(Int8)").column("arr_int16", "Array(Int16)").column("arr_int32", "Array(Int32)")
                .column("arr_int64", "Array(Int64)").column("arr_float32", "Array(Float32)").column("arr_float64", "Array(Float64)")
                .column("arr_bool", "Array(Bool)").column("arr_str_arr", "Array(Array(String))")
                .column("arr_arr_str_arr", "Array(Array(Array(String)))").column("arr_map", "Array(Map(String, String))")
                .engine("MergeTree").orderByColumn("off16").execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemalessTestData.createArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void mapTypesTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "schemaless_map_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(MAP_TYPES_TABLE).tableName(topic).execute(chc);

        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemalessTestData.createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/38
    public void specialCharTableNameTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "special-char-table-test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(MAP_TYPES_TABLE).tableName(topic).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemalessTestData.createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void emojisCharsDataTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "emojis_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic).column("off16", "Int16").column("str", "String")
                .engine("MergeTree").orderByColumn("off16").execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createDataWithEmojis(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, ClickHouseTestHelpers.countRowsWithEmojis(chc, topic));
    }

    @Test
    public void tableMappingTest() {
        Map<String, String> props = getTestProperties();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "mapping_table_test=table_mapping_test");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "mapping_table_test";
        String tableName = "table_mapping_test";
        ClickHouseTestHelpers.dropTable(chc, tableName);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(tableName).execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, tableName));
    }

    @Test
    public void decimalDataTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "decimal_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic).column("num", "String").column("decimal_14_2", "Decimal(14, 2)")
                .engine("MergeTree").orderByColumn("num").execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createDecimalTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        assertEquals(499700, ClickHouseTestHelpers.sumRows(chc, topic, "decimal_14_2"));
    }
}
