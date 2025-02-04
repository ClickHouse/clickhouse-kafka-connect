package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ClickHouseSinkTaskSchemalessProxyTest extends ClickHouseBase {
    private static ToxiproxyContainer toxiproxy = null;
    private static Proxy proxy = null;
    @BeforeAll
    public static void setup() throws IOException {
        Network network = Network.newNetwork();
        // Note: we are using a different version of ClickHouse for the proxy - https://github.com/ClickHouse/ClickHouse/issues/58828
        db = new ClickHouseContainer(ClickHouseTestHelpers.CLICKHOUSE_FOR_PROXY_DOCKER_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("clickhouse")
                .withPassword("test_password")
                .withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");
        db.start();

        toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.7.0").withNetwork(network).withNetworkAliases("toxiproxy");
        toxiproxy.start();

        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        proxy = toxiproxyClient.createProxy("clickhouse-proxy", "0.0.0.0:8666", "clickhouse:" + ClickHouseProtocol.HTTP.getDefaultPort());
    }

    private Map<String, String> getTestProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        props.put(ClickHouseSinkConfig.PROXY_TYPE, ClickHouseProxyType.HTTP.name());
        props.put(ClickHouseSinkConfig.PROXY_HOST, toxiproxy.getHost());
        props.put(ClickHouseSinkConfig.PROXY_PORT, String.valueOf(toxiproxy.getMappedPort(8666)));
        return props;
    }

    @Test
    public void proxyPingTest() throws IOException {
        ClickHouseHelperClient chc = createClient(getTestProperties(), false);
        assertTrue(chc.ping());
        proxy.disable();
        assertFalse(chc.ping());
        proxy.enable();
    }

    @Test
    public void primitiveTypesTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props, false);
        String topic = "schemaless_primitive_types_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
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
        ClickHouseHelperClient chc = createClient(props, false);
        String topic = "schemaless_empty_records_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
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
        ClickHouseHelperClient chc = createClient(props, false);
        String topic = "schemaless_nullable_values_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `null_str` Nullable(String), `p_int8` Int8, `p_int16` Int16, " +
                "`p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
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
        ClickHouseHelperClient chc = createClient(props, false);

        String topic = "schemaless_array_string_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), `arr_int8` Array(Int8), " +
                "`arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)," +
                "`arr_str_arr` Array(Array(String)), `arr_arr_str_arr` Array(Array(Array(String))), `arr_map` Array(Map(String, String))  ) Engine = MergeTree ORDER BY off16");
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
        ClickHouseHelperClient chc = createClient(props, false);

        String topic = "schemaless_map_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, map_string_string Map(String, String), map_string_int64 Map(String, Int64), " +
                "map_int64_string Map(Int64, String), map_string_map Map(String, Map(String, Int64)), map_string_array Map(String, Array(String)), " +
                "map_map_map Map(String, Map(String, Map(String, String)))   ) Engine = MergeTree ORDER BY off16");

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
        ClickHouseHelperClient chc = createClient(props, false);

        String topic = "special-char-table-test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, map_string_string Map(String, String), map_string_int64 Map(String, Int64), " +
                "map_int64_string Map(Int64, String), map_string_map Map(String, Map(String, Int64)), map_string_array Map(String, Array(String)), " +
                "map_map_map Map(String, Map(String, Map(String, String)))   ) Engine = MergeTree ORDER BY off16");
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
        ClickHouseHelperClient chc = createClient(props, false);

        String topic = "emojis_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String) Engine = MergeTree ORDER BY off16");
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
        ClickHouseHelperClient chc = createClient(props, false);

        String topic = "mapping_table_test";
        String tableName = "table_mapping_test";
        ClickHouseTestHelpers.dropTable(chc, tableName);
        ClickHouseTestHelpers.createTable(chc, tableName, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
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
        ClickHouseHelperClient chc = createClient(props, false);

        String topic = "decimal_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `num` String, `decimal_14_2` Decimal(14, 2)) Engine = MergeTree ORDER BY num");
        Collection<SinkRecord> sr = SchemalessTestData.createDecimalTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        assertEquals(499700, ClickHouseTestHelpers.sumRows(chc, topic, "decimal_14_2"));
    }
}
