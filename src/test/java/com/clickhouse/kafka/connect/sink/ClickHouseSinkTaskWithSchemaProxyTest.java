package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.*;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.SchemaTestData;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class ClickHouseSinkTaskWithSchemaProxyTest {

    private static ClickHouseContainer db = null;
    private static ToxiproxyContainer toxiproxy = null;
    private static Proxy proxy = null;

    private static ClickHouseHelperClient chc = null;

    @BeforeAll
    public static void setup() throws IOException {
        Network network = Network.newNetwork();
        db = new org.testcontainers.clickhouse.ClickHouseContainer("clickhouse/clickhouse-server:22.5").withNetwork(network).withNetworkAliases("clickhouse");
        db.start();

        toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.7.0").withNetwork(network).withNetworkAliases("toxiproxy");
        toxiproxy.start();

        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        proxy = toxiproxyClient.createProxy("clickhouse-proxy", "0.0.0.0:8666", "clickhouse:" + ClickHouseProtocol.HTTP.getDefaultPort());
    }

    private ClickHouseHelperClient createClient(Map<String,String> props) {
        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(props);

        String hostname = csc.getHostname();
        int port = csc.getPort();
        String database = csc.getDatabase();
        String username = csc.getUsername();
        String password = csc.getPassword();
        boolean sslEnabled = csc.isSslEnabled();
        int timeout = csc.getTimeout();


        chc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port, csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
                .setDatabase(database)
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .build();
        return chc;
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
        ClickHouseHelperClient chc = createClient(getTestProperties());
        assertTrue(chc.ping());
        proxy.disable();
        assertFalse(chc.ping());
        proxy.enable();
    }

    @Test
    public void arrayTypesTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "array_string_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), `arr_int8` Array(Int8), `arr_int16` Array(Int16), " +
                "`arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createArrayType(topic, 1);

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

        String topic = "map_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, map_string_string Map(String, String), map_string_int64 Map(String, Int64), " +
                "map_int64_string Map(Int64, String)  ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/33
    public void materializedViewsBug() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "m_array_string_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), `arr_int8` Array(Int8), `arr_int16` Array(Int16), " +
                "`arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
        ClickHouseTestHelpers.createTable(chc, topic + "mate", "CREATE MATERIALIZED VIEW %s ( `off16` Int16 ) Engine = MergeTree ORDER BY `off16` POPULATE AS SELECT off16 FROM m_array_string_table_test ");
        Collection<SinkRecord> sr = SchemaTestData.createArrayType(topic, 1);

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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, map_string_string Map(String, String), map_string_int64 Map(String, Int64), " +
                "map_int64_string Map(Int64, String)  ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/62
    public void nullValueDataTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "null-value-table-test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, null_value_data Nullable(DateTime64(6, 'UTC')) ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createNullValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));

    }

    @Test
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/57
    public void supportDatesTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "support-dates-table-test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, date_number Nullable(Date), date32_number Nullable(Date32), datetime_number DateTime, " +
                "datetime64_number DateTime64, timestamp_int64 Int64, timestamp_date DateTime64, time_int32 Int32, time_date32 Date32, date_date Date, datetime_date DateTime ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createDateType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }
    @Test
    public void detectUnsupportedDataConversions() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "support-unsupported-dates-table-test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, date_number Date, date32_number Date32, datetime_number DateTime, datetime64_number DateTime64) Engine = MergeTree ORDER BY off16");
        
        Collection<SinkRecord> sr = SchemaTestData.createUnsupportedDataConversions(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        assertThrows(DataException.class, () -> chst.put(sr), "Did not detect wrong date conversion ");
        chst.stop();
    }

    @Test
    public void withEmptyDataRecordsTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "schema_empty_records_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, p_int64 Int64) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = SchemaTestData.createWithEmptyDataRecords(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void withLowCardinalityTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "schema_empty_records_lc_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, p_int64 Int64, lc_string LowCardinality(String), nullable_lc_string LowCardinality(Nullable(String))) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = SchemaTestData.createWithLowCardinality(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void withUUIDTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "schema_empty_records_lc_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, uuid UUID) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = SchemaTestData.createWithUUID(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void schemaWithDefaultsTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "default-value-table-test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, default_value_data DateTime DEFAULT now() ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createNullValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void schemaWithDecimalTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);
  
        String topic = "decimal-value-table-test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, `decimal_14_2` Decimal(14, 2) ) Engine = MergeTree ORDER BY off16");

        Collection<SinkRecord> sr = SchemaTestData.createDecimalValueData(topic, 1);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        assertEquals(499700, ClickHouseTestHelpers.sumRows(chc, topic, "decimal_14_2"));
    }

    @Test
    public void schemaWithBytesTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);
        String topic = "bytes-value-table-test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` (`string` String) Engine = MergeTree ORDER BY `string`");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createBytesValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @AfterAll
    protected static void tearDown() {
        db.stop();
    }
}
