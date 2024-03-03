package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClickHouseSinkTaskSchemalessTest {

    private static ClickHouseContainer db = null;

    private static ClickHouseHelperClient chc = null;

    @BeforeAll
    public static void setup() {
        db = new ClickHouseContainer(ClickHouseTestHelpers.CLICKHOUSE_DOCKER_IMAGE);
        db.start();

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
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        return props;
    }



    @Test
    public void primitiveTypesTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
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
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
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
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = "schemaless_nullable_values_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `null_str` Nullable(String), `p_int8` Int8, " +
                "`p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), `arr_int8` Array(Int8), " +
                "`arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, map_string_string Map(String, String), map_string_int64 Map(String, Int64), " +
                "map_int64_string Map(Int64, String)  ) Engine = MergeTree ORDER BY off16");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, map_string_string Map(String, String), map_string_int64 Map(String, Int64), " +
                "map_int64_string Map(Int64, String)  ) Engine = MergeTree ORDER BY off16");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = SchemalessTestData.createDataWithEmojis(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, ClickHouseTestHelpers.countRowsWithEmojis(chc, topic));
    }

    @Test
    public void decimalDataTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

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

    @Test
    public void nullableDecimalDataTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "nullable_decimal_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `num` String, `decimal_14_2` Nullable(Decimal(14, 2))) Engine = MergeTree ORDER BY num");
        Collection<SinkRecord> sr = SchemalessTestData.createNullableDecimalTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        assertEquals(450180, ClickHouseTestHelpers.sumRows(chc, topic, "decimal_14_2"));
    }

    @Test
    public void overlappingDataTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);
        String topic = "schemaless_primitive_types_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        List<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);
        List<SinkRecord> smallerCollection = sr.subList(0, sr.size() / 2);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(smallerCollection);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @AfterAll
    protected static void tearDown() {
        db.stop();
    }
}
