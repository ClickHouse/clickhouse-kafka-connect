package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ClickHouseContainer;

import java.util.*;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClickHouseSinkTaskWithSchemaTest {

    private static ClickHouseContainer db = null;

    private static ClickHouseHelperClient chc = null;

    @BeforeAll
    private static void setup() {
        db = new ClickHouseContainer("clickhouse/clickhouse-server:22.5");
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


        chc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port)
                .setDatabase(database)
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .build();
        return chc;
    }


    private void dropTable(ClickHouseHelperClient chc, String tableName) {
        String dropTable = String.format("DROP TABLE IF EXISTS %s", tableName);
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(chc.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format


                     .query(dropTable)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();

        } catch (ClickHouseException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void createTable(ClickHouseHelperClient chc, String topic, String createTableQuery) {
        String createTableQueryTmp = String.format(createTableQuery, topic);

        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(chc.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format


                     .query(createTableQueryTmp)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();

        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }

    }

    private int countRows(ClickHouseHelperClient chc, String topic) {
        String queryCount = String.format("select count(*) from %s", topic);
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(chc.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format


                     .query(queryCount)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();
            return response.firstRecord().getValue(0).asInteger();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public Collection<SinkRecord> createArrayType(String topic, int partition) {

        Schema ARRAY_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        Schema ARRAY_INT8_SCHEMA = SchemaBuilder.array(Schema.INT8_SCHEMA).build();
        Schema ARRAY_INT16_SCHEMA = SchemaBuilder.array(Schema.INT16_SCHEMA).build();
        Schema ARRAY_INT32_SCHEMA = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
        Schema ARRAY_INT64_SCHEMA = SchemaBuilder.array(Schema.INT64_SCHEMA).build();
        Schema ARRAY_FLOAT32_SCHEMA = SchemaBuilder.array(Schema.FLOAT32_SCHEMA).build();
        Schema ARRAY_FLOAT64_SCHEMA = SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build();
        Schema ARRAY_BOOLEAN_SCHEMA = SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build();



        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("arr", ARRAY_SCHEMA)
                .field("arr_int8", ARRAY_INT8_SCHEMA)
                .field("arr_int16", ARRAY_INT16_SCHEMA)
                .field("arr_int32", ARRAY_INT32_SCHEMA)
                .field("arr_int64", ARRAY_INT64_SCHEMA)
                .field("arr_float32", ARRAY_FLOAT32_SCHEMA)
                .field("arr_float64", ARRAY_FLOAT64_SCHEMA)
                .field("arr_bool", ARRAY_BOOLEAN_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {

            List<String> arrayTmp = Arrays.asList("1","2");
            List<Byte> arrayInt8Tmp = Arrays.asList((byte)1,(byte)2);
            List<Short> arrayInt16Tmp = Arrays.asList((short)1,(short)2);
            List<Integer> arrayInt32Tmp = Arrays.asList((int)1,(int)2);
            List<Long> arrayInt64Tmp = Arrays.asList((long)1,(long)2);
            List<Float> arrayFloat32Tmp = Arrays.asList((float)1,(float)2);
            List<Double> arrayFloat64Tmp = Arrays.asList((double)1,(double)2);
            List<Boolean> arrayBoolTmp = Arrays.asList(true,false);


            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("arr", arrayTmp)
                    .put("arr_int8", arrayInt8Tmp)
                    .put("arr_int16", arrayInt16Tmp)
                    .put("arr_int32", arrayInt32Tmp)
                    .put("arr_int64", arrayInt64Tmp)
                    .put("arr_float32", arrayFloat32Tmp)
                    .put("arr_float64", arrayFloat64Tmp)
                    .put("arr_bool", arrayBoolTmp)
                    ;


            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        Collection<SinkRecord> collection = array;
        return collection;
    }
    public Collection<SinkRecord> createMapType(String topic, int partition) {

        Schema MAP_SCHEMA_STRING_STRING = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);
        Schema MAP_SCHEMA_STRING_INT64 = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA);
        Schema MAP_SCHEMA_INT64_STRING = SchemaBuilder.map(Schema.INT64_SCHEMA, Schema.STRING_SCHEMA);

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("map_string_string", MAP_SCHEMA_STRING_STRING)
                .field("map_string_int64", MAP_SCHEMA_STRING_INT64)
                .field("map_int64_string", MAP_SCHEMA_INT64_STRING)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {

            Map<String,String> mapStringString = Map.of(
                    "k1", "v1",
                    "k2", "v1"
            );

            Map<String,Long> mapStringLong = Map.of(
                    "k1", (long)1,
                    "k2", (long)2
            );

            Map<Long,String> mapLongString = Map.of(
                    (long)1, "v1",
                    (long)2, "v2"
            );


            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("map_string_string", mapStringString)
                    .put("map_string_int64", mapStringLong)
                    .put("map_int64_string", mapLongString)
                    ;


            SinkRecord sr = new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, NESTED_SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            );

            array.add(sr);
        });
        Collection<SinkRecord> collection = array;
        return collection;
    }

    @Test
    public void arrayTypesTest() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);

        String topic = "array_string_table_test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = createArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    public void mapTypesTest() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);

        String topic = "map_table_test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, map_string_string Map(String, String), map_string_int64 Map(String, Int64), map_int64_string Map(Int64, String)  ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/33
    public void materializedViewsBug() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);

        String topic = "m_array_string_table_test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
        createTable(chc, topic + "mate", "CREATE MATERIALIZED VIEW %s ( `off16` Int16 ) Engine = MergeTree ORDER BY `off16` POPULATE AS SELECT off16 FROM m_array_string_table_test ");
        Collection<SinkRecord> sr = createArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), countRows(chc, topic));
    }

    @AfterAll
    protected static void tearDown() {
        db.stop();
    }
}
