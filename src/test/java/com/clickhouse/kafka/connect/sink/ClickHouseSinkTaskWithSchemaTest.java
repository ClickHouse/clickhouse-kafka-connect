package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ClickHouseContainer;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.Date;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClickHouseSinkTaskWithSchemaTest {

    private static ClickHouseContainer db = null;

    private static ClickHouseHelperClient chc = null;

    @BeforeAll
    public static void setup() {
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
        String dropTable = String.format("DROP TABLE IF EXISTS `%s`", tableName);
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.read(chc.getServer()) // or client.connect(endpoints)
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
             ClickHouseResponse response = client.read(chc.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format


                     .query(createTableQueryTmp)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();

        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }

    }

    private int countRows(ClickHouseHelperClient chc, String topic) {
        String queryCount = String.format("select count(*) from `%s`", topic);
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

    private int sumRows(ClickHouseHelperClient chc, String topic, String column) {
        String queryCount = String.format("select SUM(`%s`) from `%s`", column, topic);
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.read(chc.getServer()) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format
                     .query(queryCount)
                     .executeAndWait()) {
            return response.firstRecord().getValue(0).asInteger();
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    public Collection<SinkRecord> createWithEmptyDataRecords(String topic, int partition) {
        List<SinkRecord> array = new ArrayList<>();

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("p_int64", Schema.INT64_SCHEMA)
                .build();

        LongStream.range(0, 1000).forEachOrdered(n -> {
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("p_int64", n);

            SinkRecord sr = null;
            if (n % 2 == 0) {
                sr = new SinkRecord(
                        topic,
                        partition,
                        null,
                        null, NESTED_SCHEMA,
                        value_struct,
                        n,
                        System.currentTimeMillis(),
                        TimestampType.CREATE_TIME
                );
            } else {
                sr = new SinkRecord(
                        topic,
                        partition,
                        null,
                        null, NESTED_SCHEMA,
                        null,
                        n,
                        System.currentTimeMillis(),
                        TimestampType.CREATE_TIME
                );
            }
            array.add(sr);
        });
        return array;
    }

    public Collection<SinkRecord> createWithLowCardinality(String topic, int partition) {
        List<SinkRecord> array = new ArrayList<>();

        Schema SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("p_int64", Schema.INT64_SCHEMA)
                .field("lc_string", Schema.STRING_SCHEMA)
                .field("nullable_lc_string", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        LongStream.range(0, 10).forEachOrdered(n -> {
            Struct value_struct = new Struct(SCHEMA)
                    .put("off16", (short)n)
                    .put("p_int64", n)
                    .put("lc_string", "abc")
                    .put("nullable_lc_string", n % 2 == 0 ? "def" : null);

            array.add(new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            ));
        });
        return array;
    }


    public Collection<SinkRecord> createWithUUID(String topic, int partition) {
        List<SinkRecord> array = new ArrayList<>();

        Schema SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("uuid", Schema.STRING_SCHEMA)
                .build();

        LongStream.range(0, 10).forEachOrdered(n -> {
            Struct value_struct = new Struct(SCHEMA)
                    .put("off16", (short)n)
                    .put("uuid", UUID.randomUUID().toString());

            array.add(new SinkRecord(
                    topic,
                    partition,
                    null,
                    null, SCHEMA,
                    value_struct,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME
            ));
        });
        return array;
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
                .field("arr_empty", ARRAY_SCHEMA)
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
            List<String> arrayEmpty = new ArrayList<>();
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
                    .put("arr_empty", arrayEmpty)
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

    public Collection<SinkRecord> createNullValueData(String topic, int partition) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("null_value_data", Schema.OPTIONAL_INT64_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 2).forEachOrdered(n -> {

            Long null_value_data = null;

            if ( n % 2 == 0) {
                null_value_data = Long.valueOf((short)n);
            }
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("null_value_data", null_value_data)
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

    public Collection<SinkRecord> createBytesValueData(String topic, int partition) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("string", Schema.BYTES_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 2).forEachOrdered(n -> {
            Struct value_struct = new Struct(NESTED_SCHEMA).put("string", Long.toBinaryString(n).getBytes());

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

    public Collection<SinkRecord> createDateType(String topic, int partition) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("date_number", Schema.OPTIONAL_INT32_SCHEMA)
                .field("date32_number", Schema.OPTIONAL_INT32_SCHEMA)
                .field("datetime_number", Schema.INT64_SCHEMA)
                .field("datetime64_number", Schema.INT64_SCHEMA)
                .field("timestamp_int64",  Timestamp.SCHEMA)
                .field("timestamp_date", Timestamp.SCHEMA)
                .field("time_int32" , Time.SCHEMA)
                .field("time_date32" , Time.SCHEMA)
                .field("date_date", Time.SCHEMA)
                .field("datetime_date", Timestamp.SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {
            long currentTime = System.currentTimeMillis();
            LocalDate localDate = LocalDate.now();
            Integer localDateInt = (int)localDate.toEpochDay();
            if(n%3 == 0) {
                localDateInt = null;
            }

            LocalDateTime localDateTime = LocalDateTime.now();
            long localDateTimeLong = localDateTime.toEpochSecond(ZoneOffset.UTC);

            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("date_number", localDateInt)
                    .put("date32_number", localDateInt)
                    .put("datetime_number", localDateTimeLong)
                    .put("datetime64_number", currentTime)
                    .put("timestamp_int64", new Date(System.currentTimeMillis()))
                    .put("timestamp_date",  new Date(System.currentTimeMillis()))
                    .put("time_int32", new Date(System.currentTimeMillis()))
                    .put("time_date32", new Date(System.currentTimeMillis()))
                    .put("date_date", new Date(System.currentTimeMillis()))
                    .put("datetime_date", new Date(System.currentTimeMillis()))
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
    public Collection<SinkRecord> createUnsupportedDataConversions(String topic, int partition) {

        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("date_number", Schema.INT64_SCHEMA)
                .field("date32_number", Schema.INT64_SCHEMA)
                .field("datetime_number", Schema.INT32_SCHEMA)
                .field("datetime64_number", Schema.INT32_SCHEMA)
                .build();


        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {
            long currentTime = System.currentTimeMillis();
            LocalDate localDate = LocalDate.now();
            int localDateInt = (int)localDate.toEpochDay();

            LocalDateTime localDateTime = LocalDateTime.now();
            long localDateTimeLong = localDateTime.toEpochSecond(ZoneOffset.UTC);

            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("date_number", localDateTimeLong)
                    .put("date32_number", currentTime)
                    .put("datetime_number", localDateInt)
                    .put("datetime64_number", localDateInt)
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
    public Collection<SinkRecord> createDecimalValueData(String topic, int partition) {
        Schema NESTED_SCHEMA = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("decimal_14_2", Decimal.schema(2))
                .build();

        List<SinkRecord> array = new ArrayList<>();
        LongStream.range(0, 1000).forEachOrdered(n -> {
            Struct value_struct = new Struct(NESTED_SCHEMA)
                    .put("off16", (short)n)
                    .put("decimal_14_2", new BigDecimal(String.format("%d.%d", n, 2)));

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

        return array;
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
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
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
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
        createTable(chc, topic + "mate", "CREATE MATERIALIZED VIEW %s ( `off16` Int16 ) Engine = MergeTree ORDER BY `off16` POPULATE AS SELECT off16 FROM m_array_string_table_test ");
        Collection<SinkRecord> sr = createArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/38
    public void specialCharTableNameTest() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);

        String topic = "special-char-table-test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, map_string_string Map(String, String), map_string_int64 Map(String, Int64), map_int64_string Map(Int64, String)  ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/62
    public void nullValueDataTest() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);

        String topic = "null-value-table-test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, null_value_data Nullable(DateTime64(6, 'UTC')) ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = createNullValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), countRows(chc, topic));

    }

    @Test
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/57
    public void supportDatesTest() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);

        String topic = "support-dates-table-test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, date_number Nullable(Date), date32_number Nullable(Date32), datetime_number DateTime, datetime64_number DateTime64, timestamp_int64 Int64, timestamp_date DateTime64, time_int32 Int32, time_date32 Date32, date_date Date, datetime_date DateTime ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = createDateType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), countRows(chc, topic));
    }
    @Test
    public void detectUnsupportedDataConversions() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);

        String topic = "support-unsupported-dates-table-test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, date_number Date, date32_number Date32, datetime_number DateTime, datetime64_number DateTime64) Engine = MergeTree ORDER BY off16");

        Collection<SinkRecord> sr = createUnsupportedDataConversions(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        assertThrows(DataException.class, () -> chst.put(sr), "Did not detect wrong date conversion ");
        chst.stop();
    }

    @Test
    public void withEmptyDataRecordsTest() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);

        String topic = "schema_empty_records_table_test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, p_int64 Int64) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = createWithEmptyDataRecords(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, countRows(chc, topic));
    }

    @Test
    public void withLowCardinalityTest() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);

        String topic = "schema_empty_records_lc_table_test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, p_int64 Int64, lc_string LowCardinality(String), nullable_lc_string LowCardinality(Nullable(String))) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = createWithLowCardinality(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    public void withUUIDTest() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);

        String topic = "schema_empty_records_lc_table_test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, uuid UUID) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = createWithUUID(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    public void testSchemaWithDefaults() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);

        String topic = "default-value-table-test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, default_value_data DateTime DEFAULT now() ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = createNullValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), countRows(chc, topic));
    }

    @Test
    public void testSchemaWithDecimal() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);
  
        String topic = "decimal-value-table-test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, `decimal_14_2` Decimal(14, 2) ) Engine = MergeTree ORDER BY off16");

        Collection<SinkRecord> sr = createDecimalValueData(topic, 1);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), countRows(chc, topic));
        assertEquals(499700, sumRows(chc, topic, "decimal_14_2"));
    }

    @Test
    public void testSchemaWithBytes() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, db.getHost());
        props.put(ClickHouseSinkConnector.PORT, db.getFirstMappedPort().toString());
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, db.getUsername());
        props.put(ClickHouseSinkConnector.PASSWORD, db.getPassword());
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");

        ClickHouseHelperClient chc = createClient(props);
        String topic = "bytes-value-table-test";
        dropTable(chc, topic);
        createTable(chc, topic, "CREATE TABLE `%s` (`string` String) Engine = MergeTree ORDER BY `string`");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = createBytesValueData(topic, 1);

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
