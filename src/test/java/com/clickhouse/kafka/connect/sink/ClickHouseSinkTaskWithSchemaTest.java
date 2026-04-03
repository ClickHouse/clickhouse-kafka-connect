package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.kafka.connect.avro.test.Event;
import com.clickhouse.kafka.connect.avro.test.Image;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.sink.helper.SchemaTestData;
import com.clickhouse.kafka.connect.test.TestProtos;
import com.clickhouse.kafka.connect.test.junit.extension.FromVersionConditionExtension;
import com.clickhouse.kafka.connect.test.junit.extension.SinceClickHouseVersion;
import com.clickhouse.kafka.connect.util.Utils;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.connect.protobuf.ProtobufConverterConfig;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseDeploymentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(FromVersionConditionExtension.class)
public class ClickHouseSinkTaskWithSchemaTest extends ClickHouseBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkTaskWithSchemaTest.class);

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
            .column("arr_str_arr", "Array(Array(String))")
            .column("arr_arr_str_arr", "Array(Array(Array(String)))")
            .column("arr_map", "Array(Map(String, String))")
            .orderByColumn("off16");

    private static final CreateTableStatement MAP_TYPES_TABLE = new CreateTableStatement()
            .column("off16", "Int16")
            .column("map_string_string", "Map(String, String)")
            .column("map_string_int64", "Map(String, Int64)")
            .column("map_int64_string", "Map(Int64, String)")
            .column("map_string_map", "Map(String, Map(String, Int64))")
            .column("map_string_array", "Map(String, Array(String))")
            .column("map_map_map", "Map(String, Map(String, Map(String, String)))")
            .orderByColumn("off16");

    private static final CreateTableStatement CHANGE_SCHEMA_TABLE = new CreateTableStatement()
            .column("off16", "Int16")
            .column("string", "String")
            .orderByColumn("`off16`");

    @ParameterizedTest()
    @MethodSource("deploymentTypesForTests")
    public void arrayTypesTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = "array_string_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement(ARRAY_TYPES_TABLE)
                .tableName(topic)
                .deploymentType(deploymentType)
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
        assertTrue(ClickHouseTestHelpers.validateRows(chc, topic, sr, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void arrayNullableSubtypesTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = "array_nullable_subtypes_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("arr_nullable_str", "Array(Nullable(String))")
                .column("arr_empty_nullable_str", "Array(Nullable(String))")
                .column("arr_nullable_int8", "Array(Nullable(Int8))")
                .column("arr_nullable_int16", "Array(Nullable(Int16))")
                .column("arr_nullable_int32", "Array(Nullable(Int32))")
                .column("arr_nullable_int64", "Array(Nullable(Int64))")
                .column("arr_nullable_float32", "Array(Nullable(Float32))")
                .column("arr_nullable_float64", "Array(Nullable(Float64))")
                .column("arr_nullable_bool", "Array(Nullable(Bool))")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createArrayNullableSubtypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void mapTypesTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = "map_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement(MAP_TYPES_TABLE)
                .tableName(topic)
                .deploymentType(deploymentType)
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void nullArrayTypeTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("nullable_array_string_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("arr", "Array(String)")

                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createNullableArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void nullableArrayTypeTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("nullable_array_string_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("arr", "Array(Nullable(String))")

                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createNullableArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/33
    public void materializedViewsBug(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("m_array_string_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic + "mate", deploymentType);
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
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
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        new CreateTableStatement()
                .tableName(topic + "_mate")
                .column("off16", "Int16")
                .engine("Null")
                .deploymentType(deploymentType)
                .execute(chc);
        ClickHouseTestHelpers.runQuery(chc, String.format("CREATE MATERIALIZED VIEW %s_mv TO " + topic + "_mate AS SELECT off16 FROM " + topic, topic));
        Collection<SinkRecord> sr = SchemaTestData.createArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/38
    public void specialCharTableNameTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("special-char-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement(MAP_TYPES_TABLE)
                .tableName(topic)
                .deploymentType(deploymentType)
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/62
    public void nullValueDataTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("null-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("null_value_data", "Nullable(DateTime64(6, 'UTC'))")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createNullValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));

    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/57
    public void supportDatesTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("support-dates-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("date_number", "Nullable(Date)")
                .column("date32_number", "Nullable(Date32)")
                .column("datetime_number", "DateTime")
                .column("datetime64_number", "DateTime64")
                .column("datetime64_3_number", "DateTime64(3)")
                .column("datetime64_6_number", "DateTime64(6)")
                .column("datetime64_9_number", "DateTime64(9)")
                .column("timestamp_int64", "Int64")
                .column("timestamp_date", "DateTime64")
                .column("time_int32", "Int32")
                .column("time_date32", "Date32")
                .column("date_date", "Date")
                .column("datetime_date", "DateTime")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createDateType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));

        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType);
        for (JSONObject row : rows) {
            String dateTime64_9 = row.getString("datetime64_9_number");
            String dateTime64_6 = row.getString("datetime64_6_number");
            String dateTime64_3 = row.getString("datetime64_3_number");

            assertTrue(dateTime64_9.contains(dateTime64_3), dateTime64_3 + " is not a substring of " + dateTime64_9);
            assertTrue(dateTime64_9.contains(dateTime64_6), dateTime64_6 + " is not a substring of " + dateTime64_9);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void supportArrayDateTime64Test(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("support-array-datetime64-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("arr_datetime64_number", "Array(DateTime64)")
                .column("arr_timestamp_date", "Array(DateTime64)")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createArrayDateTime64Type(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void detectUnsupportedDataConversions(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("support-unsupported-dates-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("date_number", "Date")
                .column("date32_number", "Date32")
                .column("datetime_number", "DateTime")
                .column("datetime64_number", "DateTime64")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createUnsupportedDataConversions(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        try {
            chst.put(sr);
        } catch (RuntimeException e) {
            assertInstanceOf(DataException.class, Utils.getRootCause(e), "Did not detect wrong date conversion ");
        }
        chst.stop();
    }


    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void supportZonedDatesStringTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("support-dates-string-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("zoned_date", "DateTime64")
                .column("offset_date", "DateTime64")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createZonedTimestampConversions(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }


    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void supportFormattedDatesStringTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.DATE_TIME_FORMAT, "format_date=yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("support-formatted-dates-string-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("format_date", "DateTime64(9)")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createFormattedTimestampConversions(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
        List<JSONObject> results = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType);
        AtomicBoolean found = new AtomicBoolean(false);
        AtomicInteger count = new AtomicInteger(0);
        sr.forEach(sinkRecord -> {
            JSONObject row = results.get(0);
            if (sinkRecord.value().toString().contains(row.get("format_date").toString())) {
                found.set(true);
                count.incrementAndGet();
            }
        });

        assertTrue(found.get());
        assertEquals(1, count.get());
    }



    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void withEmptyDataRecordsTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("schema_empty_records_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("p_int64", "Int64")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createWithEmptyDataRecords(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void withLowCardinalityTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("schema_empty_records_lc_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("p_int64", "Int64")
                .column("lc_string", "LowCardinality(String)")
                .column("nullable_lc_string", "LowCardinality(Nullable(String))")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createWithLowCardinality(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void withUUIDTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("schema_empty_records_lc_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("uuid", "UUID")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createWithUUID(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void schemaWithDefaultsTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("default-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("default_value_data", "DateTime DEFAULT now()")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createNullValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }


    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void schemaWithEphemeralTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("default-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("default_value_data", "DateTime DEFAULT now()")
                .column("ephemeral_data", "String EPHEMERAL")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createNullValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }


    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void schemaWithDefaultsAndNullableTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("default-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("null_value_data", "Nullable(DateTime)")
                .column("default_value_data", "DateTime DEFAULT now()")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createNullValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void schemaWithDecimalTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("decimal-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("decimal_14_2", "Decimal(14, 2)")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createDecimalValueData(topic, 1);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
        assertEquals(499700, ClickHouseTestHelpers.sumRows(chc, topic, "decimal_14_2", deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void schemaWithFixedStringTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("fixed-string-value-table-test");
        int fixedStringSize = RandomUtils.insecure().randomInt(1, 100);
        LOGGER.info("FixedString size: " + fixedStringSize);
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        final int fss = fixedStringSize;
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("fixed_string_string", "FixedString(" + fss + ")")
                .column("fixed_string_bytes", "FixedString(" + fss + ")")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createFixedStringData(topic, 1, fixedStringSize);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void writeBooleanValueToIntTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("schema-with-boolean-and-int-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("ui1", "UInt8")
                .column("ui2", "UInt16")
                .column("ui3", "UInt32")
                .column("ui4", "UInt64")
                .column("i5", "Int8")
                .column("i6", "Int16")
                .column("i7", "Int32")
                .column("i8", "Int64")
                .column("b1", "Boolean")
                .column("b2", "Boolean")
                .column("b3", "Boolean")
                .column("b4", "Boolean")
                .column("ii", "UInt8")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);

        int totalRecords = 100;
        int partition = 1;
        final List<SinkRecord> sr = new ArrayList<>();

        {
            Schema NESTED_SCHEMA = SchemaBuilder.struct()
                    .field("off16", Schema.INT16_SCHEMA)
                    .field("ui1", Schema.BOOLEAN_SCHEMA)
                    .field("ui2", Schema.BOOLEAN_SCHEMA)
                    .field("ui3", Schema.BOOLEAN_SCHEMA)
                    .field("ui4", Schema.BOOLEAN_SCHEMA)
                    .field("i5", Schema.BOOLEAN_SCHEMA)
                    .field("i6", Schema.BOOLEAN_SCHEMA)
                    .field("i7", Schema.BOOLEAN_SCHEMA)
                    .field("i8", Schema.BOOLEAN_SCHEMA)
                    .field("b1", Schema.INT8_SCHEMA)
                    .field("b2", Schema.INT16_SCHEMA)
                    .field("b3", Schema.INT32_SCHEMA)
                    .field("b4", Schema.INT64_SCHEMA)
                    .field("ii", Schema.INT8_SCHEMA)
                    .build();

            LongStream.range(0, totalRecords).forEachOrdered(n -> {
                Struct value_struct = new Struct(NESTED_SCHEMA)
                        .put("off16", (short) n)
                        .put("ui1", n % 2 == 0)
                        .put("ui2", n % 2 == 0)
                        .put("ui3", n % 2 == 0)
                        .put("ui4", n % 2 == 0)
                        .put("i5", n % 2 == 0)
                        .put("i6", n % 2 == 0)
                        .put("i7", n % 2 == 0)
                        .put("i8", n % 2 == 0)
                        .put("b1", (byte)(n % 2 == 0 ? 1 : 0))
                        .put("b2", (short)(n % 2 == 0 ? 1 : 0))
                        .put("b3", (int)(n % 2 == 0 ? 1 : 0))
                        .put("b4", (long)(n % 2 == 0 ? 1 : 0))
                        .put("ii", (byte)(n % 2 == 0 ? 1 : 0));

                SinkRecord record = new SinkRecord(
                        topic,
                        partition,
                        null,
                        null, NESTED_SCHEMA,
                        value_struct,
                        n,
                        System.currentTimeMillis(),
                        TimestampType.CREATE_TIME
                );

                sr.add(record);
            });
        }


        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
        List<JSONObject> rows =ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType);
        for (int i = 0; i < rows.size(); i++) {
            JSONObject row = rows.get(i);
            int off16 = row.getInt("off16");
            boolean b = off16 % 2 == 0;
            assertEquals(b, row.getBoolean("b1"));
            assertEquals(b, row.getBoolean("b2"));
            assertEquals(b, row.getBoolean("b3"));
            assertEquals(b, row.getBoolean("b4"));

            int intV = b ? 1 : 0;
            assertEquals(intV, row.getInt("ui1"));
            assertEquals(intV, row.getInt("ui2"));
            assertEquals(intV, row.getInt("ui3"));
            assertEquals(intV, row.getInt("ui4"));

            assertEquals(intV, row.getInt("i5"));
            assertEquals(intV, row.getInt("i6"));
            assertEquals(intV, row.getInt("i7"));
            assertEquals(intV, row.getInt("i8"));
            assertEquals(intV, row.getInt("ii"));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void schemaWithFixedStringMismatchTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("fixed-string-mismatch-table-test");
        int fixedStringSize = RandomUtils.insecure().randomInt(2, 100);
        LOGGER.info("FixedString size: " + fixedStringSize);
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        final int fss = fixedStringSize;
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("fixed_string_string", "FixedString(" + (fss - 1) + ")")

                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createFixedStringData(topic, 1, fixedStringSize);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        try {
            chst.put(sr);
        } catch (RuntimeException e) {
            assertInstanceOf(IllegalArgumentException.class, Utils.getRootCause(e), "Could not detect size mismatch for FixedString");
        }
        chst.stop();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void schemaWithNullableDecimalTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("nullable-decimal-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("decimal_14_2", "Nullable(Decimal(14, 2))")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createDecimalValueDataWithNulls(topic, 1);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
        assertEquals(450180, ClickHouseTestHelpers.sumRows(chc, topic, "decimal_14_2", deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void schemaWithBytesTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("bytes-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("string", "String")

                .orderByColumn("`string`")
                .deploymentType(deploymentType)
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createBytesValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void supportEnumTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("enum-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("enum8_type", "Enum8('A' = 1, 'B' = 2, 'C' = 3)")
                .column("enum16_type", "Enum16('A' = 1, 'B' = 2, 'C' = 3, 'D' = 4)")
                .orderByColumn("off16")
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createEnumValueData(topic, 1);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @Disabled("Disabled because it requires a flag on the instance.")
    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    @SinceClickHouseVersion("24.1")
    public void schemaWithTupleOfMapsWithVariantTest(ClickHouseDeploymentType deploymentType) {
        Assumptions.assumeFalse(isCloud, "Skip test since experimental is not available in cloud");
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = "tuple-array-map-variant-table-test";
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);

        String simpleTuple = "Tuple(" +
                "    `variant_with_string` Variant(Double, String)," +
                "    `variant_with_double` Variant(Double, String)," +
                "    `variant_array` Array(Variant(Double, String))," +
                "    `variant_map` Map(String, Variant(Double, String))" +
                "  )";

        String tupleType = "Tuple(" +
                "  `array` Array(String)," +
                "  `map` Map(String, String)," +
                "  `array_array` Array(Array(String))," +
                "  `map_map` Map(String, Map(String, Int64))," +
                "  `array_map` Array(Map(String, String))," +
                "  `map_array` Map(String, Array(String))," +
                "  `array_array_array` Array(Array(Array(String)))," +
                "  `map_map_map` Map(String, Map(String, Map(String, String)))," +
                "  `tuple` " + simpleTuple + "," +
                "  `array_tuple` Array(" + simpleTuple + ")," +
                "  `map_tuple` Map(String, " + simpleTuple + ")," +
                "  `array_array_tuple` Array(Array(" + simpleTuple + "))," +
                "  `map_map_tuple` Map(String, Map(String, " + simpleTuple + "))," +
                "  `array_map_tuple` Array(Map(String, " + simpleTuple + "))," +
                "  `map_array_tuple` Map(String, Array(" + simpleTuple + "))" +
                ")";
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("tuple", tupleType)
                .orderByColumn("`off16`")
                .settings(Map.of("allow_experimental_variant_type", 1))
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createTupleType(topic, 1, 5);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));

        List<JSONObject> allRows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType);
        for (int i = 0; i < sr.size(); i++) {
            JSONObject row = allRows.get(i);

            assertEquals(i, row.getInt("off16"));
            JSONObject tuple = row.getJSONObject("tuple");
            JSONObject nestedTuple = tuple.getJSONObject("tuple");
            assertEquals(1 / (double) 3, nestedTuple.getDouble("variant_with_double"));
        }
    }

    @Disabled("Disabled because it requires a flag on the instance.")
    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    @SinceClickHouseVersion("24.1")
    public void schemaWithNestedTupleMapArrayAndVariant(ClickHouseDeploymentType deploymentType) {
        if (isCloud) {
            LOGGER.warn("Skip test since experimental is not available in cloud");
            return;
        }
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = "nested-tuple-map-array-and-variant-table-test";
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);

        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("nested", "Nested(" +
                        "  `string` String," +
                        "  `decimal` Decimal(14, 2)," +
                        "  `tuple` Tuple(" +
                        "    `map` Map(String, String)," +
                        "    `variant` Variant(Boolean, String)" +
                        "))")
                .orderByColumn("`off16`")
                .settings(Map.of("allow_experimental_variant_type", 1))
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createNestedType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));

        List<JSONObject> allRows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType);
        for (int i = 0; i < allRows.size(); i++) {
            JSONObject row = allRows.get(i);

            assertEquals(i, row.getInt("off16"));

            String expectedString = String.format("v%d", i);

            int expectedNestedSize = i % 4;
            assertEquals(expectedNestedSize, row.getJSONArray("nested.string").length());
            assertEquals(expectedNestedSize, row.getJSONArray("nested.decimal").length());
            assertEquals(expectedNestedSize, row.getJSONArray("nested.tuple").length());

            BigDecimal expectedDecimal = new BigDecimal(String.format("%d.%d", i, 2));

            assertEquals(
                    expectedDecimal.multiply(new BigDecimal(expectedNestedSize)).doubleValue(),
                    row.getJSONArray("nested.decimal").toList().stream()
                            .map(object -> (BigDecimal) object)
                            .reduce(BigDecimal::add)
                            .orElseGet(() -> new BigDecimal(0)).doubleValue()
            );

            final int n = i;
            row.getJSONArray("nested.tuple").toList().forEach(object -> {
                Map<?, ?> tuple = (Map<?, ?>) object;
                if (n % 2 == 0) {
                    assertEquals(n % 8 >= 4, tuple.get("variant"));
                } else {
                    assertEquals(expectedString, tuple.get("variant"));
                }
            });

            row.getJSONArray("nested.string").toList().forEach(object ->
                    assertEquals(expectedString, object)
            );
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void unsignedIntegers(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("unsigned-integers-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("uint8", "UInt8")
                .column("uint16", "UInt16")
                .column("uint32", "UInt32")
                .column("uint64", "UInt64")
                .orderByColumn("`off16`")
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createUnsignedIntegers(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void changeSchemaWhileRunning(ClickHouseDeploymentType deploymentType) throws InterruptedException {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("change-schema-while-running-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement(CHANGE_SCHEMA_TABLE)
                .tableName(topic)
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createSimpleData(topic, 1);

        int numRecords = sr.size();
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));


        String clusterClause = deploymentType.isLocalCluster() ? " ON CLUSTER '" + deploymentType.clusterName + "'" : "";
        ClickHouseTestHelpers.runQuery(chc, String.format("ALTER TABLE `%s`%s ADD COLUMN num32 Nullable(Int32) AFTER string", topic, clusterClause));
        Thread.sleep(5000);
        sr = SchemaTestData.createSimpleExtendWithNullableData(topic, 1, 10000, 2000);
        int numRecordsWithNullable = sr.size();
        chst.put(sr);

        ClickHouseTestHelpers.runQuery(chc, String.format("ALTER TABLE `%s`%s ADD COLUMN num32_default Int32 DEFAULT 0 AFTER num32", topic, clusterClause));
        Thread.sleep(5000);

        sr = SchemaTestData.createSimpleExtendWithDefaultData(topic, 1, 20000, 3000);
        int numRecordsWithDefault = sr.size();
        System.out.println("numRecordsWithDefault: " + numRecordsWithDefault);
        chst.put(sr);
        ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType).forEach(row -> {
            //System.out.println(row);
        });
        chst.stop();
        assertEquals(numRecords + numRecordsWithNullable + numRecordsWithDefault, ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void changeSchemaWhileRunningAddDefaultColumnOldSchemaData(ClickHouseDeploymentType deploymentType) throws InterruptedException {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("change-schema-add-default-old-schema-data-test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement(CHANGE_SCHEMA_TABLE)
                .tableName(topic)
                .deploymentType(deploymentType)
                .execute(chc);

        Collection<SinkRecord> firstBatch = SchemaTestData.createSimpleData(topic, 1, 1000);
        Collection<SinkRecord> secondBatch = SchemaTestData.createSimpleData(topic, 1, 1000).stream()
                .map(record -> new SinkRecord(
                        record.topic(),
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        record.valueSchema(),
                        record.value(),
                        record.kafkaOffset() + 1000,
                        record.timestamp(),
                        record.timestampType()))
                .collect(Collectors.toList());

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(firstBatch);
        assertEquals(firstBatch.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));

        String clusterClause = deploymentType.isLocalCluster() ? " ON CLUSTER '" + deploymentType.clusterName + "'" : "";
        ClickHouseTestHelpers.runQuery(chc, String.format("ALTER TABLE `%s`%s ADD COLUMN num32_default Int32 DEFAULT 42 AFTER string", topic, clusterClause));
        Thread.sleep(5000);

        // Keep writing records with the old schema (without num32_default).
        // Connector should write default markers for the new column.
        chst.put(secondBatch);
        chst.stop();

        assertEquals(firstBatch.size() + secondBatch.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void changeSchemaWhileRunningWithRefreshEnabled(ClickHouseDeploymentType deploymentType) throws InterruptedException {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        props.put(ClickHouseSinkConfig.TABLE_REFRESH_INTERVAL, "1");
        String topic = createTopicName("change-schema-while-running-table-test-with-refresh-enabled");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement(CHANGE_SCHEMA_TABLE)
                .tableName(topic)
                .deploymentType(deploymentType)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createSimpleData(topic, 1);

        int numRecords = sr.size();
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));


        String clusterClause = deploymentType.isLocalCluster() ? " ON CLUSTER '" + deploymentType.clusterName + "'" : "";
        ClickHouseTestHelpers.runQuery(chc, String.format("ALTER TABLE `%s`%s ADD COLUMN num32 Nullable(Int32) AFTER string", topic, clusterClause));
        Thread.sleep(5000);
        sr = SchemaTestData.createSimpleExtendWithNullableData(topic, 1, 10000, 2000);
        int numRecordsWithNullable = sr.size();
        chst.put(sr);

        ClickHouseTestHelpers.runQuery(chc, String.format("ALTER TABLE `%s`%s ADD COLUMN num32_default Int32 DEFAULT 0 AFTER num32", topic, clusterClause));
        Thread.sleep(5000);

        sr = SchemaTestData.createSimpleExtendWithDefaultData(topic, 1, 20000, 3000);
        int numRecordsWithDefault = sr.size();
        System.out.println("numRecordsWithDefault: " + numRecordsWithDefault);
        chst.put(sr);
        ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType).forEach(row -> {
            //System.out.println(row);
        });
        chst.stop();
        assertEquals(numRecords + numRecordsWithNullable + numRecordsWithDefault, ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void tupleTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("tuple-table-test");

        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("string", "String")
                .column("t", "Tuple(`off16` Nullable(Int16), `string` Nullable(String))")
                .orderByColumn("`off16`")
                .deploymentType(deploymentType)
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createTupleSimpleData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void tupleTestWithDefault(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("tuple-table-test-default");

        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("string", "String")
                .column("insert_datetime", "DateTime default now()")
                .column("t", "Tuple(`off16` Nullable(Int16), `string` Nullable(String))")
                .orderByColumn("`off16`")
                .deploymentType(deploymentType)
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createTupleSimpleData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void nestedTupleTestWithDefault(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("nested-tuple-table-test-default");

        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("string", "String")
                .column("insert_datetime", "DateTime default now()")
                .column("t", "Tuple(" +
                        "`off16` Nullable(Int16), " +
                        "`string` Nullable(String), " +
                        "`n` Tuple(`off16` Nullable(Int16), `string` Nullable(String)))")
                .orderByColumn("`off16`")
                .deploymentType(deploymentType)
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createNestedTupleSimpleData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    /**
     * Validates that Jackson Struct serialization produces correct data in
     * ClickHouse for flat, nested, and deeply nested Structs via the JSON
     * insert path (JSONEachRow).  Covers the Gson→Jackson migration by
     * verifying actual inserted values, not just row counts.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void jacksonStructJsonInsertTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("jackson-struct-json-insert");

        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("name", "String")
                .column("active", "Bool")
                .column("score", "Float64")
                .column("t", "Tuple(" +
                        "`off16` Nullable(Int16), " +
                        "`label` Nullable(String), " +
                        "`n` Tuple(`value` Nullable(Int32), `tag` Nullable(String)))")
                .orderByColumn("`off16`")
                .deploymentType(deploymentType)
                .execute(chc);

        Schema innerTupleSchema = SchemaBuilder.struct()
                .field("value", Schema.OPTIONAL_INT32_SCHEMA)
                .field("tag", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema tupleSchema = SchemaBuilder.struct()
                .field("off16", Schema.OPTIONAL_INT16_SCHEMA)
                .field("label", Schema.OPTIONAL_STRING_SCHEMA)
                .field("n", innerTupleSchema)
                .build();

        Schema recordSchema = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("active", Schema.BOOLEAN_SCHEMA)
                .field("score", Schema.FLOAT64_SCHEMA)
                .field("t", tupleSchema)
                .build();

        int totalRecords = 5;
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < totalRecords; i++) {
            Struct innerTuple = new Struct(innerTupleSchema)
                    .put("value", i * 10)
                    .put("tag", "tag_" + i);

            Struct tuple = new Struct(tupleSchema)
                    .put("off16", (short) i)
                    .put("label", "label_" + i)
                    .put("n", innerTuple);

            Struct value = new Struct(recordSchema)
                    .put("off16", (short) i)
                    .put("name", "name_" + i)
                    .put("active", i % 2 == 0)
                    .put("score", i * 1.5)
                    .put("t", tuple);

            records.add(new SinkRecord(
                    topic, 1, null, null,
                    recordSchema, value, i,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME));
        }

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(records);
        chst.stop();

        assertEquals(totalRecords, ClickHouseTestHelpers.countRows(chc, topic, deploymentType));

        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType);
        assertEquals(totalRecords, rows.size());

        // Sort by off16 so we can assert deterministically
        rows.sort((a, b) -> Integer.compare(a.getInt("off16"), b.getInt("off16")));

        for (int i = 0; i < totalRecords; i++) {
            JSONObject row = rows.get(i);
            assertEquals(i, row.getInt("off16"));
            assertEquals("name_" + i, row.getString("name"));
            assertEquals(i % 2 == 0, row.getBoolean("active"));
            assertEquals(i * 1.5, row.getDouble("score"), 0.001);

            JSONObject t = row.getJSONObject("t");
            assertEquals(i, t.getInt("off16"));
            assertEquals("label_" + i, t.getString("label"));

            JSONObject n = t.getJSONObject("n");
            assertEquals(i * 10, n.getInt("value"));
            assertEquals("tag_" + i, n.getString("tag"));
        }
    }

    /**
     * Validates Struct serialization with null fields in nested Tuples —
     * Jackson's JacksonStructSerializer writes explicit nulls which
     * ClickHouse Nullable columns accept correctly.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void jacksonStructWithNullsJsonInsertTest(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("jackson-struct-nulls-json-insert");

        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("name", "Nullable(String)")
                .column("t", "Tuple(`off16` Nullable(Int16), `label` Nullable(String))")
                .orderByColumn("`off16`")
                .deploymentType(deploymentType)
                .execute(chc);

        Schema tupleSchema = SchemaBuilder.struct()
                .field("off16", Schema.OPTIONAL_INT16_SCHEMA)
                .field("label", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema recordSchema = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("t", tupleSchema)
                .build();

        // Record with non-null values
        Struct tuple1 = new Struct(tupleSchema)
                .put("off16", (short) 10)
                .put("label", "hello");
        Struct value1 = new Struct(recordSchema)
                .put("off16", (short) 1)
                .put("name", "Alice")
                .put("t", tuple1);

        // Record with null values in tuple and top-level
        Struct tuple2 = new Struct(tupleSchema)
                .put("off16", null)
                .put("label", null);
        Struct value2 = new Struct(recordSchema)
                .put("off16", (short) 2)
                .put("name", null)
                .put("t", tuple2);

        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(topic, 1, null, null, recordSchema, value1, 0,
                System.currentTimeMillis(), TimestampType.CREATE_TIME));
        records.add(new SinkRecord(topic, 1, null, null, recordSchema, value2, 1,
                System.currentTimeMillis(), TimestampType.CREATE_TIME));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(records);
        chst.stop();

        assertEquals(2, ClickHouseTestHelpers.countRows(chc, topic, deploymentType));

        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType);
        rows.sort((a, b) -> Integer.compare(a.getInt("off16"), b.getInt("off16")));

        // Row 1: all non-null
        JSONObject row1 = rows.get(0);
        assertEquals(1, row1.getInt("off16"));
        assertEquals("Alice", row1.getString("name"));
        JSONObject t1 = row1.getJSONObject("t");
        assertEquals(10, t1.getInt("off16"));
        assertEquals("hello", t1.getString("label"));

        // Row 2: nulls in name and tuple fields
        JSONObject row2 = rows.get(1);
        assertEquals(2, row2.getInt("off16"));
        assertTrue(row2.isNull("name"));
        JSONObject t2 = row2.getJSONObject("t");
        assertTrue(t2.isNull("off16"));
        assertTrue(t2.isNull("label"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void coolSchemaWithRandomFields(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("cool-schema-with-random-field");

        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("processing_time", "DateTime")
                .column("insert_time", "DateTime DEFAULT now()")
                .column("type", "String")
                .column("player", "Tuple(id Nullable(Int64), key Nullable(String), ip Nullable(String), label Nullable(String), device_id Nullable(Int64), player_tracker_id Nullable(Int64), is_new_player Nullable(Bool), player_root Nullable(String), target Nullable(String), `type` Nullable(String), name Nullable(String), processing_time Nullable(Int64), tags Map(String, String), session_id Nullable(String), machine_fingerprint Nullable(String), player_fingerprint Nullable(String))")
                .column("sensor", "Tuple(sensor_id String, origin_device String, session_id String, machine_id Int64, machine_timestamp String)")
                .column("data", "String")
                .column("id", "String")
                .column("desc", "Nullable(String)")
                .column("tag", "Nullable(String)")
                .column("va", "Nullable(Float64)")
                .orderByColumn("`processing_time`")
                .deploymentType(deploymentType)
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createCoolSchemaWithRandomFields(topic, 1);


        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    @SinceClickHouseVersion("24.10")
    public void testWritingJsonAsStringWithRowBinary(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        props.put(ClickHouseSinkConfig.CLICKHOUSE_SETTINGS, "input_format_binary_read_json_as_string=1");
        String topic = createTopicName("schema_json_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);

        Map<String, Serializable> clientSettings = new HashMap<>();
        clientSettings.put(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("struct_content", "JSON")
                .column("json_as_str", "JSON")
                .orderByColumn("off16")
                .settings(clientSettings)
                .deploymentType(deploymentType)
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createJSONType(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    @SinceClickHouseVersion("24.10")
    public void testWritingProtoMessageWithRowBinary(ClickHouseDeploymentType deploymentType) throws Exception {

        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        props.put(ClickHouseSinkConfig.CLICKHOUSE_SETTINGS, "input_format_binary_read_json_as_string=1");
        String topic = createTopicName("protobuf_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);

        TestProtos.TestMessage userMsg = SchemaTestData.createUserMessage();
        TestProtos.TestMessage productMsg = SchemaTestData.createProductMessage();

        MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        // Register your test schema
        ProtobufSchema schema = new ProtobufSchema(userMsg.getDescriptorForType());
        String subject = topic + "-value";
        schemaRegistry.register(subject, schema);

        ProtobufConverter protobufConverter = new ProtobufConverter(schemaRegistry);
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put(ProtobufConverterConfig.AUTO_REGISTER_SCHEMAS, true);
        converterConfig.put(ProtobufDataConfig.GENERATE_INDEX_FOR_UNIONS_CONFIG, false);
        converterConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test-url");

        protobufConverter.configure(converterConfig, false);
        KafkaProtobufSerializer protobufSerializer = new KafkaProtobufSerializer(schemaRegistry);
        byte[] serializedUserMsg=  protobufSerializer.serialize(topic, userMsg);
        SchemaAndValue userConnectData = protobufConverter.toConnectData(topic, serializedUserMsg);
        byte[] serializedProdudctMsg=  protobufSerializer.serialize(topic, productMsg);
        SchemaAndValue productConnectData = protobufConverter.toConnectData(topic, serializedProdudctMsg);

        List<SinkRecord> records = Arrays.asList(
                new SinkRecord(topic, 0, null, null,
                        userConnectData.schema(), userConnectData.value(), 0),
                new SinkRecord(topic, 0, null, null,
                        productConnectData.schema(), productConnectData.value(), 1)
        );

        Map<String, Serializable> clientSettings = new HashMap<>();
        clientSettings.put(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");

        new CreateTableStatement()
                .tableName(topic)
                .column("id", "Int32")
                .column("name", "String")
                .column("is_active", "Boolean")
                .column("score", "Float64")
                .column("tags", "Array(String)")
                .column("content", "JSON")
                .orderByColumn("()")
                .settings(clientSettings)
                .deploymentType(deploymentType)
                .execute(chc);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(records);
        chst.stop();
        assertEquals(records.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));

        // Verify the actual row data matches the proto JSON conversion by comparing JSON strings
        List<JSONObject> insertedRows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType);
        assertEquals(records.size(), insertedRows.size());

        // Compare database rows directly with protobuf message fields
        JSONObject firstRow = insertedRows.get(0);
        JSONObject secondRow = insertedRows.get(1);

        // Verify first row (user message) top-level fields match protobuf
        assertEquals(userMsg.getId(), firstRow.getInt("id"));
        assertEquals(userMsg.getName(), firstRow.getString("name"));
        assertEquals(userMsg.getIsActive(), firstRow.getBoolean("is_active"));
        assertEquals(userMsg.getScore(), firstRow.getDouble("score"), 0.01);

        // Verify tags array matches protobuf
        JSONArray userTagsArray = firstRow.getJSONArray("tags");
        assertEquals(userMsg.getTagsCount(), userTagsArray.length());
        for (int i = 0; i < userMsg.getTagsCount(); i++) {
            assertEquals(userMsg.getTags(i), userTagsArray.getString(i));
        }

        // Verify the content column contains the nested user_info data
        JSONObject firstRowContent = firstRow.getJSONObject("content");
        assertTrue(firstRowContent.has("user_info"), "Content should contain user_info");
        JSONObject userInfo = firstRowContent.getJSONObject("user_info");
        assertEquals(userMsg.getUserInfo().getEmail(), userInfo.getString("email"));
        assertEquals(userMsg.getUserInfo().getUserType().name(), userInfo.getString("user_type"));
        // Handle age type conversion (ClickHouse stores as string, protobuf as int)
        assertEquals(userMsg.getUserInfo().getAge(), userInfo.getNumber("age").intValue());
        // Verify address fields match protobuf
        JSONObject address = userInfo.getJSONObject("address");
        assertEquals(userMsg.getUserInfo().getAddress().getStreet(), address.getString("street"));
        assertEquals(userMsg.getUserInfo().getAddress().getCity(), address.getString("city"));
        assertEquals(userMsg.getUserInfo().getAddress().getState(), address.getString("state"));
        assertEquals(userMsg.getUserInfo().getAddress().getZip(), address.getString("zip"));
        // Verify nested address
        JSONObject altAddress = address.getJSONObject("alt");
        assertEquals(userMsg.getUserInfo().getAddress().getAlt().getStreet(), altAddress.getString("street"));
        assertEquals(userMsg.getUserInfo().getAddress().getAlt().getCity(), altAddress.getString("city"));
        assertEquals(userMsg.getUserInfo().getAddress().getAlt().getState(), altAddress.getString("state"));
        assertEquals(userMsg.getUserInfo().getAddress().getAlt().getZip(), altAddress.getString("zip"));

        // Verify second row (product message) top-level fields match protobuf
        assertEquals(productMsg.getId(), secondRow.getInt("id"));
        assertEquals(productMsg.getName(), secondRow.getString("name"));
        assertEquals(productMsg.getIsActive(), secondRow.getBoolean("is_active"));
        assertEquals(productMsg.getScore(), secondRow.getDouble("score"), 0.01);

        // Verify product tags array matches protobuf
        JSONArray productTagsArray = secondRow.getJSONArray("tags");
        assertEquals(productMsg.getTagsCount(), productTagsArray.length());
        for (int i = 0; i < productMsg.getTagsCount(); i++) {
            assertEquals(productMsg.getTags(i), productTagsArray.getString(i));
        }

        // Verify the content column contains the nested product_info data
        JSONObject secondRowContent = secondRow.getJSONObject("content");
        assertTrue(secondRowContent.has("product_info"), "Content should contain product_info");
        JSONObject productInfo = secondRowContent.getJSONObject("product_info");
        assertEquals(productMsg.getProductInfo().getSku(), productInfo.getString("sku"));
        assertEquals(productMsg.getProductInfo().getPrice(), productInfo.getDouble("price"), 0.01);
        assertEquals(productMsg.getProductInfo().getInStock(), productInfo.getBoolean("in_stock"));

        // Verify categories array in product_info matches protobuf
        JSONArray categoriesArray = productInfo.getJSONArray("categories");
        assertEquals(productMsg.getProductInfo().getCategoriesCount(), categoriesArray.length());
        for (int i = 0; i < productMsg.getProductInfo().getCategoriesCount(); i++) {
            assertEquals(productMsg.getProductInfo().getCategories(i), categoriesArray.getString(i));
        }
    }

    public List<Collection<SinkRecord>> partition(Collection<SinkRecord> collection, int size) {

        List<Collection<SinkRecord>> partitions = new ArrayList<>();
        Iterator<SinkRecord> iterator = collection.iterator();

        while (iterator.hasNext()) {
            Collection<SinkRecord> partition = new ArrayList<>();
            for (int i = 0; i < size && iterator.hasNext(); i++) {
                partition.add(iterator.next());
            }
            partitions.add(partition);
        }
        return partitions;
    }

    private Stream<Arguments> exactlyOnceStateMismatchTestArgs() {
        List<int[]> splitsAndBatches = List.of(
                new int[]{11, 7},
                new int[]{17, 11},
                new int[]{37, 17},
                new int[]{61, 37},
                new int[]{113, 120},
                new int[]{131, 150},
                new int[]{150, 160},
                new int[]{157, 131},
                new int[]{167, 161},
                new int[]{229, 220},
                new int[]{229, 221}
        );
        Stream<Arguments> argStream = Stream.of();
        for (var config : deploymentTypesForTests().collect(Collectors.toSet())) {
            argStream = Stream.concat(argStream, splitsAndBatches.stream().map(splitAndBatch -> Arguments.of(splitAndBatch[0], splitAndBatch[1], config)));
        }
        return argStream;
    }

    @ParameterizedTest
    @MethodSource("exactlyOnceStateMismatchTestArgs")
    public void exactlyOnceStateMismatchTest(int split, int batch, ClickHouseDeploymentType deploymentType) {
        Assumptions.assumeFalse(deploymentType.equals(ClickHouseDeploymentType.STANDALONE)); // skip test if running in standalone mode

        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = "exactly_once_state_mismatch_test_" + split + "_" + batch + "_" + System.currentTimeMillis();
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement(ARRAY_TYPES_TABLE)
                .tableName(topic)
                .deploymentType(deploymentType)
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createArrayType(topic, 1);
        List<Collection<SinkRecord>> data = partition(sr, split);
        data = data.subList(0, data.size() - 2);
        List<Collection<SinkRecord>> data01 = partition(sr, batch);
        // dropping first batch since connect might think someone restarted the topic and offset is set to zero
        data01.remove(0);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        props.put(ClickHouseSinkConfig.TOLERATE_STATE_MISMATCH, "true");
        props.put(ClickHouseSinkConfig.EXACTLY_ONCE, "true");
        chst.start(props);
        for (Collection<SinkRecord> records : data) {
            chst.put(records);
        }
        assertEquals(data.size() * split, ClickHouseTestHelpers.countRows(chc, topic, deploymentType));
        for (Collection<SinkRecord> records : data01) {
            chst.put(records);
        }
        chst.stop();
        // after the second insert we have exactly sr.size() records
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, deploymentType));assertTrue(ClickHouseTestHelpers.validateRows(chc, topic, sr, deploymentType));
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void testAvroWithUnion(ClickHouseDeploymentType deploymentType) throws Exception {
        Image image1 = Image.newBuilder()
                .setName("image1")
                .setContent("content")
                .build();
        Image image2 = Image.newBuilder()
                .setName("image2")
                .setContent(ByteBuffer.wrap("content2".getBytes()))
                .setDescription("description")
                .build();
        String topic = createTopicName("test_avro_union_string");

        List<SinkRecord> records = SchemaTestData.convertAvroToSinkRecord(topic, new AvroSchema(Image.getClassSchema()), Arrays.asList(image1, image2));
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("name", "String")
                .column("content", "String")
                .column("description", "Nullable(String)")
                .orderByColumn("()")
                .deploymentType(deploymentType)
                .execute(chc);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(records);
        chst.stop();

        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType);
        if (rows.size() == 0) {
            rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType);
            LOGGER.info("Second attempt read: {}", rows.size());
            if (rows.size() == 0) {
                rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType);
                LOGGER.info("Third attempt read: {}", rows.size());
            }
        }
        JSONObject row = rows.get(0);
        assertEquals("image1", row.getString("name"));
        assertEquals("content", row.getString("content"));
        assertTrue(row.isNull("description"));
        row = rows.get(1);
        assertEquals("image2", row.getString("name"));
        assertEquals("description", row.getString("description"));
        assertEquals("content2", row.getString("content"));
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void testAvroDateAndTimeTypes(ClickHouseDeploymentType deploymentType) throws Exception {

        final String topic = createTopicName("test_avro_timestamps");
        final ZoneId tz = ZoneId.of("UTC");
        final Instant now = Instant.now();

        List<Object> events = IntStream.range(0, 3).mapToObj(i -> {
            Instant time = now.plus(i * 1000, ChronoUnit.MILLIS);
            Event event = Event.newBuilder()
                    .setId(i)
                    .setTime1(time)
                    .setTime2(LocalTime.ofInstant(time, tz))
                    .build();
            return event;
        }).collect(Collectors.toList());

        List<SinkRecord> records = SchemaTestData.convertAvroToSinkRecord(topic, new AvroSchema(Event.getClassSchema()), events);

        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .column("id", "Int64")
                .column("time1", "DateTime64(3)")
                .column("time2", "DateTime64(3)")
                .orderByColumn("()")
                .deploymentType(deploymentType)
                .execute(chc);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(records);
        chst.stop();


        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType);
        assertEquals(events.size(), rows.size());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(tz);
        DateTimeFormatter localFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        for (int i = 0; i < events.size(); i++) {
            Event event = (Event) events.get(i);
            JSONObject row = rows.get(i);
            assertEquals(event.getId(), row.getLong("id"));
            assertEquals(formatter.format(event.getTime1()), row.get("time1"));
            assertEquals(event.getTime2().atDate(LocalDate.of(1970, 1, 1)).format(localFormatter), row.get("time2"));
        }
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
    }
}
