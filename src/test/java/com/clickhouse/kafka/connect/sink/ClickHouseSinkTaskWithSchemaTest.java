package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.query.Records;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
            .engine("MergeTree")
            .orderByColumn("off16");

    private static final CreateTableStatement MAP_TYPES_TABLE = new CreateTableStatement()
            .column("off16", "Int16")
            .column("map_string_string", "Map(String, String)")
            .column("map_string_int64", "Map(String, Int64)")
            .column("map_int64_string", "Map(Int64, String)")
            .column("map_string_map", "Map(String, Map(String, Int64))")
            .column("map_string_array", "Map(String, Array(String))")
            .column("map_map_map", "Map(String, Map(String, Map(String, String)))")
            .engine("MergeTree")
            .orderByColumn("off16");

    private static final CreateTableStatement CHANGE_SCHEMA_TABLE = new CreateTableStatement()
            .column("off16", "Int16")
            .column("string", "String")
            .engine("MergeTree")
            .orderByColumn("`off16`");

    @Test
    public void arrayTypesTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "array_string_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(ARRAY_TYPES_TABLE)
                .tableName(topic)
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        assertTrue(ClickHouseTestHelpers.validateRows(chc, topic, sr));
    }

    @Test
    public void arrayNullableSubtypesTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "array_nullable_subtypes_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
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
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createArrayNullableSubtypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void mapTypesTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "map_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(MAP_TYPES_TABLE)
                .tableName(topic)
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void nullArrayTypeTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("nullable_array_string_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("arr", "Array(String)")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createNullableArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void nullableArrayTypeTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("nullable_array_string_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("arr", "Array(Nullable(String))")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createNullableArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/33
    public void materializedViewsBug() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("m_array_string_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic + "mate");
        ClickHouseTestHelpers.dropTable(chc, topic);
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
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        new CreateTableStatement()
                .tableName(topic + "_mate")
                .column("off16", "Int16")
                .engine("Null")
                .execute(chc);
        ClickHouseTestHelpers.runQuery(chc, String.format("CREATE MATERIALIZED VIEW %s_mv TO " + topic + "_mate AS SELECT off16 FROM " + topic, topic));
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("special-char-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(MAP_TYPES_TABLE)
                .tableName(topic)
                .execute(chc);
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("null-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("null_value_data", "Nullable(DateTime64(6, 'UTC'))")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("support-dates-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
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
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createDateType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));

        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
        for (JSONObject row : rows) {
            String dateTime64_9 = row.getString("datetime64_9_number");
            String dateTime64_6 = row.getString("datetime64_6_number");
            String dateTime64_3 = row.getString("datetime64_3_number");

            assertTrue(dateTime64_9.contains(dateTime64_3), dateTime64_3 + " is not a substring of " + dateTime64_9);
            assertTrue(dateTime64_9.contains(dateTime64_6), dateTime64_6 + " is not a substring of " + dateTime64_9);
        }
    }

    @Test
    public void supportArrayDateTime64Test() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("support-array-datetime64-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("arr_datetime64_number", "Array(DateTime64)")
                .column("arr_timestamp_date", "Array(DateTime64)")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createArrayDateTime64Type(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void detectUnsupportedDataConversions() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("support-unsupported-dates-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("date_number", "Date")
                .column("date32_number", "Date32")
                .column("datetime_number", "DateTime")
                .column("datetime64_number", "DateTime64")
                .engine("MergeTree")
                .orderByColumn("off16")
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


    @Test
    public void supportZonedDatesStringTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("support-dates-string-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("zoned_date", "DateTime64")
                .column("offset_date", "DateTime64")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createZonedTimestampConversions(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }


    @Test
    public void supportFormattedDatesStringTest() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.DATE_TIME_FORMAT, "format_date=yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("support-formatted-dates-string-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("format_date", "DateTime64(9)")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createFormattedTimestampConversions(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        List<JSONObject> results = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
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



    @Test
    public void withEmptyDataRecordsTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schema_empty_records_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("p_int64", "Int64")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createWithEmptyDataRecords(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void withLowCardinalityTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schema_empty_records_lc_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("p_int64", "Int64")
                .column("lc_string", "LowCardinality(String)")
                .column("nullable_lc_string", "LowCardinality(Nullable(String))")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createWithLowCardinality(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void withUUIDTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schema_empty_records_lc_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("uuid", "UUID")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createWithUUID(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void schemaWithDefaultsTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("default-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("default_value_data", "DateTime DEFAULT now()")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createNullValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }


    @Test
    public void schemaWithEphemeralTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("default-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("default_value_data", "DateTime DEFAULT now()")
                .column("ephemeral_data", "String EPHEMERAL")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createNullValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }


    @Test
    public void schemaWithDefaultsAndNullableTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("default-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("null_value_data", "Nullable(DateTime)")
                .column("default_value_data", "DateTime DEFAULT now()")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("decimal-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("decimal_14_2", "Decimal(14, 2)")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createDecimalValueData(topic, 1);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        assertEquals(499700, ClickHouseTestHelpers.sumRows(chc, topic, "decimal_14_2"));
    }

    @Test
    public void schemaWithFixedStringTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("fixed-string-value-table-test");
        int fixedStringSize = RandomUtils.insecure().randomInt(1, 100);
        LOGGER.info("FixedString size: " + fixedStringSize);
        ClickHouseTestHelpers.dropTable(chc, topic);
        final int fss = fixedStringSize;
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("fixed_string_string", "FixedString(" + fss + ")")
                .column("fixed_string_bytes", "FixedString(" + fss + ")")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createFixedStringData(topic, 1, fixedStringSize);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void writeBooleanValueToIntTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schema-with-boolean-and-int-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
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
                .engine("MergeTree")
                .orderByColumn("off16")
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

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        List<JSONObject> rows =ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
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

    @Test
    public void schemaWithFixedStringMismatchTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("fixed-string-mismatch-table-test");
        int fixedStringSize = RandomUtils.insecure().randomInt(2, 100);
        LOGGER.info("FixedString size: " + fixedStringSize);
        ClickHouseTestHelpers.dropTable(chc, topic);
        final int fss = fixedStringSize;
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("fixed_string_string", "FixedString(" + (fss - 1) + ")")
                .engine("MergeTree")
                .orderByColumn("off16")
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

    @Test
    public void schemaWithNullableDecimalTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("nullable-decimal-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("decimal_14_2", "Nullable(Decimal(14, 2))")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createDecimalValueDataWithNulls(topic, 1);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        assertEquals(450180, ClickHouseTestHelpers.sumRows(chc, topic, "decimal_14_2"));
    }

    @Test
    public void schemaWithBytesTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("bytes-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("string", "String")
                .engine("MergeTree")
                .orderByColumn("`string`")
                .execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createBytesValueData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }
    @Test
    public void supportEnumTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("enum-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("enum8_type", "Enum8('A' = 1, 'B' = 2, 'C' = 3)")
                .column("enum16_type", "Enum16('A' = 1, 'B' = 2, 'C' = 3, 'D' = 4)")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createEnumValueData(topic, 1);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Disabled("Disabled because it requires a flag on the instance.")
    @Test
    @SinceClickHouseVersion("24.1")
    public void schemaWithTupleOfMapsWithVariantTest() {
        Assumptions.assumeFalse(isCloud, "Skip test since experimental is not available in cloud");
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = "tuple-array-map-variant-table-test";
        ClickHouseTestHelpers.dropTable(chc, topic);

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
                .engine("MergeTree")
                .orderByColumn("`off16`")
                .settings(Map.of("allow_experimental_variant_type", 1))
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createTupleType(topic, 1, 5);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));

        List<JSONObject> allRows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
        for (int i = 0; i < sr.size(); i++) {
            JSONObject row = allRows.get(i);

            assertEquals(i, row.getInt("off16"));
            JSONObject tuple = row.getJSONObject("tuple");
            JSONObject nestedTuple = tuple.getJSONObject("tuple");
            assertEquals(1 / (double) 3, nestedTuple.getDouble("variant_with_double"));
        }
    }

    @Disabled("Disabled because it requires a flag on the instance.")
    @Test
    @SinceClickHouseVersion("24.1")
    public void schemaWithNestedTupleMapArrayAndVariant() {
        if (isCloud) {
            LOGGER.warn("Skip test since experimental is not available in cloud");
            return;
        }
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = "nested-tuple-map-array-and-variant-table-test";
        ClickHouseTestHelpers.dropTable(chc, topic);

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
                .engine("MergeTree")
                .orderByColumn("`off16`")
                .settings(Map.of("allow_experimental_variant_type", 1))
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createNestedType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));

        List<JSONObject> allRows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
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

    @Test
    public void unsignedIntegers() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("unsigned-integers-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("uint8", "UInt8")
                .column("uint16", "UInt16")
                .column("uint32", "UInt32")
                .column("uint64", "UInt64")
                .engine("MergeTree")
                .orderByColumn("`off16`")
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createUnsignedIntegers(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void changeSchemaWhileRunning() throws InterruptedException {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("change-schema-while-running-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(CHANGE_SCHEMA_TABLE)
                .tableName(topic)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createSimpleData(topic, 1);

        int numRecords = sr.size();
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));


        ClickHouseTestHelpers.runQuery(chc, String.format("ALTER TABLE `%s` ADD COLUMN num32 Nullable(Int32) AFTER string", topic));
        Thread.sleep(5000);
        sr = SchemaTestData.createSimpleExtendWithNullableData(topic, 1, 10000, 2000);
        int numRecordsWithNullable = sr.size();
        chst.put(sr);

        ClickHouseTestHelpers.runQuery(chc, String.format("ALTER TABLE `%s` ADD COLUMN num32_default Int32 DEFAULT 0 AFTER num32", topic));
        Thread.sleep(5000);

        sr = SchemaTestData.createSimpleExtendWithDefaultData(topic, 1, 20000, 3000);
        int numRecordsWithDefault = sr.size();
        System.out.println("numRecordsWithDefault: " + numRecordsWithDefault);
        chst.put(sr);
        ClickHouseTestHelpers.getAllRowsAsJson(chc, topic).forEach(row -> {
            //System.out.println(row);
        });
        chst.stop();
        assertEquals(numRecords + numRecordsWithNullable + numRecordsWithDefault, ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void changeSchemaWhileRunningAddDefaultColumnOldSchemaData() throws InterruptedException {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("change-schema-add-default-old-schema-data-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(CHANGE_SCHEMA_TABLE)
                .tableName(topic)
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
        assertEquals(firstBatch.size(), ClickHouseTestHelpers.countRows(chc, topic));

        ClickHouseTestHelpers.runQuery(chc, String.format("ALTER TABLE `%s` ADD COLUMN num32_default Int32 DEFAULT 42 AFTER string", topic));
        Thread.sleep(5000);

        // Keep writing records with the old schema (without num32_default).
        // Connector should write default markers for the new column.
        chst.put(secondBatch);
        chst.stop();

        assertEquals(firstBatch.size() + secondBatch.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void changeSchemaWhileRunningWithRefreshEnabled() throws InterruptedException {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        props.put(ClickHouseSinkConfig.TABLE_REFRESH_INTERVAL, "1");
        String topic = createTopicName("change-schema-while-running-table-test-with-refresh-enabled");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(CHANGE_SCHEMA_TABLE)
                .tableName(topic)
                .execute(chc);
        Collection<SinkRecord> sr = SchemaTestData.createSimpleData(topic, 1);

        int numRecords = sr.size();
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));


        ClickHouseTestHelpers.runQuery(chc, String.format("ALTER TABLE `%s` ADD COLUMN num32 Nullable(Int32) AFTER string", topic));
        Thread.sleep(5000);
        sr = SchemaTestData.createSimpleExtendWithNullableData(topic, 1, 10000, 2000);
        int numRecordsWithNullable = sr.size();
        chst.put(sr);

        ClickHouseTestHelpers.runQuery(chc, String.format("ALTER TABLE `%s` ADD COLUMN num32_default Int32 DEFAULT 0 AFTER num32", topic));
        Thread.sleep(5000);

        sr = SchemaTestData.createSimpleExtendWithDefaultData(topic, 1, 20000, 3000);
        int numRecordsWithDefault = sr.size();
        System.out.println("numRecordsWithDefault: " + numRecordsWithDefault);
        chst.put(sr);
        ClickHouseTestHelpers.getAllRowsAsJson(chc, topic).forEach(row -> {
            //System.out.println(row);
        });
        chst.stop();
        assertEquals(numRecords + numRecordsWithNullable + numRecordsWithDefault, ClickHouseTestHelpers.countRows(chc, topic));
    }
    @Test
    public void tupleTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("tuple-table-test");

        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("string", "String")
                .column("t", "Tuple(`off16` Nullable(Int16), `string` Nullable(String))")
                .engine("MergeTree")
                .orderByColumn("`off16`")
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createTupleSimpleData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }
    @Test
    public void tupleTestWithDefault() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("tuple-table-test-default");

        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("string", "String")
                .column("insert_datetime", "DateTime default now()")
                .column("t", "Tuple(`off16` Nullable(Int16), `string` Nullable(String))")
                .engine("MergeTree")
                .orderByColumn("`off16`")
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createTupleSimpleData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void nestedTupleTestWithDefault() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("nested-tuple-table-test-default");

        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("string", "String")
                .column("insert_datetime", "DateTime default now()")
                .column("t", "Tuple(" +
                        "`off16` Nullable(Int16), " +
                        "`string` Nullable(String), " +
                        "`n` Tuple(`off16` Nullable(Int16), `string` Nullable(String)))")
                .engine("MergeTree")
                .orderByColumn("`off16`")
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createNestedTupleSimpleData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    /**
     * Validates that Jackson Struct serialization produces correct data in
     * ClickHouse for flat, nested, and deeply nested Structs via the JSON
     * insert path (JSONEachRow).  Covers the Gson→Jackson migration by
     * verifying actual inserted values, not just row counts.
     */
    @Test
    public void jacksonStructJsonInsertTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("jackson-struct-json-insert");

        ClickHouseTestHelpers.dropTable(chc, topic);
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
                .engine("MergeTree")
                .orderByColumn("`off16`")
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

        assertEquals(totalRecords, ClickHouseTestHelpers.countRows(chc, topic));

        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
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
    @Test
    public void jacksonStructWithNullsJsonInsertTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("jackson-struct-nulls-json-insert");

        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("name", "Nullable(String)")
                .column("t", "Tuple(`off16` Nullable(Int16), `label` Nullable(String))")
                .engine("MergeTree")
                .orderByColumn("`off16`")
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

        assertEquals(2, ClickHouseTestHelpers.countRows(chc, topic));

        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
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

    @Test
    public void coolSchemaWithRandomFields() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("cool-schema-with-random-field");

        ClickHouseTestHelpers.dropTable(chc, topic);
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
                .engine("MergeTree")
                .orderByColumn("`processing_time`")
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createCoolSchemaWithRandomFields(topic, 1);


        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    @SinceClickHouseVersion("24.10")
    public void testWritingJsonAsStringWithRowBinary() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        props.put(ClickHouseSinkConfig.CLICKHOUSE_SETTINGS, "input_format_binary_read_json_as_string=1");
        String topic = createTopicName("schema_json_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);

        Map<String, Serializable> clientSettings = new HashMap<>();
        clientSettings.put(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("struct_content", "JSON")
                .column("json_as_str", "JSON")
                .engine("MergeTree")
                .orderByColumn("off16")
                .settings(clientSettings)
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createJSONType(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    @SinceClickHouseVersion("24.10")
    public void testWritingProtoMessageWithRowBinary() throws Exception {

        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        props.put(ClickHouseSinkConfig.CLICKHOUSE_SETTINGS, "input_format_binary_read_json_as_string=1");
        String topic = createTopicName("protobuf_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);

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
                .engine("MergeTree")
                .orderByColumn("()")
                .settings(clientSettings)
                .execute(chc);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(records);
        chst.stop();
        assertEquals(records.size(), ClickHouseTestHelpers.countRows(chc, topic));

        // Verify the actual row data matches the proto JSON conversion by comparing JSON strings
        List<JSONObject> insertedRows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
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

    @ParameterizedTest
    //@ValueSource(ints = {11, 17 , 37,  61, 113, 131, 150, 157, 167, 229})
    @CsvSource({
            "11, 7",
            "17, 11",
            "37, 17",
            "61, 37",
            "113, 120",
            "131, 150",
            "150, 160",
            "157, 131",
            "167, 161",
            "229, 220",
            "229, 221",
    })
    public void exactlyOnceStateMismatchTest(int split, int batch) {
        // This test is running only cloud
        if (!isCloud)
            return;
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "exactly_once_state_mismatch_test_" + split + "_" + batch + "_" + System.currentTimeMillis();
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(ARRAY_TYPES_TABLE)
                .tableName(topic)
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
        assertEquals(data.size() * split, ClickHouseTestHelpers.countRows(chc, topic));
        for (Collection<SinkRecord> records : data01) {
            chst.put(records);
        }
        chst.stop();
        // after the second insert we have exactly sr.size() records
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));assertTrue(ClickHouseTestHelpers.validateRows(chc, topic, sr));

    }

    @Test
    public void testAvroWithUnion() throws Exception {
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("name", "String")
                .column("content", "String")
                .column("description", "Nullable(String)")
                .engine("MergeTree")
                .orderByColumn("()")
                .execute(chc);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(records);
        chst.stop();

        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
        if (rows.size() == 0) {
            rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
            LOGGER.info("Second attempt read: {}", rows.size());
            if (rows.size() == 0) {
                rows = ClickHouseTestHelpers.getAllRowsAsJsonCloud(chc, topic);
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
    }

    @Test
    public void testAvroDateAndTimeTypes() throws Exception {

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

        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("id", "Int64")
                .column("time1", "DateTime64(3)")
                .column("time2", "DateTime64(3)")
                .engine("MergeTree")
                .orderByColumn("()")
                .execute(chc);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(records);
        chst.stop();


        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
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
    }

    @Test
    public void autoEvolveDisabledRejectsNewField() {
        Map<String, String> props = createProps();
        // auto.evolve defaults to false
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_disabled_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Insert V1 records first (should succeed)
        Collection<SinkRecord> srV1 = SchemaTestData.createSchemaV1(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(srV1);
        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Insert V2 records with new field (should succeed because input_format_skip_unknown_fields=1)
        // But the new column should NOT be added to the table
        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithNewNullableField(topic, 1, 10);
        chst.put(srV2);
        chst.stop();

        // Rows inserted but new column should not exist
        assertEquals(20, ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void autoEvolveAddsNullableColumn() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_nullable_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Insert V1 records
        Collection<SinkRecord> srV1 = SchemaTestData.createSchemaV1(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(srV1);
        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Insert V2 records with new nullable field -> should trigger ALTER TABLE ADD COLUMN
        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithNewNullableField(topic, 1, 10);
        chst.put(srV2);
        chst.stop();

        assertEquals(20, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify the new column exists
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("new_string_field"),
                "New column 'new_string_field' should have been added by auto.evolve");
    }

    @Test
    public void autoEvolveAddsNonNullableFieldAsNullable() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_non_nullable_as_nullable_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithNewNonNullableField(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(srV2);
        chst.stop();

        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Non-nullable fields are created as Nullable columns - mandatory fields always have a value,
        // and Nullable allows old records (without this field) to insert with NULL
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("non_nullable_field"),
                "New column 'non_nullable_field' should have been added by auto.evolve");
    }

    @Test
    public void autoEvolveMultipleNewColumns() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_multi_cols_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Insert V1 first
        Collection<SinkRecord> srV1 = SchemaTestData.createSchemaV1(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(srV1);
        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Insert V2 with multiple new nullable fields
        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithMultipleNewNullableFields(topic, 1, 10);
        chst.put(srV2);
        chst.stop();

        assertEquals(20, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify all new columns exist
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("new_string_field"),
                "Column 'new_string_field' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("new_int32_field"),
                "Column 'new_int32_field' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("new_float64_field"),
                "Column 'new_float64_field' should exist");
    }

    @Test
    public void autoEvolveCachesSchemaAfterDDL() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_cache_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);

        // First batch triggers DDL
        Collection<SinkRecord> srV2batch1 = SchemaTestData.createSchemaV2WithNewNullableField(topic, 1, 10);
        chst.put(srV2batch1);
        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Second batch with same schema should not re-trigger DDL (just insert)
        // Use partition 2 to avoid offset deduplication
        Collection<SinkRecord> srV2batch2 = SchemaTestData.createSchemaV2WithNewNullableField(topic, 2, 10);
        chst.put(srV2batch2);
        chst.stop();

        assertEquals(20, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify column exists (only one 'new_string_field' column)
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        long count = described.getRootColumnsList().stream()
                .filter(c -> c.getName().equals("new_string_field"))
                .count();
        assertEquals(1, count, "Should have exactly one 'new_string_field' column");
    }

    @Test
    public void autoEvolveMixedSchemaInSingleBatch() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_mixed_batch_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Build a single batch with V1 records followed by V2 records (mixed schemas)
        List<SinkRecord> mixedBatch = new ArrayList<>();
        mixedBatch.addAll(SchemaTestData.createSchemaV1(topic, 1, 5));
        mixedBatch.addAll(SchemaTestData.createSchemaV2WithNewNullableField(topic, 1, 5));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(mixedBatch);
        chst.stop();

        // All 10 records should be inserted
        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // The new column should exist (evolved from V2 records in same batch)
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("new_string_field"),
                "Column 'new_string_field' should be added even when schema changes mid-batch");
    }

    @Test
    public void autoEvolveMixedSchemaOlderRecordsGetNull() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_older_records_null_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // V1 records (no new_string_field) followed by V2 records (has new_string_field)
        // Schema is evolved using last record (V2), then entire batch is inserted.
        // V1 records should get NULL for the new column.
        List<SinkRecord> mixedBatch = new ArrayList<>();
        mixedBatch.addAll(SchemaTestData.createSchemaV1(topic, 1, 5));
        mixedBatch.addAll(SchemaTestData.createSchemaV2WithNewNullableField(topic, 1, 5));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(mixedBatch);
        chst.stop();

        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // V1 records should have NULL for the new column
        String nullCountQuery = String.format(
                "SELECT COUNT(*) FROM `%s` WHERE `new_string_field` IS NULL SETTINGS select_sequential_consistency = 1", topic);
        try {
            Records nullRecords = chc.getClient().queryRecords(nullCountQuery).get();
            int nullCount = Integer.parseInt(nullRecords.iterator().next().getString(1));
            assertEquals(5, nullCount, "V1 records should have NULL for new_string_field");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // V2 records should have non-NULL values
        String nonNullQuery = String.format(
                "SELECT COUNT(*) FROM `%s` WHERE `new_string_field` IS NOT NULL SETTINGS select_sequential_consistency = 1", topic);
        try {
            Records nonNullRecords = chc.getClient().queryRecords(nonNullQuery).get();
            int nonNullCount = Integer.parseInt(nonNullRecords.iterator().next().getString(1));
            assertEquals(5, nonNullCount, "V2 records should have non-NULL values for new_string_field");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void autoEvolveLogicalTypes() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_logical_types_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Insert V1 first
        Collection<SinkRecord> srV1 = SchemaTestData.createSchemaV1(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(srV1);
        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Insert V2 with logical type fields (Decimal, Date, Timestamp)
        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithLogicalTypes(topic, 1, 10);
        chst.put(srV2);
        chst.stop();

        assertEquals(20, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify the logical type columns were created with correct ClickHouse types
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("new_decimal_field"),
                "Column 'new_decimal_field' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("new_date_field"),
                "Column 'new_date_field' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("new_timestamp_field"),
                "Column 'new_timestamp_field' should exist");

        // Verify types
        com.clickhouse.kafka.connect.sink.db.mapping.Column decimalCol = described.getRootColumnsMap().get("new_decimal_field");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.Decimal, decimalCol.getType(),
                "Decimal logical type should map to ClickHouse Decimal");

        com.clickhouse.kafka.connect.sink.db.mapping.Column dateCol = described.getRootColumnsMap().get("new_date_field");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.Date32, dateCol.getType(),
                "Date logical type should map to ClickHouse Date32");

        com.clickhouse.kafka.connect.sink.db.mapping.Column tsCol = described.getRootColumnsMap().get("new_timestamp_field");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.DateTime64, tsCol.getType(),
                "Timestamp logical type should map to ClickHouse DateTime64");
    }

    @Test
    public void autoEvolveRejectsStructField() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_struct_reject_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithStructField(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);

        try {
            chst.put(srV2);
            assertTrue(false, "Expected exception for STRUCT field auto-evolution");
        } catch (RuntimeException e) {
            Throwable t = e;
            boolean found = false;
            while (t != null) {
                if (t.getMessage() != null && t.getMessage().contains("Cannot auto-evolve STRUCT")) {
                    found = true;
                    break;
                }
                t = t.getCause();
            }
            assertTrue(found, "Should reject STRUCT field with appropriate message, got: " + e.getMessage());
        } finally {
            chst.stop();
        }
    }

    // STRUCT field auto-evolved as JSON column when auto.evolve.struct.to.json=true
    @Test
    public void autoEvolveStructToJsonCreatesJsonColumn() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE_STRUCT_TO_JSON, "true");
        props.put(ClickHouseSinkConfig.CLICKHOUSE_SETTINGS, "input_format_binary_read_json_as_string=1");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_struct_to_json_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithStructField(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(srV2);
        chst.stop();

        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify the new column was created as JSON type
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("new_struct_field"),
                "Column 'new_struct_field' should exist");
        com.clickhouse.kafka.connect.sink.db.mapping.Column col = described.getRootColumnsMap().get("new_struct_field");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.JSON, col.getType(),
                "Column 'new_struct_field' should be JSON type");
    }

    // V1 records (no struct) inserted first, then V2 records (with struct) trigger JSON column creation.
    @Test
    public void autoEvolveStructToJsonMixedBatchOlderRecordsGetDefault() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE_STRUCT_TO_JSON, "true");
        props.put(ClickHouseSinkConfig.CLICKHOUSE_SETTINGS, "input_format_binary_read_json_as_string=1");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_struct_json_mixed_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Insert V1 records (no struct field)
        Collection<SinkRecord> srV1 = SchemaTestData.createSchemaV1(topic, 1, 5);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(srV1);
        assertEquals(5, ClickHouseTestHelpers.countRows(chc, topic));

        // Insert V2 records (with struct field) - triggers JSON column creation
        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithStructField(topic, 1, 5);
        chst.put(srV2);
        chst.stop();

        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify JSON column exists
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        com.clickhouse.kafka.connect.sink.db.mapping.Column col = described.getRootColumnsMap().get("new_struct_field");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.JSON, col.getType(),
                "Column 'new_struct_field' should be JSON type");
    }

    // STRUCT field with auto.evolve.struct.to.json explicitly false rejects with helpful error message
    @Test
    public void autoEvolveStructToJsonExplicitlyFalseRejectsStruct() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE_STRUCT_TO_JSON, "false");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_struct_json_false_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithStructField(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);

        try {
            chst.put(srV2);
            assertTrue(false, "Expected exception for STRUCT field when struct.to.json is false");
        } catch (RuntimeException e) {
            Throwable t = e;
            boolean found = false;
            while (t != null) {
                if (t.getMessage() != null && t.getMessage().contains("auto.evolve.struct.to.json=true")) {
                    found = true;
                    break;
                }
                t = t.getCause();
            }
            assertTrue(found, "Error message should suggest auto.evolve.struct.to.json=true, got: " + e.getMessage());
        } finally {
            chst.stop();
        }
    }

    @Test
    public void autoEvolveArrayAndMapFields() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_array_map_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Insert V1 first
        Collection<SinkRecord> srV1 = SchemaTestData.createSchemaV1(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(srV1);
        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Insert V2 with Array and Map fields
        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithArrayAndMapFields(topic, 1, 10);
        chst.put(srV2);
        chst.stop();

        assertEquals(20, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify array and map columns were created
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("new_array_field"),
                "Column 'new_array_field' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("new_map_field"),
                "Column 'new_map_field' should exist");
    }

    @Test
    public void autoEvolveTripleSchemaInOneBatch() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_triple_schema_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Build a single batch with V1 + V2 + V3 records
        List<SinkRecord> combined = new ArrayList<>();
        combined.addAll(SchemaTestData.createSchemaV1(topic, 1, 5));
        combined.addAll(SchemaTestData.createSchemaV2WithNewNullableField(topic, 1, 5));
        combined.addAll(SchemaTestData.createSchemaV3WithExtraField(topic, 1, 5));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(combined);
        chst.stop();

        assertEquals(15, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify columns from both V2 and V3 exist
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("new_string_field"),
                "V2 column 'new_string_field' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("v3_bool_field"),
                "V3 column 'v3_bool_field' should exist");
    }

    @Test
    public void autoEvolveThreeSeparateBatches() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_three_batches_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);

        // Batch 1: Schema V1 (3 fields: off16, p_int64, name)
        List<SinkRecord> batch1 = new ArrayList<>(SchemaTestData.createRichSchemaV1(topic, 1, 5, 0));
        chst.put(batch1);

        // Batch 2: Schema V2 (8 fields: off16, p_int64, name, email, age, score, active, city)
        List<SinkRecord> batch2 = new ArrayList<>(SchemaTestData.createRichSchemaV2(topic, 1, 5, 5));
        chst.put(batch2);

        // Batch 3: Schema V3 (5 fields: off16, p_int64, name, email, country)
        List<SinkRecord> batch3 = new ArrayList<>(SchemaTestData.createRichSchemaV3(topic, 1, 5, 10));
        chst.put(batch3);

        chst.stop();

        assertEquals(15, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify all evolved columns exist
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("name"), "V1 column 'name' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("email"), "V2 column 'email' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("age"), "V2 column 'age' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("score"), "V2 column 'score' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("active"), "V2 column 'active' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("city"), "V2 column 'city' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("country"), "V3 column 'country' should exist");

        // V1 records should have NULL for V2/V3 columns
        String nullEmailQuery = String.format(
                "SELECT COUNT(*) FROM `%s` WHERE `email` IS NULL SETTINGS select_sequential_consistency = 1", topic);
        // V3 records don't include age/score/active/city — those should be NULL
        String nullAgeQuery = String.format(
                "SELECT COUNT(*) FROM `%s` WHERE `age` IS NULL SETTINGS select_sequential_consistency = 1", topic);
        try {
            Records emailNulls = chc.getClient().queryRecords(nullEmailQuery).get();
            int emailNullCount = Integer.parseInt(emailNulls.iterator().next().getString(1));
            assertEquals(5, emailNullCount, "V1 records should have NULL for email");

            Records ageNulls = chc.getClient().queryRecords(nullAgeQuery).get();
            int ageNullCount = Integer.parseInt(ageNulls.iterator().next().getString(1));
            assertEquals(10, ageNullCount, "V1 + V3 records (10) should have NULL for age");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void autoEvolveMixedSchemasTenRecordsInOneBatch() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_mixed_ten_records_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Single batch with 10 records spanning 3 schema versions:
        //   Records 1–5:  Schema V1 (3 fields: off16, p_int64, name)
        //   Records 6–7:  Schema V2 (8 fields: off16, p_int64, name, email, age, score, active, city)
        //   Records 8–10: Schema V3 (5 fields: off16, p_int64, name, email, country)
        List<SinkRecord> batch = new ArrayList<>();
        batch.addAll(SchemaTestData.createRichSchemaV1(topic, 1, 5, 0));
        batch.addAll(SchemaTestData.createRichSchemaV2(topic, 1, 2, 5));
        batch.addAll(SchemaTestData.createRichSchemaV3(topic, 1, 3, 7));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(batch);
        chst.stop();

        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify all evolved columns from all versions exist
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("name"), "V1 column 'name' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("email"), "V2 column 'email' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("age"), "V2 column 'age' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("score"), "V2 column 'score' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("active"), "V2 column 'active' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("city"), "V2 column 'city' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("country"), "V3 column 'country' should exist");

        // Verify NULL distribution:
        //   name: all 10 records have it - 0 NULLs
        //   email: V1 records (5) lack it - 5 NULLs
        //   age: only V2 records (2) have it - 8 NULLs
        //   country: only V3 records (3) have it - 7 NULLs
        try {
            String nameNullQuery = String.format(
                    "SELECT COUNT(*) FROM `%s` WHERE `name` IS NULL SETTINGS select_sequential_consistency = 1", topic);
            Records nameNulls = chc.getClient().queryRecords(nameNullQuery).get();
            assertEquals(0, Integer.parseInt(nameNulls.iterator().next().getString(1)),
                    "All records have name, so 0 NULLs expected");

            String emailNullQuery = String.format(
                    "SELECT COUNT(*) FROM `%s` WHERE `email` IS NULL SETTINGS select_sequential_consistency = 1", topic);
            Records emailNulls = chc.getClient().queryRecords(emailNullQuery).get();
            assertEquals(5, Integer.parseInt(emailNulls.iterator().next().getString(1)),
                    "V1 records (5) should have NULL for email");

            String ageNullQuery = String.format(
                    "SELECT COUNT(*) FROM `%s` WHERE `age` IS NULL SETTINGS select_sequential_consistency = 1", topic);
            Records ageNulls = chc.getClient().queryRecords(ageNullQuery).get();
            assertEquals(8, Integer.parseInt(ageNulls.iterator().next().getString(1)),
                    "V1 (5) + V3 (3) records should have NULL for age");

            String countryNullQuery = String.format(
                    "SELECT COUNT(*) FROM `%s` WHERE `country` IS NULL SETTINGS select_sequential_consistency = 1", topic);
            Records countryNulls = chc.getClient().queryRecords(countryNullQuery).get();
            assertEquals(7, Integer.parseInt(countryNulls.iterator().next().getString(1)),
                    "V1 (5) + V2 (2) records should have NULL for country");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // auto-evolve adds columns for every supported primitive + logical type in a single batch
    @Test
    public void autoEvolveAllPrimitiveAndLogicalTypes() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_all_types_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Insert V1 first to ensure existing rows get NULL for new columns
        Collection<SinkRecord> srV1 = SchemaTestData.createSchemaV1(topic, 1, 5);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(srV1);
        assertEquals(5, ClickHouseTestHelpers.countRows(chc, topic));

        // Insert V2 with all primitive + logical type fields
        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithAllPrimitiveTypes(topic, 1, 5);
        chst.put(srV2);
        chst.stop();

        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify all columns were created with correct types
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        Map<String, com.clickhouse.kafka.connect.sink.db.mapping.Column> cols = described.getRootColumnsMap();

        // Primitive types
        assertTrue(cols.containsKey("new_int8"), "Column 'new_int8' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.INT8, cols.get("new_int8").getType());
        assertTrue(cols.containsKey("new_int16"), "Column 'new_int16' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.INT16, cols.get("new_int16").getType());
        assertTrue(cols.containsKey("new_int32"), "Column 'new_int32' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.INT32, cols.get("new_int32").getType());
        assertTrue(cols.containsKey("new_int64"), "Column 'new_int64' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.INT64, cols.get("new_int64").getType());
        assertTrue(cols.containsKey("new_float32"), "Column 'new_float32' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.FLOAT32, cols.get("new_float32").getType());
        assertTrue(cols.containsKey("new_float64"), "Column 'new_float64' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.FLOAT64, cols.get("new_float64").getType());
        assertTrue(cols.containsKey("new_bool"), "Column 'new_bool' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.BOOLEAN, cols.get("new_bool").getType());
        assertTrue(cols.containsKey("new_string"), "Column 'new_string' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.STRING, cols.get("new_string").getType());
        assertTrue(cols.containsKey("new_bytes"), "Column 'new_bytes' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.STRING, cols.get("new_bytes").getType());

        // Logical types
        assertTrue(cols.containsKey("new_decimal"), "Column 'new_decimal' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.Decimal, cols.get("new_decimal").getType());
        assertTrue(cols.containsKey("new_date"), "Column 'new_date' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.Date32, cols.get("new_date").getType());
        assertTrue(cols.containsKey("new_timestamp"), "Column 'new_timestamp' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.DateTime64, cols.get("new_timestamp").getType());
    }

    // auto-evolve creates Array columns with different element types (Int32, Float64, Bool, String)
    @Test
    public void autoEvolveTypedArrayColumns() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_typed_arrays_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithTypedArrays(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(srV2);
        chst.stop();

        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify all array columns were created
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        Map<String, com.clickhouse.kafka.connect.sink.db.mapping.Column> cols = described.getRootColumnsMap();

        assertTrue(cols.containsKey("arr_int32"), "Column 'arr_int32' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.ARRAY, cols.get("arr_int32").getType());
        assertTrue(cols.containsKey("arr_float64"), "Column 'arr_float64' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.ARRAY, cols.get("arr_float64").getType());
        assertTrue(cols.containsKey("arr_bool"), "Column 'arr_bool' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.ARRAY, cols.get("arr_bool").getType());
        assertTrue(cols.containsKey("arr_string"), "Column 'arr_string' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.ARRAY, cols.get("arr_string").getType());
    }

    // DDL refresh timeout - retries set to 0 so refresh loop never runs, throws RetriableException
    @Test
    public void autoEvolveDdlRefreshTimeoutThrowsRetriable() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE_DDL_REFRESH_RETRIES, "0");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_ddl_timeout_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // V2 schema with a new field - DDL will succeed but refresh will timeout with 0 retries
        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithNewNullableField(topic, 1, 5);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);

        try {
            chst.put(srV2);
            assertTrue(false, "Expected RetriableException due to DDL refresh timeout with 0 retries");
        } catch (RuntimeException e) {
            // Processing layer may wrap the RetriableException - walk the cause chain
            Throwable t = e;
            boolean found = false;
            while (t != null) {
                if (t instanceof org.apache.kafka.connect.errors.RetriableException
                        && t.getMessage() != null && t.getMessage().contains("DDL propagation timeout")) {
                    found = true;
                    break;
                }
                t = t.getCause();
            }
            assertTrue(found, "Should contain RetriableException with DDL propagation timeout in cause chain, got: " + e);
        } finally {
            chst.stop();
        }
    }

    // ALTER TABLE itself fails (table dropped externally after cache populated)
    @Test
    public void autoEvolveDdlExecutionFailureThrowsRuntimeException() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_ddl_exec_failure_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Insert V1 records to populate the connector's internal table mapping cache
        Collection<SinkRecord> srV1 = SchemaTestData.createSchemaV1(topic, 1, 5);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(srV1);
        assertEquals(5, ClickHouseTestHelpers.countRows(chc, topic));

        // Drop the table externally - the connector still has it cached in memory
        ClickHouseTestHelpers.dropTable(chc, topic);

        // V2 schema with a new field - ALTER TABLE will fail because the table no longer exists
        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithNewNullableField(topic, 1, 5);
        try {
            chst.put(srV2);
            assertTrue(false, "Expected RuntimeException due to ALTER TABLE on dropped table");
        } catch (RuntimeException e) {
            // Processing layer wraps exceptions - walk the cause chain for the DDL failure
            Throwable t = e;
            boolean found = false;
            while (t != null) {
                if (t.getMessage() != null && (t.getMessage().contains("ALTER TABLE") || t.getMessage().contains("UNKNOWN_TABLE"))) {
                    found = true;
                    break;
                }
                t = t.getCause();
            }
            assertTrue(found, "Should indicate DDL failure in cause chain, got: " + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            chst.stop();
        }
    }

    // STRUCT field without struct-to-json flag throws SchemaTypeInferenceException with helpful message
    @Test
    public void autoEvolveUnsupportedStructTypeThrowsError() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        // auto.evolve.struct.to.json is false by default
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_unsupported_struct_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithStructField(topic, 1, 5);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);

        try {
            chst.put(srV2);
            assertTrue(false, "Expected SchemaTypeInferenceException for unsupported STRUCT type");
        } catch (RuntimeException e) {
            Throwable t = e;
            boolean foundInference = false;
            while (t != null) {
                if (t instanceof com.clickhouse.kafka.connect.sink.db.mapping.SchemaTypeInferenceException) {
                    foundInference = true;
                    break;
                }
                t = t.getCause();
            }
            assertTrue(foundInference,
                    "Should throw SchemaTypeInferenceException for unsupported STRUCT, got: " + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            chst.stop();
        }
    }

    // Avro-style union(string, bytes) STRUCT collapses to Nullable(String), not JSON
    @Test
    public void autoEvolveStringBytesUnionCollapsesToString() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_union_string_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        Collection<SinkRecord> srV2 = SchemaTestData.createSchemaV2WithStringBytesUnionField(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(srV2);
        chst.stop();

        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify the union field was created as String (not JSON)
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        com.clickhouse.kafka.connect.sink.db.mapping.Column col = described.getRootColumnsMap().get("new_union_field");
        assertNotNull(col, "Column 'new_union_field' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.STRING, col.getType(),
                "Union(string, bytes) should collapse to String, not JSON");
    }

    // Avro union(string, int) auto-evolved as Variant(String, Int32) column
    @Test
    @SinceClickHouseVersion("24.1")
    public void autoEvolveMixedUnionCreatesVariantColumn() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        props.put(ClickHouseSinkConfig.CLICKHOUSE_SETTINGS, "allow_experimental_variant_type=1");
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("auto_evolve_variant_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        Collection<SinkRecord> records = SchemaTestData.createSchemaV2WithMixedTypeUnionField(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(records);
        chst.stop();

        assertEquals(10, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify the union field was created as Variant
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        com.clickhouse.kafka.connect.sink.db.mapping.Column col = described.getRootColumnsMap().get("mixed_union");
        assertNotNull(col, "Column 'mixed_union' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.VARIANT, col.getType(),
                "union(string, int) should map to Variant, not String or JSON");
    }

    @Test
    public void autoEvolveSchemalessRecordsThrowError() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_schemaless_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Create schemaless (string) records. No valueSchema.
        List<SinkRecord> schemaless = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String json = String.format("{\"off16\": %d, \"p_int64\": %d}", i, (long) i);
            schemaless.add(new SinkRecord(
                    topic, 1, null, null, null, json,
                    i, System.currentTimeMillis(), TimestampType.CREATE_TIME
            ));
        }

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);

        try {
            chst.put(schemaless);
            assertTrue(false, "Expected exception for schemaless records with auto.evolve=true");
        } catch (RuntimeException e) {
            Throwable t = e;
            boolean found = false;
            while (t != null) {
                if (t.getMessage() != null && t.getMessage().contains("auto.evolve requires a Connect schema")) {
                    found = true;
                    break;
                }
                t = t.getCause();
            }
            assertTrue(found, "Expected error about schemaless records in cause chain, got: " + e.getMessage());
        } finally {
            chst.stop();
        }
    }

    // Avro union(string, bytes) fields auto-evolved as Nullable(String) columns
    @Test
    public void autoEvolveAvroUnionStringBytesCreatesStringColumn() throws Exception {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "auto_evolve_avro_union_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        // Table starts with only "name" - union fields "content" and "description" will be auto-evolved
        ClickHouseTestHelpers.createTable(chc, topic,
                "CREATE TABLE `%s` (`name` String) Engine = MergeTree ORDER BY name");

        Image image1 = Image.newBuilder()
                .setName("image1")
                .setContent("content1")
                .build();
        Image image2 = Image.newBuilder()
                .setName("image2")
                .setContent(ByteBuffer.wrap("content2".getBytes()))
                .setDescription("desc2")
                .build();

        List<SinkRecord> records = SchemaTestData.convertAvroToSinkRecord(
                topic, new AvroSchema(Image.getClassSchema()), Arrays.asList(image1, image2));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(records);
        chst.stop();

        assertEquals(2, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify that union columns were created as String (not JSON)
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        Map<String, com.clickhouse.kafka.connect.sink.db.mapping.Column> cols = described.getRootColumnsMap();

        assertTrue(cols.containsKey("content"), "Column 'content' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.STRING, cols.get("content").getType(),
                "union(string, bytes) should map to String, not JSON");

        assertTrue(cols.containsKey("description"), "Column 'description' should exist");
        assertEquals(com.clickhouse.kafka.connect.sink.db.mapping.Type.STRING, cols.get("description").getType(),
                "union(null, string, bytes) should map to Nullable(String), not JSON");

        // Verify data was inserted correctly
        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
        if (rows.size() == 0) {
            rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
        }
        assertEquals(2, rows.size());
    }

    @Test
    public void autoEvolveMixedBatchLastRecordOlderSchema() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("auto_evolve_last_record_older_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Build batch where V2 records come first and V1 (older) records are last.
        // Before the fix, only the last record was checked - V1 has no new fields, so ALTER TABLE was skipped.
        List<SinkRecord> batch = new ArrayList<>();
        batch.addAll(SchemaTestData.createSchemaV2WithNewNullableField(topic, 1, 3));
        batch.addAll(SchemaTestData.createSchemaV1(topic, 1, 2));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(batch);
        chst.stop();

        assertEquals(5, ClickHouseTestHelpers.countRows(chc, topic));

        // The new column from V2 should exist even though V1 was the last record
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("new_string_field"),
                "Column 'new_string_field' should be added even when last record is V1 (older schema)");

        // V1 records should have NULL for the new column
        String nullCountQuery = String.format(
                "SELECT COUNT(*) FROM `%s` WHERE `new_string_field` IS NULL SETTINGS select_sequential_consistency = 1", topic);
        try {
            Records nullRecords = chc.getClient().queryRecords(nullCountQuery).get();
            int nullCount = Integer.parseInt(nullRecords.iterator().next().getString(1));
            assertEquals(2, nullCount, "V1 records should have NULL for new_string_field");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void autoEvolveMultiVersionUnionSemantics() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("auto_evolve_union_semantics_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Batch with V1, V2, V3, V4 - each version adds a different field.
        // All new fields should be added in a single ALTER TABLE.
        List<SinkRecord> batch = new ArrayList<>();
        batch.addAll(SchemaTestData.createSchemaV1(topic, 1, 2));
        batch.addAll(SchemaTestData.createSchemaV2WithNewNullableField(topic, 1, 2));
        batch.addAll(SchemaTestData.createSchemaV3WithExtraField(topic, 1, 2));
        batch.addAll(SchemaTestData.createSchemaV4WithUniqueField(topic, 1, 2));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(batch);
        chst.stop();

        assertEquals(8, ClickHouseTestHelpers.countRows(chc, topic));

        // Verify all fields from V2, V3, V4 exist
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("new_string_field"),
                "V2 column 'new_string_field' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("v3_bool_field"),
                "V3 column 'v3_bool_field' should exist");
        assertTrue(described.getRootColumnsMap().containsKey("v4_float_field"),
                "V4 column 'v4_float_field' should exist");
    }

    @Test
    public void autoEvolveInterleavedSchemaVersions() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("auto_evolve_non_monotonic_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Interleaved schema versions [V1, V3, V2, V1, V3]
        List<SinkRecord> batch = new ArrayList<>();
        batch.addAll(SchemaTestData.createSchemaV1(topic, 1, 1));
        batch.addAll(SchemaTestData.createSchemaV3WithExtraField(topic, 1, 1));
        batch.addAll(SchemaTestData.createSchemaV2WithNewNullableField(topic, 1, 1));
        batch.addAll(SchemaTestData.createSchemaV1(topic, 1, 1));
        batch.addAll(SchemaTestData.createSchemaV3WithExtraField(topic, 1, 1));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(batch);
        chst.stop();

        assertEquals(5, ClickHouseTestHelpers.countRows(chc, topic));

        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("new_string_field"),
                "V2 column 'new_string_field' should exist despite non-monotonic order");
        assertTrue(described.getRootColumnsMap().containsKey("v3_bool_field"),
                "V3 column 'v3_bool_field' should exist despite non-monotonic order");
    }

    @Test
    public void autoEvolveCrossPartitionSchemaDrift() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.AUTO_EVOLVE, "true");
        props.put(ClickHouseSinkConfig.IGNORE_PARTITIONS_WHEN_BATCHING, "true");
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("auto_evolve_cross_partition_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `p_int64` Int64 ) Engine = MergeTree ORDER BY off16");

        // Records from different partitions with different schema versions.
        // With ignorePartitionsWhenBatching=true, they are merged into a single batch.
        // Partition 0: V2 records (has new_string_field)
        // Partition 1: V1 records (no new_string_field) - these may end up last
        List<SinkRecord> batch = new ArrayList<>();
        batch.addAll(SchemaTestData.createSchemaV2WithNewNullableField(topic, 0, 3));
        batch.addAll(SchemaTestData.createSchemaV1(topic, 1, 3));

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(batch);
        chst.stop();

        assertEquals(6, ClickHouseTestHelpers.countRows(chc, topic));

        // The new column from V2 (partition 0) should exist even though
        // V1 records from partition 1 may be last in the merged batch
        com.clickhouse.kafka.connect.sink.db.mapping.Table described = chc.describeTable(chc.getDatabase(), topic);
        assertTrue(described.getRootColumnsMap().containsKey("new_string_field"),
                "Column 'new_string_field' should be added with cross-partition schema drift");
    }
}
