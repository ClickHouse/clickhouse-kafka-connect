package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.SchemaTestData;
import com.clickhouse.kafka.connect.test.junit.extension.FromVersionConditionExtension;
import com.clickhouse.kafka.connect.test.junit.extension.SinceClickHouseVersion;
import com.clickhouse.kafka.connect.test.TestProtos;
import com.clickhouse.kafka.connect.util.Utils;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.connect.protobuf.ProtobufConverterConfig;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.json.JSONArray;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(FromVersionConditionExtension.class)
public class ClickHouseSinkTaskWithSchemaTest extends ClickHouseBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkTaskWithSchemaTest.class);
    @Test
    public void arrayTypesTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = "array_string_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), " +
                "`arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), " +
                "`arr_float64` Array(Float64), `arr_bool` Array(Bool), `arr_str_arr` Array(Array(String)), `arr_arr_str_arr` Array(Array(Array(String))), " +
                "`arr_map` Array(Map(String, String))  ) Engine = MergeTree ORDER BY off16");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr_nullable_str` Array(Nullable(String)), `arr_empty_nullable_str` Array(Nullable(String)), `arr_nullable_int8` Array(Nullable(Int8)), `arr_nullable_int16` Array(Nullable(Int16)), `arr_nullable_int32` Array(Nullable(Int32)), `arr_nullable_int64` Array(Nullable(Int64)), `arr_nullable_float32` Array(Nullable(Float32)), `arr_nullable_float64` Array(Nullable(Float64)), `arr_nullable_bool` Array(Nullable(Bool))  ) Engine = MergeTree ORDER BY off16");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, map_string_string Map(String, String), " +
                "map_string_int64 Map(String, Int64), map_int64_string Map(Int64, String), map_string_map Map(String, Map(String, Int64))," +
                "map_string_array Map(String, Array(String)), map_map_map Map(String, Map(String, Map(String, String)))  ) Engine = MergeTree ORDER BY off16");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String)  ) Engine = MergeTree ORDER BY off16");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(Nullable(String))  ) Engine = MergeTree ORDER BY off16");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE MATERIALIZED VIEW %s_mv TO " + topic + "_mate AS SELECT off16 FROM " + topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s_mate ( `off16` Int16 ) Engine = Null");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, " +
                "map_string_string Map(String, String), map_string_int64 Map(String, Int64), map_int64_string Map(Int64, String), " +
                "map_string_map Map(String, Map(String, Int64)), map_string_array Map(String, Array(String))," +
                "map_map_map Map(String, Map(String, Map(String, String)))  ) Engine = MergeTree ORDER BY off16");
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("support-dates-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, date_number Nullable(Date), date32_number Nullable(Date32), datetime_number DateTime, datetime64_number DateTime64, timestamp_int64 Int64, timestamp_date DateTime64, time_int32 Int32, time_date32 Date32, date_date Date, datetime_date DateTime ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createDateType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void supportArrayDateTime64Test() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("support-array-datetime64-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, arr_datetime64_number Array(DateTime64), arr_timestamp_date Array(DateTime64) ) Engine = MergeTree ORDER BY off16");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, date_number Date, date32_number Date32, datetime_number DateTime, datetime64_number DateTime64) Engine = MergeTree ORDER BY off16");

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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, zoned_date DateTime64, offset_date DateTime64) Engine = MergeTree ORDER BY off16");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, format_date DateTime64(9)) Engine = MergeTree ORDER BY off16");
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schema_empty_records_lc_table_test");
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schema_empty_records_lc_table_test");
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("default-value-table-test");
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
    public void schemaWithEphemeralTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("default-value-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, default_value_data DateTime DEFAULT now(), ephemeral_data String EPHEMERAL ) Engine = MergeTree ORDER BY off16");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, null_value_data Nullable(DateTime), default_value_data DateTime DEFAULT now() ) Engine = MergeTree ORDER BY off16");
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
    public void schemaWithFixedStringTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("fixed-string-value-table-test");
        int fixedStringSize = RandomUtils.nextInt(1, 100);
        LOGGER.info("FixedString size: " + fixedStringSize);
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, " +
                "`fixed_string_string` FixedString("+fixedStringSize+"), " +
                "`fixed_string_bytes` FixedString("+fixedStringSize+")" +
                ") Engine = MergeTree ORDER BY off16");

        Collection<SinkRecord> sr = SchemaTestData.createFixedStringData(topic, 1, fixedStringSize);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void schemaWithFixedStringMismatchTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("fixed-string-mismatch-table-test");
        int fixedStringSize = RandomUtils.nextInt(2, 100);
        LOGGER.info("FixedString size: " + fixedStringSize);
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, " +
                "`fixed_string_string` FixedString(" + (fixedStringSize - 1 ) + ") ) Engine = MergeTree ORDER BY off16");

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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` ( `off16` Int16, `decimal_14_2` Nullable(Decimal(14, 2)) ) Engine = MergeTree ORDER BY off16");

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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` (`string` String) Engine = MergeTree ORDER BY `string`");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` (`off16` Int16, `enum8_type` Enum8('A' = 1, 'B' = 2, 'C' = 3), `enum16_type` Enum16('A' = 1, 'B' = 2, 'C' = 3, 'D' = 4)) Engine = MergeTree ORDER BY off16");
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

        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` (" +
                "`off16` Int16," +
                "`tuple` Tuple(" +
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
                ")) Engine = MergeTree ORDER BY `off16`",
                Map.of(
                        "allow_experimental_variant_type", 1
                ));
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

        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` (" +
                "`off16` Int16," +
                "`nested` Nested(" +
                "  `string` String," +
                "  `decimal` Decimal(14, 2)," +
                "  `tuple` Tuple(" +
                "    `map` Map(String, String)," +
                "    `variant` Variant(Boolean, String)" +
                "))) Engine = MergeTree ORDER BY `off16`",
                Map.of(
                        "allow_experimental_variant_type", 1
                ));
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` (" +
                "`off16` Int16," +
                "`uint8` UInt8," +
                "`uint16` UInt16," +
                "`uint32` UInt32," +
                "`uint64` UInt64" +
                ") Engine = MergeTree ORDER BY `off16`");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` (" +
                "`off16` Int16," +
                "`string` String" +
                ") Engine = MergeTree ORDER BY `off16`");
        Collection<SinkRecord> sr = SchemaTestData.createSimpleData(topic, 1);

        int numRecords = sr.size();
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));


        ClickHouseTestHelpers.createTable(chc, topic, "ALTER TABLE `%s` ADD COLUMN num32 Nullable(Int32) AFTER string");
        Thread.sleep(5000);
        sr = SchemaTestData.createSimpleExtendWithNullableData(topic, 1, 10000, 2000);
        int numRecordsWithNullable = sr.size();
        chst.put(sr);

        ClickHouseTestHelpers.createTable(chc, topic, "ALTER TABLE `%s` ADD COLUMN num32_default Int32 DEFAULT 0 AFTER num32");
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
    public void changeSchemaWhileRunningWithRefreshEnabled() throws InterruptedException {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        props.put(ClickHouseSinkConfig.TABLE_REFRESH_INTERVAL, "1");
        String topic = createTopicName("change-schema-while-running-table-test-with-refresh-enabled");
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` (" +
                "`off16` Int16," +
                "`string` String" +
                ") Engine = MergeTree ORDER BY `off16`");
        Collection<SinkRecord> sr = SchemaTestData.createSimpleData(topic, 1);

        int numRecords = sr.size();
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));


        ClickHouseTestHelpers.createTable(chc, topic, "ALTER TABLE `%s` ADD COLUMN num32 Nullable(Int32) AFTER string");
        Thread.sleep(5000);
        sr = SchemaTestData.createSimpleExtendWithNullableData(topic, 1, 10000, 2000);
        int numRecordsWithNullable = sr.size();
        chst.put(sr);

        ClickHouseTestHelpers.createTable(chc, topic, "ALTER TABLE `%s` ADD COLUMN num32_default Int32 DEFAULT 0 AFTER num32");
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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` (" +
                "`off16` Int16," +
                "`string` String," +
                "`t` Tuple(" +
                "    `off16` Nullable(Int16)," +
                "    `string` Nullable(String) " +
                ")) Engine = MergeTree ORDER BY `off16`");

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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` (" +
                "`off16` Int16," +
                "`string` String," +
                "`insert_datetime` DateTime default now()," +
                "`t` Tuple(" +
                "    `off16` Nullable(Int16)," +
                "    `string` Nullable(String) " +
                ")" +
                ") Engine = MergeTree ORDER BY `off16`");

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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` (" +
                "`off16` Int16," +
                "`string` String," +
                "`insert_datetime` DateTime default now()," +
                "`t` Tuple(" +
                "    `off16` Nullable(Int16)," +
                "    `string` Nullable(String), " +
                "    `n` Tuple(" +
                "    `off16` Nullable(Int16)," +
                "    `string` Nullable(String) " +
                ")" +
                ")" +
                ") Engine = MergeTree ORDER BY `off16`");

        Collection<SinkRecord> sr = SchemaTestData.createNestedTupleSimpleData(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void coolSchemaWithRandomFields() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("cool-schema-with-random-field");

        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE `%s` (" +
                "`processing_time` DateTime," +
                "`insert_time` DateTime DEFAULT now()," +
                "`type` String," +
                "`player` Tuple(id Nullable(Int64), key Nullable(String), ip Nullable(String), label Nullable(String), device_id Nullable(Int64), player_tracker_id Nullable(Int64), is_new_player Nullable(Bool), player_root Nullable(String), target Nullable(String), `type` Nullable(String), name Nullable(String), processing_time Nullable(Int64), tags Map(String, String), session_id Nullable(String), machine_fingerprint Nullable(String), player_fingerprint Nullable(String))," +
                "`sensor` Tuple(sensor_id String, origin_device String, session_id String, machine_id Int64, machine_timestamp String)," +
                "`data` String, " +
                "`id` String, " +
                "`desc` Nullable(String), " +
                "`tag` Nullable(String), " +
                "`va` Nullable(Float64) " +
                ") Engine = MergeTree ORDER BY `processing_time`");

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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, " +
                " `struct_content` JSON, `json_as_str` JSON ) Engine = MergeTree ORDER BY off16", clientSettings);

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

        ClickHouseTestHelpers.createTable(chc, topic,
                "CREATE TABLE %s ( `id` Int32, `name` String, `is_active` Boolean, `score` Float64, `tags` Array(String), " +
                " `content` JSON ) Engine = MergeTree ORDER BY ()", clientSettings);

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
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), " +
                "`arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), " +
                "`arr_float64` Array(Float64), `arr_bool` Array(Bool), `arr_str_arr` Array(Array(String)), `arr_arr_str_arr` Array(Array(Array(String))), " +
                "`arr_map` Array(Map(String, String))  ) Engine = MergeTree ORDER BY off16");
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
    public void splitDBTopicTest() throws Exception {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic1 = "tenant_1__events";
        String topic2 = "tenant_2__events";
        ClickHouseTestHelpers.dropDatabase(chc, "tenant_1");
        ClickHouseTestHelpers.dropDatabase(chc, "tenant_2");
        ClickHouseTestHelpers.createDatabase(chc, "tenant_1");
        ClickHouseTestHelpers.createDatabase(chc, "tenant_2");

        ClickHouseTestHelpers.query(chc, "CREATE TABLE `tenant_1`.`events` (" +
                "`off16` Int16," +
                "`string` String" +
                ") Engine = MergeTree ORDER BY `off16`");
        ClickHouseTestHelpers.query(chc, "CREATE TABLE `tenant_2`.`events` (" +
                "`off16` Int16," +
                "`string` String" +
                ") Engine = MergeTree ORDER BY `off16`");

        Collection<SinkRecord> sr1 = SchemaTestData.createSimpleData(topic1, 1, 5);
        Collection<SinkRecord> sr2 = SchemaTestData.createSimpleData(topic2, 1, 10);

        List<SinkRecord> records = new ArrayList<>();
        records.addAll(sr1);
        records.addAll(sr2);
        Collections.shuffle(records);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        props.put(ClickHouseSinkConfig.DB_TOPIC_SPLIT_CHAR, "__");
        props.put(ClickHouseSinkConfig.ENABLE_DB_TOPIC_SPLIT, "true");
        props.put(ClickHouseSinkConfig.DATABASE, "tenant_1");
        chst.start(props);
        chst.put(records);
        chst.stop();


        assertEquals(sr1.size(), ClickHouseTestHelpers.countRows(chc, "events", "tenant_1"));
        assertEquals(sr2.size(), ClickHouseTestHelpers.countRows(chc, "events", "tenant_2"));
    }
}
