package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.SchemaTestData;
import com.clickhouse.kafka.connect.sink.junit.extension.FromVersionConditionExtension;
import com.clickhouse.kafka.connect.sink.junit.extension.SinceClickHouseVersion;
import com.clickhouse.kafka.connect.util.Utils;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils;

import java.awt.desktop.SystemSleepEvent;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig.TABLE_REFRESH_INTERVAL;
import static org.junit.jupiter.api.Assertions.*;

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
        props.put(TABLE_REFRESH_INTERVAL, "1");
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
}
