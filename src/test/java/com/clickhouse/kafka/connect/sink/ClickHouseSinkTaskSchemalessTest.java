package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import com.clickhouse.kafka.connect.test.junit.extension.FromVersionConditionExtension;
import com.clickhouse.kafka.connect.test.junit.extension.SinceClickHouseVersion;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(FromVersionConditionExtension.class)
public class ClickHouseSinkTaskSchemalessTest extends ClickHouseBase {

    private static final CreateTableStatement PRIMITIVE_TYPES_TABLE = new CreateTableStatement()
            .setColumn("off16", "Int16")
            .setColumn("str", "String")
            .setColumn("p_int8", "Int8")
            .setColumn("p_int16", "Int16")
            .setColumn("p_int32", "Int32")
            .setColumn("p_int64", "Int64")
            .setColumn("p_float32", "Float32")
            .setColumn("p_float64", "Float64")
            .setColumn("p_bool", "Bool")
            .setEngine("MergeTree")
            .setOrderByColumn("off16");

    @Test
    public void primitiveTypesTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = createTopicName("schemaless_primitive_types_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).setTableName(topic).execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        assertTrue(ClickHouseTestHelpers.validateRows(chc, topic, sr));
    }

    @Test
    public void primitiveTypesSubsetTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = createTopicName("schemaless_primitive_types_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .setTableName(topic)
                .setColumn("off16", "Int16")
                .setColumn("str", "String")
                .setColumn("p_int8", "Int8")
                .setEngine("MergeTree").setOrderByColumn("off16").execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        assertFalse(ClickHouseTestHelpers.validateRows(chc, topic, sr));
    }

    @Test
    public void withEmptyDataRecordsTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = createTopicName("schemaless_empty_records_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).setTableName(topic).execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createWithEmptyDataRecords(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void NullableValuesTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = createTopicName("schemaless_nullable_values_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .setTableName(topic)
                .setColumn("off16", "Int16")
                .setColumn("str", "String")
                .setColumn("null_str", "Nullable(String)")
                .setColumn("p_int8", "Int8")
                .setColumn("p_int16", "Int16")
                .setColumn("p_int32", "Int32")
                .setColumn("p_int64", "Int64")
                .setColumn("p_float32", "Float32")
                .setColumn("p_float64", "Float64")
                .setColumn("p_bool", "Bool")
                .setEngine("MergeTree").setOrderByColumn("off16").execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypesWithNulls(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }

    @Test
    public void arrayTypesTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schemaless_array_string_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .setTableName(topic)
                .setColumn("off16", "Int16")
                .setColumn("arr", "Array(String)")
                .setColumn("arr_empty", "Array(String)")
                .setColumn("arr_int8", "Array(Int8)")
                .setColumn("arr_int16", "Array(Int16)")
                .setColumn("arr_int32", "Array(Int32)")
                .setColumn("arr_int64", "Array(Int64)")
                .setColumn("arr_float32", "Array(Float32)")
                .setColumn("arr_float64", "Array(Float64)")
                .setColumn("arr_bool", "Array(Bool)")
                .setEngine("MergeTree").setOrderByColumn("off16").execute(chc);
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schemaless_map_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .setTableName(topic)
                .setColumn("off16", "Int16")
                .setColumn("map_string_string", "Map(String, String)")
                .setColumn("map_string_int64", "Map(String, Int64)")
                .setColumn("map_int64_string", "Map(Int64, String)")
                .setEngine("MergeTree").setOrderByColumn("off16").execute(chc);
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("special-char-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .setTableName(topic)
                .setColumn("off16", "Int16")
                .setColumn("map_string_string", "Map(String, String)")
                .setColumn("map_string_int64", "Map(String, Int64)")
                .setColumn("map_int64_string", "Map(Int64, String)")
                .setEngine("MergeTree").setOrderByColumn("off16").execute(chc);
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("emojis_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .setTableName(topic)
                .setColumn("off16", "Int16")
                .setColumn("str", "String")
                .setEngine("MergeTree").setOrderByColumn("off16").execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createDataWithEmojis(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, ClickHouseTestHelpers.countRowsWithEmojis(chc, topic));
    }

    @Test
    public void decimalDataTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("decimal_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .setTableName(topic)
                .setColumn("num", "String")
                .setColumn("decimal_14_2", "Decimal(14, 2)")
                .setEngine("MergeTree").setOrderByColumn("num").execute(chc);
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("nullable_decimal_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .setTableName(topic)
                .setColumn("num", "String")
                .setColumn("decimal_14_2", "Nullable(Decimal(14, 2))")
                .setEngine("MergeTree").setOrderByColumn("num").execute(chc);
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
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("schemaless_primitive_types_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).setTableName(topic).execute(chc);
        List<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);
        List<SinkRecord> smallerCollection = sr.subList(0, sr.size() / 2);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(smallerCollection);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }


    @Test
    @SinceClickHouseVersion("24.10")
    public void jsonTypeTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schemaless_json_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        Map<String, Serializable> clientSettings = new HashMap<>();
        clientSettings.put(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");
        new CreateTableStatement()
                .setTableName(topic)
                .setColumn("off16", "Int16")
                .setColumn("content", "JSON")
                .setColumn("struct", "JSON")
                .setEngine("MergeTree").setOrderByColumn("off16").setSettings(clientSettings).execute(chc);

        Collection<SinkRecord> sr = SchemalessTestData.createJSONType(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }
}
