package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import com.clickhouse.kafka.connect.test.junit.extension.FromVersionConditionExtension;
import com.clickhouse.kafka.connect.test.junit.extension.SinceClickHouseVersion;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(FromVersionConditionExtension.class)
public class ClickHouseSinkTaskSchemalessTest extends ClickHouseBase {

    private static LinkedHashMap<String, String> primitiveTypesSchema() {
        LinkedHashMap<String, String> s = new LinkedHashMap<>();
        s.put("off16", "Int16"); s.put("str", "String");
        s.put("p_int8", "Int8"); s.put("p_int16", "Int16"); s.put("p_int32", "Int32");
        s.put("p_int64", "Int64"); s.put("p_float32", "Float32");
        s.put("p_float64", "Float64"); s.put("p_bool", "Bool");
        return s;
    }

    @Test
    public void primitiveTypesTest() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = createTopicName("schemaless_primitive_types_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new ClickHouseTestHelpers.CreateTableStatement(chc)
                .setTableName(topic).setSchema(primitiveTypesSchema())
                .setEngine("MergeTree").setOrderByColumn("off16").execute();
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
        new ClickHouseTestHelpers.CreateTableStatement(chc)
                .setTableName(topic).setSchema(new LinkedHashMap<>() {{ put("off16", "Int16"); put("str", "String"); put("p_int8", "Int8"); }})
                .setEngine("MergeTree").setOrderByColumn("off16").execute();
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
        new ClickHouseTestHelpers.CreateTableStatement(chc)
                .setTableName(topic).setSchema(primitiveTypesSchema())
                .setEngine("MergeTree").setOrderByColumn("off16").execute();
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
        new ClickHouseTestHelpers.CreateTableStatement(chc)
                .setTableName(topic).setSchema(new LinkedHashMap<>() {{
                    put("off16", "Int16"); put("str", "String"); put("null_str", "Nullable(String)");
                    put("p_int8", "Int8"); put("p_int16", "Int16"); put("p_int32", "Int32");
                    put("p_int64", "Int64"); put("p_float32", "Float32"); put("p_float64", "Float64"); put("p_bool", "Bool");
                }}).setEngine("MergeTree").setOrderByColumn("off16").execute();
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
        new ClickHouseTestHelpers.CreateTableStatement(chc)
                .setTableName(topic).setSchema(new LinkedHashMap<>() {{
                    put("off16", "Int16"); put("arr", "Array(String)"); put("arr_empty", "Array(String)");
                    put("arr_int8", "Array(Int8)"); put("arr_int16", "Array(Int16)"); put("arr_int32", "Array(Int32)");
                    put("arr_int64", "Array(Int64)"); put("arr_float32", "Array(Float32)");
                    put("arr_float64", "Array(Float64)"); put("arr_bool", "Array(Bool)");
                }}).setEngine("MergeTree").setOrderByColumn("off16").execute();
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
        new ClickHouseTestHelpers.CreateTableStatement(chc)
                .setTableName(topic).setSchema(new LinkedHashMap<>() {{
                    put("off16", "Int16"); put("map_string_string", "Map(String, String)");
                    put("map_string_int64", "Map(String, Int64)"); put("map_int64_string", "Map(Int64, String)");
                }}).setEngine("MergeTree").setOrderByColumn("off16").execute();
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
        new ClickHouseTestHelpers.CreateTableStatement(chc)
                .setTableName(topic).setSchema(new LinkedHashMap<>() {{
                    put("off16", "Int16"); put("map_string_string", "Map(String, String)");
                    put("map_string_int64", "Map(String, Int64)"); put("map_int64_string", "Map(Int64, String)");
                }}).setEngine("MergeTree").setOrderByColumn("off16").execute();
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
        new ClickHouseTestHelpers.CreateTableStatement(chc)
                .setTableName(topic).setSchema(new LinkedHashMap<>() {{ put("off16", "Int16"); put("str", "String"); }})
                .setEngine("MergeTree").setOrderByColumn("off16").execute();
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
        new ClickHouseTestHelpers.CreateTableStatement(chc)
                .setTableName(topic).setSchema(new LinkedHashMap<>() {{ put("num", "String"); put("decimal_14_2", "Decimal(14, 2)"); }})
                .setEngine("MergeTree").setOrderByColumn("num").execute();
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
        new ClickHouseTestHelpers.CreateTableStatement(chc)
                .setTableName(topic).setSchema(new LinkedHashMap<>() {{ put("num", "String"); put("decimal_14_2", "Nullable(Decimal(14, 2))"); }})
                .setEngine("MergeTree").setOrderByColumn("num").execute();
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
        new ClickHouseTestHelpers.CreateTableStatement(chc)
                .setTableName(topic).setSchema(primitiveTypesSchema())
                .setEngine("MergeTree").setOrderByColumn("off16").execute();
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
        new ClickHouseTestHelpers.CreateTableStatement(chc)
                .setTableName(topic).setSchema(new LinkedHashMap<>() {{ put("off16", "Int16"); put("content", "JSON"); put("struct", "JSON"); }})
                .setEngine("MergeTree").setOrderByColumn("off16").setSettings(clientSettings).execute();

        Collection<SinkRecord> sr = SchemalessTestData.createJSONType(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
    }
}
