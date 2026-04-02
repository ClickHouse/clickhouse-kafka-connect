package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseDeploymentType;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import com.clickhouse.kafka.connect.test.junit.extension.FromVersionConditionExtension;
import com.clickhouse.kafka.connect.test.junit.extension.SinceClickHouseVersion;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(FromVersionConditionExtension.class)
public class ClickHouseSinkTaskSchemalessTest extends ClickHouseBase {

    private static final CreateTableStatement PRIMITIVE_TYPES_TABLE = new CreateTableStatement()
            .column("off16", "Int16")
            .column("str", "String")
            .column("p_int8", "Int8")
            .column("p_int16", "Int16")
            .column("p_int32", "Int32")
            .column("p_int64", "Int64")
            .column("p_float32", "Float32")
            .column("p_float64", "Float64")
            .column("p_bool", "Bool")
            .orderByColumn("off16");

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void primitiveTypesTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = createTopicName("schemaless_primitive_types_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
        assertTrue(ClickHouseTestHelpers.validateRows(chc, topic, sr, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void primitiveTypesSubsetTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = createTopicName("schemaless_primitive_types_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("str", "String")
                .column("p_int8", "Int8")
                .orderByColumn("off16").clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
        assertFalse(ClickHouseTestHelpers.validateRows(chc, topic, sr, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void withEmptyDataRecordsTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = createTopicName("schemaless_empty_records_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createWithEmptyDataRecords(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void NullableValuesTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = createClient(props);
        // `arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), `arr_float64` Array(Float64), `arr_bool` Array(Bool)
        String topic = createTopicName("schemaless_nullable_values_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("str", "String")
                .column("null_str", "Nullable(String)")
                .column("p_int8", "Int8")
                .column("p_int16", "Int16")
                .column("p_int32", "Int32")
                .column("p_int64", "Int64")
                .column("p_float32", "Float32")
                .column("p_float64", "Float64")
                .column("p_bool", "Bool")
                .orderByColumn("off16").clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypesWithNulls(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void arrayTypesTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schemaless_array_string_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
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
                .orderByColumn("off16").clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemalessTestData.createArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void mapTypesTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schemaless_map_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("map_string_string", "Map(String, String)")
                .column("map_string_int64", "Map(String, Int64)")
                .column("map_int64_string", "Map(Int64, String)")
                .orderByColumn("off16").clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemalessTestData.createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    // https://github.com/ClickHouse/clickhouse-kafka-connect/issues/38
    public void specialCharTableNameTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("special-char-table-test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("map_string_string", "Map(String, String)")
                .column("map_string_int64", "Map(String, Int64)")
                .column("map_int64_string", "Map(Int64, String)")
                .orderByColumn("off16").clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemalessTestData.createMapType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void emojisCharsDataTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("emojis_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("str", "String")
                .orderByColumn("off16").clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createDataWithEmojis(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size() / 2, ClickHouseTestHelpers.countRowsWithEmojis(chc, topic, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void decimalDataTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("decimal_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .tableName(topic)
                .column("num", "String")
                .column("decimal_14_2", "Decimal(14, 2)")
                .orderByColumn("num").clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createDecimalTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
        assertEquals(499700, ClickHouseTestHelpers.sumRows(chc, topic, "decimal_14_2", clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void nullableDecimalDataTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("nullable_decimal_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement()
                .tableName(topic)
                .column("num", "String")
                .column("decimal_14_2", "Nullable(Decimal(14, 2))")
                .orderByColumn("num").clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createNullableDecimalTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
        assertEquals(450180, ClickHouseTestHelpers.sumRows(chc, topic, "decimal_14_2", clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void overlappingDataTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("schemaless_primitive_types_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic).clusterConfig(clusterConfig).execute(chc);
        List<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);
        List<SinkRecord> smallerCollection = sr.subList(0, sr.size() / 2);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(smallerCollection);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }


    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    @SinceClickHouseVersion("24.10")
    public void jsonTypeTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = createClient(props);

        String topic = createTopicName("schemaless_json_table_test");
        ClickHouseTestHelpers.dropTable(chc, topic, clusterConfig);
        Map<String, Serializable> clientSettings = new HashMap<>();
        clientSettings.put(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("content", "JSON")
                .column("struct", "JSON")
                .orderByColumn("off16").settings(clientSettings).clusterConfig(clusterConfig).execute(chc);

        Collection<SinkRecord> sr = SchemalessTestData.createJSONType(topic, 1, 10);
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic, clusterConfig));
    }
}
