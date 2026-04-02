package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseDeploymentType;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.sink.helper.SchemaTestData;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClickHouseSinkTaskMappingTest extends ClickHouseBase{

    private static final CreateTableStatement PRIMITIVE_TYPES_TABLE = new CreateTableStatement()
            .column("off16", "Int16").column("str", "String")
            .column("p_int8", "Int8").column("p_int16", "Int16").column("p_int32", "Int32")
            .column("p_int64", "Int64").column("p_float32", "Float32")
            .column("p_float64", "Float64").column("p_bool", "Bool")
            .orderByColumn("off16");

    private static final CreateTableStatement ARRAY_TYPES_TABLE = new CreateTableStatement()
            .column("off16", "Int16").column("arr", "Array(String)").column("arr_empty", "Array(String)")
            .column("arr_int8", "Array(Int8)").column("arr_int16", "Array(Int16)").column("arr_int32", "Array(Int32)")
            .column("arr_int64", "Array(Int64)").column("arr_float32", "Array(Float32)")
            .column("arr_float64", "Array(Float64)").column("arr_bool", "Array(Bool)")
            .orderByColumn("off16");

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void schemalessSingleTableMappingTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "mapping_table_test=table_mapping_test");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "mapping_table_test";
        String tableName = "table_mapping_test";
        ClickHouseTestHelpers.dropTable(chc, tableName, clusterConfig);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(tableName).clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, tableName, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void schemalessMultiDifferentTableMappingTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "mapping_table_test=table_mapping_test, mapping_table_test2=table_mapping_test2");
        ClickHouseHelperClient chc = createClient(props);

        String topic1 = "mapping_table_test";
        String topic2 = "mapping_table_test2";
        String tableName1 = "table_mapping_test";
        String tableName2 = "table_mapping_test2";
        ClickHouseTestHelpers.dropTable(chc, tableName1, clusterConfig);
        ClickHouseTestHelpers.dropTable(chc, tableName2, clusterConfig);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(tableName1).clusterConfig(clusterConfig).execute(chc);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(tableName2).clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr1 = SchemalessTestData.createPrimitiveTypes(topic1, 1);
        Collection<SinkRecord> sr2 = SchemalessTestData.createPrimitiveTypes(topic2, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr1);
        chst.put(sr2);
        chst.stop();
        assertEquals(sr1.size(), ClickHouseTestHelpers.countRows(chc, tableName1, clusterConfig));
        assertEquals(sr2.size(), ClickHouseTestHelpers.countRows(chc, tableName2, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void schemalessMultiSameTableMappingTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "mapping_table_test=table_mapping_test, mapping_table_test2=table_mapping_test");
        ClickHouseHelperClient chc = createClient(props);

        String topic1 = "mapping_table_test";
        String topic2 = "mapping_table_test2";
        String tableName = "table_mapping_test";
        ClickHouseTestHelpers.dropTable(chc, tableName, clusterConfig);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(tableName).clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr1 = SchemalessTestData.createPrimitiveTypes(topic1, 1);
        Collection<SinkRecord> sr2 = SchemalessTestData.createPrimitiveTypes(topic2, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr1);
        chst.put(sr2);
        chst.stop();
        assertEquals(sr1.size() + sr2.size(), ClickHouseTestHelpers.countRows(chc, tableName, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void schemalessMixedTableMappingTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "mapping_table_test=table_mapping_test, mapping_table_test2=table_mapping_test2");
        ClickHouseHelperClient chc = createClient(props);

        String topic1 = "mapping_table_test";
        String topic2 = "mapping_table_test2";
        String topic3 = "mapping_table_test3";
        String tableName1 = "table_mapping_test";
        String tableName2 = "table_mapping_test2";
        ClickHouseTestHelpers.dropTable(chc, tableName1, clusterConfig);
        ClickHouseTestHelpers.dropTable(chc, tableName2, clusterConfig);
        ClickHouseTestHelpers.dropTable(chc, topic3, clusterConfig);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(tableName1).clusterConfig(clusterConfig).execute(chc);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(tableName2).clusterConfig(clusterConfig).execute(chc);
        new CreateTableStatement(PRIMITIVE_TYPES_TABLE).tableName(topic3).clusterConfig(clusterConfig).execute(chc);
        Collection<SinkRecord> sr1 = SchemalessTestData.createPrimitiveTypes(topic1, 1);
        Collection<SinkRecord> sr2 = SchemalessTestData.createPrimitiveTypes(topic2, 1);
        Collection<SinkRecord> sr3 = SchemalessTestData.createPrimitiveTypes(topic3, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr1);
        chst.put(sr2);
        chst.put(sr3);
        chst.stop();
        assertEquals(sr1.size(), ClickHouseTestHelpers.countRows(chc, tableName1, clusterConfig));
        assertEquals(sr2.size(), ClickHouseTestHelpers.countRows(chc, tableName2, clusterConfig));
        assertEquals(sr3.size(), ClickHouseTestHelpers.countRows(chc, topic3, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void schemaArrayTypesSingleTableMappingTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "array_string_table_test=array_string_mapping_table_test");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "array_string_table_test";
        String tableName = "array_string_mapping_table_test";
        ClickHouseTestHelpers.dropTable(chc, tableName, clusterConfig);
        new CreateTableStatement(ARRAY_TYPES_TABLE).tableName(tableName).clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, tableName, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void schemaArrayTypesMultipleDifferentTableMappingTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "array_string_table_test=array_string_mapping_table_test, array_string_table_test2=array_string_mapping_table_test2");
        ClickHouseHelperClient chc = createClient(props);

        String topic1 = "array_string_table_test";
        String topic2 = "array_string_table_test2";
        String tableName1 = "array_string_mapping_table_test";
        String tableName2 = "array_string_mapping_table_test2";
        ClickHouseTestHelpers.dropTable(chc, tableName1, clusterConfig);
        ClickHouseTestHelpers.dropTable(chc, tableName2, clusterConfig);
        new CreateTableStatement(ARRAY_TYPES_TABLE).tableName(tableName1).clusterConfig(clusterConfig).execute(chc);
        new CreateTableStatement(ARRAY_TYPES_TABLE).tableName(tableName2).clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr1 = SchemaTestData.createArrayType(topic1, 1);
        Collection<SinkRecord> sr2 = SchemaTestData.createArrayType(topic2, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr1);
        chst.put(sr2);
        chst.stop();

        assertEquals(sr1.size(), ClickHouseTestHelpers.countRows(chc, tableName1, clusterConfig));
        assertEquals(sr2.size(), ClickHouseTestHelpers.countRows(chc, tableName2, clusterConfig));
    }


    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void schemaArrayTypesMultipleSameTableMappingTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "array_string_table_test=array_string_mapping_table_test, array_string_table_test2=array_string_mapping_table_test");
        ClickHouseHelperClient chc = createClient(props);

        String topic1 = "array_string_table_test";
        String topic2 = "array_string_table_test2";
        String tableName = "array_string_mapping_table_test";
        ClickHouseTestHelpers.dropTable(chc, tableName, clusterConfig);
        new CreateTableStatement(ARRAY_TYPES_TABLE).tableName(tableName).clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr1 = SchemaTestData.createArrayType(topic1, 1);
        Collection<SinkRecord> sr2 = SchemaTestData.createArrayType(topic2, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr1);
        chst.put(sr2);
        chst.stop();

        assertEquals(sr1.size() + sr2.size(), ClickHouseTestHelpers.countRows(chc, tableName, clusterConfig));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterConfigs")
    public void schemaArrayTypesMixedTableMappingTest(ClickHouseDeploymentType clusterConfig) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "array_string_table_test=array_string_mapping_table_test, array_string_table_test2=array_string_mapping_table_test2");
        ClickHouseHelperClient chc = createClient(props);

        String topic1 = "array_string_table_test";
        String topic2 = "array_string_table_test2";
        String topic3 = "array_string_table_test3";
        String tableName1 = "array_string_mapping_table_test";
        String tableName2 = "array_string_mapping_table_test2";
        ClickHouseTestHelpers.dropTable(chc, tableName1, clusterConfig);
        ClickHouseTestHelpers.dropTable(chc, tableName2, clusterConfig);
        ClickHouseTestHelpers.dropTable(chc, topic3, clusterConfig);
        new CreateTableStatement(ARRAY_TYPES_TABLE).tableName(tableName1).clusterConfig(clusterConfig).execute(chc);
        new CreateTableStatement(ARRAY_TYPES_TABLE).tableName(tableName2).clusterConfig(clusterConfig).execute(chc);
        new CreateTableStatement(ARRAY_TYPES_TABLE).tableName(topic3).clusterConfig(clusterConfig).execute(chc);
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr1 = SchemaTestData.createArrayType(topic1, 1);
        Collection<SinkRecord> sr2 = SchemaTestData.createArrayType(topic2, 1);
        Collection<SinkRecord> sr3 = SchemaTestData.createArrayType(topic3, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr1);
        chst.put(sr2);
        chst.put(sr3);
        chst.stop();

        assertEquals(sr1.size(), ClickHouseTestHelpers.countRows(chc, tableName1, clusterConfig));
        assertEquals(sr2.size(), ClickHouseTestHelpers.countRows(chc, tableName2, clusterConfig));
        assertEquals(sr3.size(), ClickHouseTestHelpers.countRows(chc, topic3, clusterConfig));
    }
}
