package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.SchemaTestData;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClickHouseSinkTaskMappingTest extends ClickHouseBase{
    @Test
    public void schemalessSingleTableMappingTest() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "mapping_table_test=table_mapping_test");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "mapping_table_test";
        String tableName = "table_mapping_test";
        ClickHouseTestHelpers.dropTable(chc, tableName);
        ClickHouseTestHelpers.createTable(chc, tableName, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();
        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, tableName));
    }

    @Test
    public void schemalessMultiDifferentTableMappingTest() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "mapping_table_test=table_mapping_test, mapping_table_test2=table_mapping_test2");
        ClickHouseHelperClient chc = createClient(props);

        String topic1 = "mapping_table_test";
        String topic2 = "mapping_table_test2";
        String tableName1 = "table_mapping_test";
        String tableName2 = "table_mapping_test2";
        ClickHouseTestHelpers.dropTable(chc, tableName1);
        ClickHouseTestHelpers.dropTable(chc, tableName2);
        ClickHouseTestHelpers.createTable(chc, tableName1, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        ClickHouseTestHelpers.createTable(chc, tableName2, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr1 = SchemalessTestData.createPrimitiveTypes(topic1, 1);
        Collection<SinkRecord> sr2 = SchemalessTestData.createPrimitiveTypes(topic2, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr1);
        chst.put(sr2);
        chst.stop();
        assertEquals(sr1.size(), ClickHouseTestHelpers.countRows(chc, tableName1));
        assertEquals(sr2.size(), ClickHouseTestHelpers.countRows(chc, tableName2));
    }

    @Test
    public void schemalessMultiSameTableMappingTest() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "mapping_table_test=table_mapping_test, mapping_table_test2=table_mapping_test");
        ClickHouseHelperClient chc = createClient(props);

        String topic1 = "mapping_table_test";
        String topic2 = "mapping_table_test2";
        String tableName = "table_mapping_test";
        ClickHouseTestHelpers.dropTable(chc, tableName);
        ClickHouseTestHelpers.createTable(chc, tableName, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr1 = SchemalessTestData.createPrimitiveTypes(topic1, 1);
        Collection<SinkRecord> sr2 = SchemalessTestData.createPrimitiveTypes(topic2, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr1);
        chst.put(sr2);
        chst.stop();
        assertEquals(sr1.size() + sr2.size(), ClickHouseTestHelpers.countRows(chc, tableName));
    }

    @Test
    public void schemalessMixedTableMappingTest() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "mapping_table_test=table_mapping_test, mapping_table_test2=table_mapping_test2");
        ClickHouseHelperClient chc = createClient(props);

        String topic1 = "mapping_table_test";
        String topic2 = "mapping_table_test2";
        String topic3 = "mapping_table_test3";
        String tableName1 = "table_mapping_test";
        String tableName2 = "table_mapping_test2";
        ClickHouseTestHelpers.dropTable(chc, tableName1);
        ClickHouseTestHelpers.dropTable(chc, tableName2);
        ClickHouseTestHelpers.dropTable(chc, topic3);
        ClickHouseTestHelpers.createTable(chc, tableName1, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        ClickHouseTestHelpers.createTable(chc, tableName2, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        ClickHouseTestHelpers.createTable(chc, topic3, "CREATE TABLE %s ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = MergeTree ORDER BY off16");
        Collection<SinkRecord> sr1 = SchemalessTestData.createPrimitiveTypes(topic1, 1);
        Collection<SinkRecord> sr2 = SchemalessTestData.createPrimitiveTypes(topic2, 1);
        Collection<SinkRecord> sr3 = SchemalessTestData.createPrimitiveTypes(topic3, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr1);
        chst.put(sr2);
        chst.put(sr3);
        chst.stop();
        assertEquals(sr1.size(), ClickHouseTestHelpers.countRows(chc, tableName1));
        assertEquals(sr2.size(), ClickHouseTestHelpers.countRows(chc, tableName2));
        assertEquals(sr3.size(), ClickHouseTestHelpers.countRows(chc, topic3));
    }

    @Test
    public void schemaArrayTypesSingleTableMappingTest() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "array_string_table_test=array_string_mapping_table_test");
        ClickHouseHelperClient chc = createClient(props);

        String topic = "array_string_table_test";
        String tableName = "array_string_mapping_table_test";
        ClickHouseTestHelpers.dropTable(chc, tableName);
        ClickHouseTestHelpers.createTable(chc, tableName, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), " +
                "`arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), " +
                "`arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr = SchemaTestData.createArrayType(topic, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, tableName));
    }

    @Test
    public void schemaArrayTypesMultipleDifferentTableMappingTest() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "array_string_table_test=array_string_mapping_table_test, array_string_table_test2=array_string_mapping_table_test2");
        ClickHouseHelperClient chc = createClient(props);

        String topic1 = "array_string_table_test";
        String topic2 = "array_string_table_test2";
        String tableName1 = "array_string_mapping_table_test";
        String tableName2 = "array_string_mapping_table_test2";
        ClickHouseTestHelpers.dropTable(chc, tableName1);
        ClickHouseTestHelpers.dropTable(chc, tableName2);
        ClickHouseTestHelpers.createTable(chc, tableName1, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), " +
                "`arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), " +
                "`arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
        ClickHouseTestHelpers.createTable(chc, tableName2, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), " +
                "`arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), " +
                "`arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr1 = SchemaTestData.createArrayType(topic1, 1);
        Collection<SinkRecord> sr2 = SchemaTestData.createArrayType(topic2, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr1);
        chst.put(sr2);
        chst.stop();

        assertEquals(sr1.size(), ClickHouseTestHelpers.countRows(chc, tableName1));
        assertEquals(sr2.size(), ClickHouseTestHelpers.countRows(chc, tableName2));
    }


    @Test
    public void schemaArrayTypesMultipleSameTableMappingTest() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "array_string_table_test=array_string_mapping_table_test, array_string_table_test2=array_string_mapping_table_test");
        ClickHouseHelperClient chc = createClient(props);

        String topic1 = "array_string_table_test";
        String topic2 = "array_string_table_test2";
        String tableName = "array_string_mapping_table_test";
        ClickHouseTestHelpers.dropTable(chc, tableName);
        ClickHouseTestHelpers.createTable(chc, tableName, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), " +
                "`arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), " +
                "`arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
        // https://github.com/apache/kafka/blob/trunk/connect/api/src/test/java/org/apache/kafka/connect/data/StructTest.java#L95-L98
        Collection<SinkRecord> sr1 = SchemaTestData.createArrayType(topic1, 1);
        Collection<SinkRecord> sr2 = SchemaTestData.createArrayType(topic2, 1);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr1);
        chst.put(sr2);
        chst.stop();

        assertEquals(sr1.size() + sr2.size(), ClickHouseTestHelpers.countRows(chc, tableName));
    }

    @Test
    public void schemaArrayTypesMixedTableMappingTest() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "array_string_table_test=array_string_mapping_table_test, array_string_table_test2=array_string_mapping_table_test2");
        ClickHouseHelperClient chc = createClient(props);

        String topic1 = "array_string_table_test";
        String topic2 = "array_string_table_test2";
        String topic3 = "array_string_table_test3";
        String tableName1 = "array_string_mapping_table_test";
        String tableName2 = "array_string_mapping_table_test2";
        ClickHouseTestHelpers.dropTable(chc, tableName1);
        ClickHouseTestHelpers.dropTable(chc, tableName2);
        ClickHouseTestHelpers.dropTable(chc, topic3);
        ClickHouseTestHelpers.createTable(chc, tableName1, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), " +
                "`arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), " +
                "`arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
        ClickHouseTestHelpers.createTable(chc, tableName2, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), " +
                "`arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), " +
                "`arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
        ClickHouseTestHelpers.createTable(chc, topic3, "CREATE TABLE %s ( `off16` Int16, `arr` Array(String), `arr_empty` Array(String), " +
                "`arr_int8` Array(Int8), `arr_int16` Array(Int16), `arr_int32` Array(Int32), `arr_int64` Array(Int64), `arr_float32` Array(Float32), " +
                "`arr_float64` Array(Float64), `arr_bool` Array(Bool)  ) Engine = MergeTree ORDER BY off16");
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

        assertEquals(sr1.size(), ClickHouseTestHelpers.countRows(chc, tableName1));
        assertEquals(sr2.size(), ClickHouseTestHelpers.countRows(chc, tableName2));
        assertEquals(sr3.size(), ClickHouseTestHelpers.countRows(chc, topic3));
    }
}
