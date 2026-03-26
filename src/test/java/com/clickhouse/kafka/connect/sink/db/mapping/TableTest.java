package com.clickhouse.kafka.connect.sink.db.mapping;

import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseBase;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers.newDescriptor;
import static org.junit.jupiter.api.Assertions.*;

class TableTest extends ClickHouseBase {

    @Test
    public void extractMapOfPrimitives() {
        Table table = new Table("default", "t");

        Column map = Column.extractColumn(newDescriptor("map", "Map(String, Decimal(5))"));
        Column mapValues = Column.extractColumn(newDescriptor("map.values", "Array(Decimal(5))"));

        assertEquals(Type.MAP, map.getType());
        assertNull(map.getMapValueType());

        table.addColumn(map);
        table.addColumn(mapValues);

        Column mapValueType = map.getMapValueType();
        assertEquals(Type.Decimal, mapValueType.getType());
        assertEquals(5, mapValueType.getPrecision());
    }

    @Test
    public void extractNullables() {
        Map<String, String> props = createProps();
        ClickHouseHelperClient chc = createClient(props);

        String tableName = createTopicName("extract-table-test");
        ClickHouseTestHelpers.dropTable(chc, tableName);
        ClickHouseTestHelpers.createTable(chc, tableName, "CREATE TABLE `%s` (`off16` Int16, date_number Nullable(Date)) Engine = MergeTree ORDER BY off16");

        Table table = chc.describeTable(chc.getDatabase(), tableName);
        assertNotNull(table);
        assertEquals(table.getRootColumnsList().size(), 2);
        assertEquals(table.getAllColumnsList().size(), 3);
        ClickHouseTestHelpers.dropTable(chc, tableName);
    }

    @Test
    public void extractCommentV1() {
        Map<String, String> props = createProps();
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, "V1");
        ClickHouseHelperClient chc = createClient(props);

        String tableName = createTopicName("extract-table-test");
        ClickHouseTestHelpers.dropTable(chc, tableName);
        ClickHouseTestHelpers.createTable(chc, tableName, "CREATE TABLE `%s` ( c String COMMENT '\\\\', d String COMMENT '\\n'" +
                ")" +
                "ENGINE = MergeTree()" +
                "ORDER BY tuple()");

        Table table = chc.describeTable(chc.getDatabase(), tableName);
        assertNotNull(table);
        assertEquals(table.getRootColumnsList().size(), 2);
        ClickHouseTestHelpers.dropTable(chc, tableName);
    }

    @Test
    public void extractMapWithComplexValue() {
        Table table = new Table("default", "t");

        Column map = Column.extractColumn(newDescriptor("map", "Map(String, Map(String, Decimal(5)))"));
        Column mapValues = Column.extractColumn(newDescriptor("map.values", "Array(Map(String, Decimal(5)))"));
        Column mapValuesValues = Column.extractColumn(newDescriptor("map.values.values", "Array(Array(Decimal(5)))"));

        assertEquals(Type.MAP, map.getType());
        assertNull(map.getMapValueType());

        table.addColumn(map);
        table.addColumn(mapValues);
        table.addColumn(mapValuesValues);

        Column mapValueType = map.getMapValueType();
        assertEquals(Type.MAP, mapValueType.getType());
        assertEquals(Type.STRING, mapValueType.getMapKeyType());

        Column nestedMapValue = mapValueType.getMapValueType();
        assertEquals(Type.Decimal, nestedMapValue.getType());
        assertEquals(5, nestedMapValue.getPrecision());
    }

    @Test
    public void extractMapOfMapOfMapOfString() {
        Table table = new Table("default", "t");

        Column map = Column.extractColumn(newDescriptor("map", "Map(String, Map(String, Map(String, String)))"));
        Column mapValues = Column.extractColumn(newDescriptor("map.values", "Array(Map(String, Map(String, String)))"));
        Column mapValuesValues = Column.extractColumn(newDescriptor("map.values.values", "Array(Array(Map(String, String)))"));
        Column mapValuesValuesValues = Column.extractColumn(newDescriptor("map.values.values.values", "Array(Array(Array(String)))"));

        assertEquals(Type.MAP, map.getType());
        assertNull(map.getMapValueType());

        table.addColumn(map);
        table.addColumn(mapValues);
        table.addColumn(mapValuesValues);
        table.addColumn(mapValuesValuesValues);

        Column mapValueType = map.getMapValueType();
        assertEquals(Type.MAP, mapValueType.getType());
        assertEquals(Type.STRING, mapValueType.getMapKeyType());

        Column nestedMapValue = mapValueType.getMapValueType();
        assertEquals(Type.MAP, nestedMapValue.getType());
        assertEquals(Type.STRING, nestedMapValue.getMapKeyType());

        Column againNestedMapValue = nestedMapValue.getMapValueType();
        assertEquals(Type.STRING, againNestedMapValue.getType());
    }

    @Test
    public void extractTupleOfPrimitives() {
        Table table = new Table("default", "t");
        Column tuple = Column.extractColumn(newDescriptor("tuple", "Tuple(first String, second Decimal(5))"));
        Column tupleFirst = Column.extractColumn(newDescriptor("tuple.first", "String"));
        Column tupleSecond = Column.extractColumn(newDescriptor("tuple.second", "Decimal(5)"));

        assertEquals(Type.TUPLE, tuple.getType());
        assertEquals(List.of(), tuple.getTupleFields());

        table.addColumn(tuple);
        table.addColumn(tupleFirst);
        table.addColumn(tupleSecond);

        assertEquals(2, tuple.getTupleFields().size());
        assertEquals(List.of("tuple.first", "tuple.second"), tuple.getTupleFields().stream().map(Column::getName).collect(Collectors.toList()));
        assertEquals(List.of(Type.STRING, Type.Decimal), tuple.getTupleFields().stream().map(Column::getType).collect(Collectors.toList()));
        assertEquals(List.of(0, 5), tuple.getTupleFields().stream().map(Column::getPrecision).collect(Collectors.toList()));
    }

    @Test
    public void extractTupleOfTupleOfTuple() {
        Table table = new Table("default", "t");
        Column tuple = Column.extractColumn(newDescriptor("tuple", "Tuple(tuple Tuple(tuple Tuple(string String)))"));
        Column tupleTuple = Column.extractColumn(newDescriptor("tuple.tuple", "Tuple(tuple Tuple(string String))"));
        Column tupleTupleTuple = Column.extractColumn(newDescriptor("tuple.tuple.tuple", "Tuple(string String)"));
        Column tupleTupleTupleString = Column.extractColumn(newDescriptor("tuple.tuple.tuple.string", "String"));

        assertEquals(Type.TUPLE, tuple.getType());
        assertEquals(List.of(), tuple.getTupleFields());

        table.addColumn(tuple);
        table.addColumn(tupleTuple);
        table.addColumn(tupleTupleTuple);
        table.addColumn(tupleTupleTupleString);

        assertEquals(1, tuple.getTupleFields().size());
        assertEquals(1, tuple.getTupleFields().get(0).getTupleFields().size());
        assertEquals(1, tuple.getTupleFields().get(0).getTupleFields().get(0).getTupleFields().size());

        Column stringColumn = tuple.getTupleFields().get(0).getTupleFields().get(0).getTupleFields().get(0);
        assertEquals("tuple.tuple.tuple.string", stringColumn.getName());
        assertEquals(Type.STRING, stringColumn.getType());
    }
}
