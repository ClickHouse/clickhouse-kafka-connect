package com.clickhouse.kafka.connect.sink.db.mapping;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseBase;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers.newDescriptor;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class TableTest extends ClickHouseBase {

    private static Column add(Table table, String name, String type) {
        Column column = Column.extractColumn(newDescriptor(name, type));
        table.addColumn(column);
        return column;
    }

    private static Table tableWithHeader(String... namesAndTypes) {
        Table table = new Table("default", "t");
        for (int i = 0; i < namesAndTypes.length; i += 2) {
            table.rememberColumnNameAndType(namesAndTypes[i], namesAndTypes[i + 1]);
        }
        table.composeRowBinaryWithNamesAndTypesHeader();
        return table;
    }

    // Creates a table (off16, Nullable(Date)), describes it and drops it, returning the description.
    private Table describeOff16DateTable(Map<String, String> props, String namePrefix) {
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String tableName = createTopicName(namePrefix);
        ClickHouseTestHelpers.dropTable(chc, tableName);
        new CreateTableStatement()
                .tableName(tableName)
                .column("off16", "Int16")
                .column("date_number", "Nullable(Date)")
                .engine("MergeTree").orderByColumn("off16").execute(chc);

        Table table = chc.describeTable(chc.getDatabase(), tableName);
        ClickHouseTestHelpers.dropTable(chc, tableName);
        return table;
    }

    @Test
    public void extractMapOfPrimitives() {
        Table table = new Table("default", "t");
        Column map = add(table, "map", "Map(String, Decimal(5))");
        assertEquals(Type.MAP, map.getType());
        assertNull(map.getMapValueType());

        add(table, "map.values", "Array(Decimal(5))");

        Column mapValueType = map.getMapValueType();
        assertEquals(Type.Decimal, mapValueType.getType());
        assertEquals(5, mapValueType.getPrecision());
    }

    @Test
    public void extractNullables() {
        Table table = describeOff16DateTable(getBaseProps(), "extract-table-test");
        assertNotNull(table);
        assertEquals(2, table.getRootColumnsList().size());
        assertEquals(3, table.getAllColumnsList().size());
    }

    @Test
    public void extractCommentV1() {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, "V1");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String tableName = createTopicName("extract-table-test");
        ClickHouseTestHelpers.dropTable(chc, tableName);
        new CreateTableStatement()
                .tableName(tableName)
                .column("c", "String COMMENT '\\\\'")
                .column("d", "String COMMENT '\\n'")
                .engine("MergeTree").orderByColumn("tuple()").execute(chc);

        Table table = chc.describeTable(chc.getDatabase(), tableName);
        assertNotNull(table);
        assertEquals(2, table.getRootColumnsList().size());
        ClickHouseTestHelpers.dropTable(chc, tableName);
    }

    @Test
    public void extractMapWithComplexValue() {
        Table table = new Table("default", "t");
        Column map = add(table, "map", "Map(String, Map(String, Decimal(5)))");
        assertEquals(Type.MAP, map.getType());
        assertNull(map.getMapValueType());

        add(table, "map.values", "Array(Map(String, Decimal(5)))");
        add(table, "map.values.values", "Array(Array(Decimal(5)))");

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
        Column map = add(table, "map", "Map(String, Map(String, Map(String, String)))");
        assertEquals(Type.MAP, map.getType());
        assertNull(map.getMapValueType());

        add(table, "map.values", "Array(Map(String, Map(String, String)))");
        add(table, "map.values.values", "Array(Array(Map(String, String)))");
        add(table, "map.values.values.values", "Array(Array(Array(String)))");

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
        Column tuple = add(table, "tuple", "Tuple(first String, second Decimal(5))");
        assertEquals(Type.TUPLE, tuple.getType());
        assertEquals(List.of(), tuple.getTupleFields());

        add(table, "tuple.first", "String");
        add(table, "tuple.second", "Decimal(5)");

        assertEquals(2, tuple.getTupleFields().size());
        assertEquals(List.of("tuple.first", "tuple.second"), tuple.getTupleFields().stream().map(Column::getName).collect(Collectors.toList()));
        assertEquals(List.of(Type.STRING, Type.Decimal), tuple.getTupleFields().stream().map(Column::getType).collect(Collectors.toList()));
        assertEquals(List.of(0, 5), tuple.getTupleFields().stream().map(Column::getPrecision).collect(Collectors.toList()));
    }

    @Test
    public void rowBinaryHeaderIsNullWhenNotCollected() {
        // Header stays null until the feature flag triggers composition during describe.
        Table table = new Table("default", "t");
        add(table, "off16", "Int16");
        add(table, "name", "String");

        assertNull(table.getRowBinaryWithNamesAndTypesHeader());
        assertEquals(2, table.getRootColumnsList().size());
    }

    @Test
    public void composeRowBinaryWithNamesAndTypesHeader() {
        Table table = tableWithHeader("a", "UInt8", "b", "String");

        // LEB128 count, then names, then types (each length-prefixed UTF-8).
        byte[] expected = new byte[] {
                0x02,
                0x01, 'a',
                0x01, 'b',
                0x05, 'U', 'I', 'n', 't', '8',
                0x06, 'S', 't', 'r', 'i', 'n', 'g'
        };
        assertArrayEquals(expected, table.getRowBinaryWithNamesAndTypesHeader());
    }

    @Test
    public void composeEmptyRowBinaryWithNamesAndTypesHeader() {
        assertArrayEquals(new byte[] {0x00}, tableWithHeader().getRowBinaryWithNamesAndTypesHeader());
    }

    @Test
    public void writeNamesAndTypesEmitsExactlyComposedHeaderWithoutTrailingBytes() throws Exception {
        // Any trailing padding from the over-estimated buffer would be read as row data by ClickHouse.
        Table table = tableWithHeader("a", "UInt8", "b", "String");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        table.writeNamesAndTypes(out);

        assertArrayEquals(table.getRowBinaryWithNamesAndTypesHeader(), out.toByteArray());
    }

    @Test
    public void describeCollectsRowBinaryHeaderWhenEnabled() throws Exception {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.USE_ROW_BINARY_WITH_NAMES_AND_TYPES, "true");

        Table table = describeOff16DateTable(props, "rbwnat-header-test");
        assertNotNull(table);
        byte[] header = table.getRowBinaryWithNamesAndTypesHeader();
        assertNotNull(header);

        HeaderContents decoded = decodeHeader(header);
        List<String> expectedNames = table.getRootColumnsList().stream()
                .map(Column::getName).collect(Collectors.toList());
        assertEquals(expectedNames, decoded.names);
        assertEquals(List.of("Int16", "Nullable(Date)"), decoded.types);
    }

    @Test
    public void describeDoesNotCollectRowBinaryHeaderWhenDisabled() {
        Table table = describeOff16DateTable(getBaseProps(), "rbwnat-header-disabled-test");
        assertNotNull(table);
        assertNull(table.getRowBinaryWithNamesAndTypesHeader());
    }

    private static final class HeaderContents {
        final List<String> names;
        final List<String> types;

        HeaderContents(List<String> names, List<String> types) {
            this.names = names;
            this.types = types;
        }
    }

    private static HeaderContents decodeHeader(byte[] header) throws Exception {
        InputStream in = new ByteArrayInputStream(header);
        int columns = BinaryStreamUtils.readVarInt(in);
        List<String> names = new ArrayList<>();
        List<String> types = new ArrayList<>();
        for (int i = 0; i < columns; i++) {
            names.add(BinaryStreamReader.readString(in));
        }
        for (int i = 0; i < columns; i++) {
            types.add(BinaryStreamReader.readString(in));
        }
        return new HeaderContents(names, types);
    }

    @Test
    public void extractTupleOfTupleOfTuple() {
        Table table = new Table("default", "t");
        Column tuple = add(table, "tuple", "Tuple(tuple Tuple(tuple Tuple(string String)))");
        assertEquals(Type.TUPLE, tuple.getType());
        assertEquals(List.of(), tuple.getTupleFields());

        add(table, "tuple.tuple", "Tuple(tuple Tuple(string String))");
        add(table, "tuple.tuple.tuple", "Tuple(string String)");
        add(table, "tuple.tuple.tuple.string", "String");

        assertEquals(1, tuple.getTupleFields().size());
        assertEquals(1, tuple.getTupleFields().get(0).getTupleFields().size());
        assertEquals(1, tuple.getTupleFields().get(0).getTupleFields().get(0).getTupleFields().size());

        Column stringColumn = tuple.getTupleFields().get(0).getTupleFields().get(0).getTupleFields().get(0);
        assertEquals("tuple.tuple.tuple.string", stringColumn.getName());
        assertEquals(Type.STRING, stringColumn.getType());
    }
}
