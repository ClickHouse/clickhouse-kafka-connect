package com.clickhouse.kafka.connect.sink.db.mapping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ColumnTest {

    @Test
    public void extractNullableColumn() {
        Column col = Column.extractColumn("columnName", "Nullable(String)", true, false);
        assertEquals(Type.STRING, col.getType());
    }

    @Test
    public void extractLowCardinalityColumn() {
        Column col = Column.extractColumn("columnName", "LowCardinality(String)", true, false);
        assertEquals(Type.STRING, col.getType());
    }

    @Test
    public void extractLowCardinalityNullableColumn() {
        Column col = Column.extractColumn("columnName", "LowCardinality(Nullable(String))", true, false);
        assertEquals(Type.STRING, col.getType());
    }

    @Test
    public void extractArrayOfLowCardinalityNullableColumn() {
        Column col = Column.extractColumn("columnName", "Array(LowCardinality(Nullable(String)))", true, false);
        assertEquals(Type.ARRAY, col.getType());
        assertEquals(Type.STRING, col.getSubType().getType());
    }

    @Test
    public void extractDecimalNullableColumn() {
        Column col = Column.extractColumn("columnName", "Nullable(Decimal)", true, false);
        assertEquals(Type.Decimal, col.getType());
    }

    @Test
    public void extractDecimal_default() {
        Column col = Column.extractColumn("columnName", "Decimal", true, false);
        assertEquals(Type.Decimal, col.getType());
        assertEquals(10, col.getPrecision());
        assertEquals(0, col.getScale());
    }

    @Test
    public void extractDecimal_default_5() {
        Column col = Column.extractColumn("columnName", "Decimal(5)", true, false);
        assertEquals(Type.Decimal, col.getType());
        assertEquals(5, col.getPrecision());
        assertEquals(0, col.getScale());
    }

    @Test
    public void extractDecimal_sized_5() {
        Column col = Column.extractColumn("columnName", "Decimal256(5)", true, false);
        assertEquals(Type.Decimal, col.getType());
        assertEquals(76, col.getPrecision());
        assertEquals(5, col.getScale());
    }

    @Test
    public void extractDecimal_14_2() {
        Column col = Column.extractColumn("columnName", "Decimal(14,2)", true, false);
        assertEquals(Type.Decimal, col.getType());
        assertEquals(14, col.getPrecision());
        assertEquals(2, col.getScale());
    }

    @Test
    public void extractArrayOfDecimalNullable_5() {
        Column col = Column.extractColumn("columnName", "Array(Nullable(Decimal(5)))", true, false);
        assertEquals(Type.ARRAY, col.getType());

        Column subType = col.getSubType();
        assertEquals(Type.Decimal, subType.getType());
        assertEquals(5, subType.getPrecision());
        assertTrue(subType.isNullable());
    }

    @Test
    public void extractMapOfPrimitives() {
        Column col = Column.extractColumn("columnName", "Map(String, Decimal(5))", true, false);
        assertEquals(Type.MAP, col.getType());

        assertNull(col.getSubType());
        assertEquals(Type.STRING, col.getMapKeyType());

        Column mapValueType = col.getMapValueType();
        assertEquals(Type.Decimal, mapValueType.getType());
        assertEquals(5, mapValueType.getPrecision());
    }

    @Test
    public void extractMapWithComplexValue() {
        Column col = Column.extractColumn("columnName", "Map(String, Map(String, Decimal(5)))", true, false);
        assertEquals(Type.MAP, col.getType());

        assertNull(col.getSubType());
        assertEquals(Type.STRING, col.getMapKeyType());

        Column mapValueType = col.getMapValueType();
        assertEquals(Type.MAP, mapValueType.getType());
        assertEquals(Type.STRING, mapValueType.getMapKeyType());

        Column nestedMapValue = mapValueType.getMapValueType();
        assertEquals(Type.Decimal, nestedMapValue.getType());
        assertEquals(5, nestedMapValue.getPrecision());

    }
}