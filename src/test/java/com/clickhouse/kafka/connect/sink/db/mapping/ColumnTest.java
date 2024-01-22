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
    public void extractDecimal_14_5() {
        Column col = Column.extractColumn("columnName", "Decimal(14,2)", true, false);
        assertEquals(Type.Decimal, col.getType());
        assertEquals(14, col.getPrecision());
        assertEquals(2, col.getScale());
    }
}