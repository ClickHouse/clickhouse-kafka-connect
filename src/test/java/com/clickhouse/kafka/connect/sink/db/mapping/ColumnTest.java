package com.clickhouse.kafka.connect.sink.db.mapping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ColumnTest {

    @Test
    public void testExtractNullableColumn() {
        Column col = Column.extractColumn("columnName", "Nullable(String)", true);
        assertEquals(Type.STRING, col.getType());
    }

    @Test
    public void testExtractLowCardinalityColumn() {
        Column col = Column.extractColumn("columnName", "LowCardinality(String)", true);
        assertEquals(Type.STRING, col.getType());
    }

    @Test
    public void testExtractLowCardinalityNullableColumn() {
        Column col = Column.extractColumn("columnName", "LowCardinality(Nullable(String))", true);
        assertEquals(Type.STRING, col.getType());
    }

    @Test
    public void testExtractArrayOfLowCardinalityNullableColumn() {
        Column col = Column.extractColumn("columnName", "Array(LowCardinality(Nullable(String)))", true);
        assertEquals(Type.ARRAY, col.getType());
        assertEquals(Type.STRING, col.getSubType().getType());
    }

}