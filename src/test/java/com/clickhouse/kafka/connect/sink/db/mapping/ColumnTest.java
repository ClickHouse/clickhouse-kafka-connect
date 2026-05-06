package com.clickhouse.kafka.connect.sink.db.mapping;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers.col;
import static com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers.newDescriptor;
import static org.junit.jupiter.api.Assertions.*;

class ColumnTest {

    @Test
    public void extractNullableColumn() {
        Column col = Column.extractColumn(newDescriptor("Nullable(String)"));
        assertEquals(Type.STRING, col.getType());
    }

    @Test
    public void extractLowCardinalityColumn() {
        Column col = Column.extractColumn(newDescriptor("LowCardinality(String)"));
        assertEquals(Type.STRING, col.getType());
    }

    @Test
    public void extractLowCardinalityNullableColumn() {
        Column col = Column.extractColumn(newDescriptor("LowCardinality(Nullable(String))"));
        assertEquals(Type.STRING, col.getType());
    }

    @Test
    public void extractSimpleAggregateFunctionColumn() {
        Column col = Column.extractColumn(newDescriptor("SimpleAggregateFunction(sum, Int64)"));
        assertEquals(Type.INT64, col.getType());
    }

    @Test
    public void extractSimpleAggregateFunctionNullableColumn() {
        Column col = Column.extractColumn(newDescriptor("SimpleAggregateFunction(sum, Nullable(Int64))"));
        assertEquals(Type.INT64, col.getType());
    }

    @Test
    public void extractArrayOfLowCardinalityNullableColumn() {
        Column col = Column.extractColumn(newDescriptor("Array(LowCardinality(Nullable(String)))"));
        assertEquals(Type.ARRAY, col.getType());
        assertEquals(Type.STRING, col.getArrayType().getType());

        assertNull(col.getMapKeyType());
        assertNull(col.getMapValueType());
        assertNull(col.getTupleFields());
    }

    @Test
    public void extractDecimalNullableColumn() {
        Column col = Column.extractColumn(newDescriptor("Nullable(Decimal)"));
        assertEquals(Type.Decimal, col.getType());
    }

    @Test
    public void extractDecimal_default() {
        Column col = Column.extractColumn(newDescriptor("Decimal"));
        assertEquals(Type.Decimal, col.getType());
        assertEquals(10, col.getPrecision());
        assertEquals(0, col.getScale());
    }

    @Test
    public void extractDecimal_default_5() {
        Column col = Column.extractColumn(newDescriptor("Decimal(5)"));
        assertEquals(Type.Decimal, col.getType());
        assertEquals(5, col.getPrecision());
        assertEquals(0, col.getScale());
    }

    @Test
    public void extractDecimal_sized_5() {
        Column col = Column.extractColumn(newDescriptor("Decimal256(5)"));
        assertEquals(Type.Decimal, col.getType());
        assertEquals(76, col.getPrecision());
        assertEquals(5, col.getScale());
    }

    @Test
    public void extractDecimal_14_2() {
        Column col = Column.extractColumn(newDescriptor("Decimal(14, 2)"));
        assertEquals(Type.Decimal, col.getType());
        assertEquals(14, col.getPrecision());
        assertEquals(2, col.getScale());
    }

    @Test
    public void extractArrayOfDecimalNullable_5() {
        Column col = Column.extractColumn(newDescriptor("Array(Nullable(Decimal(5)))"));
        assertEquals(Type.ARRAY, col.getType());

        assertNull(col.getMapKeyType());
        assertNull(col.getMapValueType());
        assertNull(col.getTupleFields());

        Column subType = col.getArrayType();
        assertEquals(Type.Decimal, subType.getType());
        assertEquals(5, subType.getPrecision());
        assertTrue(subType.isNullable());
    }

    @Test
    public void extractArrayOfArrayOfArrayOfString() {
        Column col = Column.extractColumn(newDescriptor("Array(Array(Array(String)))"));
        assertEquals(Type.ARRAY, col.getType());

        assertNull(col.getMapKeyType());
        assertNull(col.getMapValueType());
        assertNull(col.getTupleFields());

        Column subType = col.getArrayType();
        assertEquals(Type.ARRAY, subType.getType());

        Column subSubType = subType.getArrayType();
        assertEquals(Type.ARRAY, subSubType.getType());

        Column subSubSubType = subSubType.getArrayType();
        assertEquals(Type.STRING, subSubSubType.getType());
        assertNull(subSubSubType.getArrayType());
    }

    @Test
    public void extractMapOfPrimitives() {
        Column col = Column.extractColumn(newDescriptor("Map(String, Decimal(5)"));
        assertEquals(Type.MAP, col.getType());

        assertEquals(Type.STRING, col.getMapKeyType());

        assertNull(col.getArrayType());
        assertNull(col.getMapValueType());
        assertNull(col.getTupleFields());
    }

    @Test
    public void extractTupleOfPrimitives() {
        Column col = Column.extractColumn(newDescriptor("Tuple(first String, second Decimal(5))"));
        assertEquals(Type.TUPLE, col.getType());

        assertNull(col.getArrayType());
        assertNull(col.getMapValueType());
        assertEquals(List.of(), col.getTupleFields());
    }

    @Test
    public void extractVariantOfPrimitives() {
        Column col = Column.extractColumn(newDescriptor("Variant(String, Decimal256(5), Decimal(14, 2), Decimal(5))"));
        assertEquals(Type.VARIANT, col.getType());
        assertEquals(4, col.getVariantTypes().size());

        List<Column> expectedSubtypes = List.of(
                col(Type.STRING),
                col(Type.Decimal, 76, 5),
                col(Type.Decimal, 14, 2),
                col(Type.Decimal, 5, 0)
        );

        for (int i = 0; i < expectedSubtypes.size(); i++) {
            Column expectedSubtype = expectedSubtypes.get(i);
            Column actualSubtype = col.getVariantTypes().get(i).getT1();

            assertEquals(expectedSubtype.getType(), actualSubtype.getType());
            assertEquals(expectedSubtype.getPrecision(), actualSubtype.getPrecision());
            assertEquals(expectedSubtype.getScale(), actualSubtype.getScale());
        }
    }

    @Test
    public void extractEnumOfPrimitives() {
        Column col = Column.extractColumn(newDescriptor("Enum8('a, valid' = 1, 'b' = 2)"));
        assertEquals(Type.Enum8, col.getType());
        assertEquals(2, col.getEnumValues().size());
        assertTrue(col.getEnumValues().containsKey("a, valid"));
        assertTrue(col.getEnumValues().containsKey("b"));
    }

    // --- isUnionSchema detection tests ---

    @Test
    public void isUnionSchema_avroUnion() {
        Schema union = SchemaBuilder.struct()
                .name(Column.AVRO_UNION_SCHEMA_NAME)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("int", Schema.OPTIONAL_INT32_SCHEMA)
                .optional()
                .build();
        assertTrue(Column.isUnionSchema(union));
    }

    @Test
    public void isUnionSchema_protobufOneof() {
        Schema union = SchemaBuilder.struct()
                .name("io.confluent.connect.protobuf.Union.content")
                .field("user_info", Schema.OPTIONAL_STRING_SCHEMA)
                .field("product_info", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        assertTrue(Column.isUnionSchema(union));
    }

    @Test
    public void isUnionSchema_generalizedUnion() {
        Schema union = SchemaBuilder.struct()
                .name("connect_union_0")
                .parameter(Column.CONNECT_UNION_PARAMETER, "connect_union_0")
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("int", Schema.OPTIONAL_INT32_SCHEMA)
                .optional()
                .build();
        assertTrue(Column.isUnionSchema(union));
    }

    @Test
    public void isUnionSchema_connectUnionParameter() {
        Schema union = SchemaBuilder.struct()
                .parameter(Column.CONNECT_UNION_PARAMETER, "some_annotation")
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("int", Schema.OPTIONAL_INT32_SCHEMA)
                .optional()
                .build();
        assertTrue(Column.isUnionSchema(union));
    }

    @Test
    public void isUnionSchema_realStruct_notDetected() {
        Schema struct = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();
        assertFalse(Column.isUnionSchema(struct));
    }

    @Test
    public void isUnionSchema_primitiveType_notDetected() {
        assertFalse(Column.isUnionSchema(Schema.STRING_SCHEMA));
    }

    // --- connectTypeToClickHouseType union mapping tests ---

    @Test
    public void unionStringBytes_collapsesToNullableString() {
        Schema union = SchemaBuilder.struct()
                .name(Column.AVRO_UNION_SCHEMA_NAME)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("bytes", Schema.OPTIONAL_BYTES_SCHEMA)
                .optional()
                .build();
        assertEquals("Nullable(String)", Column.connectTypeToClickHouseType(union));
    }

    @Test
    public void unionStringInt_mapsToVariant() {
        Schema union = SchemaBuilder.struct()
                .name(Column.AVRO_UNION_SCHEMA_NAME)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("int", Schema.OPTIONAL_INT32_SCHEMA)
                .optional()
                .build();
        assertEquals("Variant(String, Int32)", Column.connectTypeToClickHouseType(union));
    }

    @Test
    public void unionStringIntBoolean_mapsToVariant() {
        Schema union = SchemaBuilder.struct()
                .name(Column.AVRO_UNION_SCHEMA_NAME)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("int", Schema.OPTIONAL_INT32_SCHEMA)
                .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .optional()
                .build();
        assertEquals("Variant(String, Int32, Bool)", Column.connectTypeToClickHouseType(union));
    }

    @Test
    public void unionSuspiciousNumericTypes_fallsBackToString() {
        // Variant(Int32, Int64) is rejected by ClickHouse unless allow_suspicious_variant_types
        Schema union = SchemaBuilder.struct()
                .name(Column.AVRO_UNION_SCHEMA_NAME)
                .field("int", Schema.OPTIONAL_INT32_SCHEMA)
                .field("long", Schema.OPTIONAL_INT64_SCHEMA)
                .optional()
                .build();
        assertEquals("Nullable(String)", Column.connectTypeToClickHouseType(union));
    }

    @Test
    public void unionSuspiciousNumericWithString_fallsBackToString() {
        Schema union = SchemaBuilder.struct()
                .name(Column.AVRO_UNION_SCHEMA_NAME)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("int", Schema.OPTIONAL_INT32_SCHEMA)
                .field("long", Schema.OPTIONAL_INT64_SCHEMA)
                .optional()
                .build();
        assertEquals("Nullable(String)", Column.connectTypeToClickHouseType(union));
    }

    @Test
    public void protobufOneof_stringBytes_collapsesToNullableString() {
        // Protobuf oneof { string url = 1; bytes raw = 2; } — field names differ from Avro
        Schema union = SchemaBuilder.struct()
                .name("io.confluent.connect.protobuf.Union.image")
                .field("url", Schema.OPTIONAL_STRING_SCHEMA)
                .field("raw", Schema.OPTIONAL_BYTES_SCHEMA)
                .optional()
                .build();
        assertEquals("Nullable(String)", Column.connectTypeToClickHouseType(union));
    }

    @Test
    public void realStruct_withStructToJson_mapsToJSON() {
        Schema struct = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();
        assertEquals("Nullable(JSON)", Column.connectTypeToClickHouseType(struct, true));
    }

    @Test
    public void realStruct_withoutFlag_throws() {
        Schema struct = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();
        assertThrows(SchemaTypeInferenceException.class,
                () -> Column.connectTypeToClickHouseType(struct, false));
    }

    @Test
    public void unionEmptyFields_fallsBackToNullableString() {
        Schema union = SchemaBuilder.struct()
                .name(Column.AVRO_UNION_SCHEMA_NAME)
                .optional()
                .build();
        assertEquals("Nullable(String)", Column.connectTypeToClickHouseType(union));
    }

    @Test
    public void unionVariant_notWrappedInNullable() {
        Schema union = SchemaBuilder.struct()
                .name(Column.AVRO_UNION_SCHEMA_NAME)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .optional()
                .build();
        String result = Column.connectTypeToClickHouseType(union);
        assertEquals("Variant(String, Bool)", result);
        assertFalse(result.startsWith("Nullable("));
    }
}

