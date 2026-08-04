package com.clickhouse.kafka.connect.sink.db;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.StructToJsonMap;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Type;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

/**
 * Reproduction/verification for Avro nullable multi-type unions mapping to a ClickHouse {@code
 * Variant} (issue #799, from #726). Confluent's Avro converter turns the non-null branches of a
 * multi-branch union into a Connect struct named {@code io.confluent.connect.avro.Union} with one
 * optional field per branch, and the {@code null} branch makes the whole field optional. These tests
 * drive that Connect representation through the convert path ({@link StructToJsonMap}) and the
 * RowBinary Variant write path, without a live ClickHouse.
 *
 * <p>Coverage per review feedback — not all unions fit a {@code Variant}:
 *
 * <ul>
 *   <li>{@code [null, string, int]} → {@code Variant(String, Int32)} (string / int / all-null values)
 *   <li>{@code [null, string, int, boolean]} → {@code Variant(String, Int32, Bool)} (bool value)
 *   <li>{@code [null, int, long]} → falls back to {@code Nullable(String)} (suspicious same-group
 *       numeric types ClickHouse rejects inside a Variant), not a {@code Variant}
 * </ul>
 *
 * <p>ClickHouse assigns Variant global discriminators in sorted type order, so for {@code
 * Variant(String, Int32)} the order is {@code [Int32, String]} (Int32 = 0, String = 1) and for {@code
 * Variant(String, Int32, Bool)} it is {@code [Bool, Int32, String]} (Bool = 0, Int32 = 1, String = 2).
 */
public class AvroNullableUnionVariantTest {

  // Confluent's Avro converter names multi-branch union structs with this schema name.
  private static final String AVRO_UNION_SCHEMA_NAME = "io.confluent.connect.avro.Union";

  private static Schema unionSchema(SchemaBuilder builder) {
    return builder.name(AVRO_UNION_SCHEMA_NAME).optional().build();
  }

  private static final Schema STRING_INT_UNION =
      unionSchema(
          SchemaBuilder.struct()
              .field("string", Schema.OPTIONAL_STRING_SCHEMA)
              .field("int", Schema.OPTIONAL_INT32_SCHEMA));

  private static final Schema STRING_INT_BOOL_UNION =
      unionSchema(
          SchemaBuilder.struct()
              .field("string", Schema.OPTIONAL_STRING_SCHEMA)
              .field("int", Schema.OPTIONAL_INT32_SCHEMA)
              .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA));

  /** Serialize a single "mixed" union value into a Variant column and return the bytes. */
  private static byte[] writeMixed(Schema unionSchema, String clickHouseType, Struct unionValue)
      throws Exception {
    Schema recordSchema =
        SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).field("mixed", unionSchema).build();
    Struct record = new Struct(recordSchema).put("id", 1).put("mixed", unionValue);
    Map<String, Data> data = StructToJsonMap.toJsonMap(record);

    Column col = Column.extractColumn("mixed", clickHouseType, false, false, false);
    assertEquals(Type.VARIANT, col.getType());

    ClickHouseWriter writer = new ClickHouseWriter(new SinkTaskStatistics(0));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.doWriteColValue(col, out, data.get("mixed"), false);
    return out.toByteArray();
  }

  @Test
  public void stringBranch_writesStringDiscriminatorAndValue() throws Exception {
    byte[] actual =
        writeMixed(
            STRING_INT_UNION, "Variant(String, Int32)", new Struct(STRING_INT_UNION).put("string", "hello"));

    ByteArrayOutputStream expected = new ByteArrayOutputStream();
    BinaryStreamUtils.writeUnsignedInt8(expected, 1); // String discriminator
    BinaryStreamUtils.writeString(expected, "hello".getBytes(StandardCharsets.UTF_8));
    assertArrayEquals(expected.toByteArray(), actual);
  }

  @Test
  public void intBranch_writesIntDiscriminatorAndValue() throws Exception {
    byte[] actual =
        writeMixed(STRING_INT_UNION, "Variant(String, Int32)", new Struct(STRING_INT_UNION).put("int", 42));

    ByteArrayOutputStream expected = new ByteArrayOutputStream();
    BinaryStreamUtils.writeUnsignedInt8(expected, 0); // Int32 discriminator
    BinaryStreamUtils.writeInt32(expected, 42);
    assertArrayEquals(expected.toByteArray(), actual);
  }

  @Test
  public void allBranchesNull_writesNullDiscriminator() throws Exception {
    // A union struct present but with every branch null → the ClickHouse "null" discriminator (255).
    byte[] actual =
        writeMixed(STRING_INT_UNION, "Variant(String, Int32)", new Struct(STRING_INT_UNION));

    ByteArrayOutputStream expected = new ByteArrayOutputStream();
    BinaryStreamUtils.writeUnsignedInt8(expected, 255);
    assertArrayEquals(expected.toByteArray(), actual);
  }

  @Test
  public void booleanBranch_inThreeTypeVariant_writesBoolDiscriminatorAndValue() throws Exception {
    byte[] actual =
        writeMixed(
            STRING_INT_BOOL_UNION,
            "Variant(String, Int32, Bool)",
            new Struct(STRING_INT_BOOL_UNION).put("boolean", true));

    ByteArrayOutputStream expected = new ByteArrayOutputStream();
    BinaryStreamUtils.writeUnsignedInt8(expected, 0); // Bool discriminator (sorted first)
    BinaryStreamUtils.writeBoolean(expected, true);
    assertArrayEquals(expected.toByteArray(), actual);
  }

  @Test
  public void suspiciousNumericUnion_fallsBackToNullableString_notVariant() {
    // ClickHouse rejects Variant(Int32, Int64) unless allow_suspicious_variant_types is set, so the
    // connector maps such unions to Nullable(String) rather than a Variant — not everything is a Variant.
    Schema union =
        unionSchema(
            SchemaBuilder.struct()
                .field("int", Schema.OPTIONAL_INT32_SCHEMA)
                .field("long", Schema.OPTIONAL_INT64_SCHEMA));
    assertEquals("Nullable(String)", Column.connectTypeToClickHouseType(union));
  }
}
