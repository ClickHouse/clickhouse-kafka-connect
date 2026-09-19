package com.clickhouse.kafka.connect.sink.db;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.StructToJsonMap;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Type;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.util.Map;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

/**
 * Reproduction/verification for the Avro {@code decimal} logical type on a {@code fixed} base
 * (issue #798, from #726). Confluent's {@code AvroData} converts both {@code bytes}- and
 * {@code fixed}-based Avro decimals into a Kafka Connect {@link Decimal} logical field whose value
 * is a {@link BigDecimal}; the {@code fixed} case additionally carries a {@code connect.fixed.size}
 * parameter. This test models that Connect representation and drives it through the convert path
 * ({@link StructToJsonMap}) and the RowBinary write path, without needing a live ClickHouse.
 */
public class AvroDecimalLogicalTest {

    @Test
    public void decimalOnFixed_convertsToBigDecimal_andSerializesToDecimal_18_4() throws Exception {
        // What the Confluent Avro converter emits for {"type":"fixed","size":8,
        // "logicalType":"decimal","precision":18,"scale":4}: a Connect Decimal(scale=4) field
        // (BYTES base, logical name org.apache.kafka.connect.data.Decimal) + connect.fixed.size,
        // value = BigDecimal.
        Schema amountSchema =
                Decimal.builder(4).parameter("connect.fixed.size", "8").optional().build();
        Schema recordSchema =
                SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).field("amount", amountSchema).build();

        BigDecimal amount = new BigDecimal("0.0100"); // unscaled 100, scale 4 — matches fixture row 1
        Struct struct = new Struct(recordSchema).put("id", 1).put("amount", amount);

        // Convert side: the field must survive as a BigDecimal.
        Map<String, Data> data = StructToJsonMap.toJsonMap(struct);
        Data amountData = data.get("amount");
        assertNotNull(amountData, "amount field should be converted");
        assertInstanceOf(BigDecimal.class, amountData.getObject(), "decimal must convert to BigDecimal");
        assertEquals(amount, amountData.getObject());

        // Write side: into a ClickHouse Decimal(18, 4) column.
        Column col = Column.extractColumn("amount", "Decimal(18, 4)", false, false, false);
        assertEquals(Type.Decimal, col.getType());
        assertEquals(18, col.getPrecision());
        assertEquals(4, col.getScale());

        ClickHouseWriter writer = new ClickHouseWriter(new SinkTaskStatistics(0));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer.doWriteColValue(col, out, amountData, false);

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        BinaryStreamUtils.writeDecimal(expected, amount, 18, 4);
        assertArrayEquals(expected.toByteArray(), out.toByteArray(), "RowBinary decimal encoding must match");
    }
}
