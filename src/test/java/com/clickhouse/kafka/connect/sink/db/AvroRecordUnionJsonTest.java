package com.clickhouse.kafka.connect.sink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.StructToJsonMap;
import com.clickhouse.kafka.connect.util.DataJson;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

/**
 * Verifies how a record union {@code [TypeA, TypeB]} serializes to JSON for a ClickHouse {@code JSON}
 * column (issue #800, from #726). Confluent's Avro converter turns a union of two records into a
 * Connect struct named {@code io.confluent.connect.avro.Union} with one optional struct field per
 * branch (keyed by the branch record name). Per the maintainer, the value is kept in this tagged
 * form (which branch it was is preserved); ClickHouse's {@code JSON} type stores it as-is.
 *
 * <p>Asserted by parsing the output rather than string-matching, since field order in the tag object
 * is not significant.
 */
public class AvroRecordUnionJsonTest {

  private static final String AVRO_UNION_SCHEMA_NAME = "io.confluent.connect.avro.Union";
  private static final ObjectMapper JSON = new ObjectMapper();

  private static Schema recordUnionSchema() {
    Schema typeA =
        SchemaBuilder.struct().name("TypeA").field("label", Schema.STRING_SCHEMA).optional().build();
    Schema typeB =
        SchemaBuilder.struct().name("TypeB").field("count", Schema.INT32_SCHEMA).optional().build();
    Schema union =
        SchemaBuilder.struct()
            .name(AVRO_UNION_SCHEMA_NAME)
            .field("TypeA", typeA)
            .field("TypeB", typeB)
            .optional()
            .build();
    return SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).field("payload", union).build();
  }

  private static JsonNode serializePayload(Struct record) throws Exception {
    Map<String, Data> data = StructToJsonMap.toJsonMap(record);
    return JSON.readTree(DataJson.OBJECT_MAPPER.writeValueAsBytes(data.get("payload")));
  }

  @Test
  public void recordUnion_typeABranch_serializesTaggedByBranchName() throws Exception {
    Schema record = recordUnionSchema();
    Schema union = record.field("payload").schema();
    Schema typeA = union.field("TypeA").schema();
    Struct value =
        new Struct(record)
            .put("id", 1)
            .put("payload", new Struct(union).put("TypeA", new Struct(typeA).put("label", "foo")));

    JsonNode json = serializePayload(value);

    assertTrue(json.has("TypeA"), "payload should be tagged by branch name: " + json);
    assertEquals("foo", json.get("TypeA").get("label").asText());
  }

  @Test
  public void recordUnion_typeBBranch_serializesTaggedByBranchName() throws Exception {
    Schema record = recordUnionSchema();
    Schema union = record.field("payload").schema();
    Schema typeB = union.field("TypeB").schema();
    Struct value =
        new Struct(record)
            .put("id", 2)
            .put("payload", new Struct(union).put("TypeB", new Struct(typeB).put("count", 42)));

    JsonNode json = serializePayload(value);

    assertTrue(json.has("TypeB"), "payload should be tagged by branch name: " + json);
    assertEquals(42, json.get("TypeB").get("count").asInt());
  }
}
