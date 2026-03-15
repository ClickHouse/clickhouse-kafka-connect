package com.clickhouse.kafka.connect.util;

import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.StructToJsonMap;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.gson.*;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

public final class DataJson {
  private DataJson() {}

  // Thread-safe: you can reuse this instance everywhere
  public static final Gson GSON = new GsonBuilder()
      .registerTypeHierarchyAdapter(Data.class, new DataSerializer())
      .registerTypeHierarchyAdapter(Struct.class, new StructSerializer())
      .disableHtmlEscaping() // optional: keeps URLs/slashes readable
      .create();

  public static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

  private static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(Struct.class, new JacksonStructSerializer());
    module.addSerializer(Data.class, new JacksonDataSerializer());
    mapper.registerModule(module);
    return mapper;
  }

  static final class JacksonStructSerializer extends com.fasterxml.jackson.databind.JsonSerializer<Struct> {
    @Override
    public void serialize(Struct struct, JsonGenerator gen, SerializerProvider provider) throws IOException {
      gen.writeStartObject();
      for (Field field : struct.schema().fields()) {
        gen.writeFieldName(field.name());
        Object value = struct.get(field);
        if (value == null) {
          gen.writeNull();
        } else {
          provider.defaultSerializeValue(value, gen);
        }
      }
      gen.writeEndObject();
    }
  }

  static final class JacksonDataSerializer extends com.fasterxml.jackson.databind.JsonSerializer<Data> {
    @Override
    public void serialize(Data data, JsonGenerator gen, SerializerProvider provider) throws IOException {
      if (data == null || data.getObject() == null) {
        gen.writeNull();
      } else if (data.getFieldType() == Schema.Type.STRUCT && data.getObject() instanceof Struct) {
        provider.defaultSerializeValue((Struct) data.getObject(), gen);
      } else {
        provider.defaultSerializeValue(data.getObject(), gen);
      }
    }
  }

  static final class DataSerializer implements JsonSerializer<Data> {
    @Override
    public JsonElement serialize(Data src, Type typeOfSrc, JsonSerializationContext ctx) {
      if (src == null) return JsonNull.INSTANCE;
      Object o = src.getObject();
      if (src.getFieldType() == Schema.Type.STRUCT && o instanceof Struct) {
        return ctx.serialize((Struct) o);
      }
      return ctx.serialize(o);
    }
  }

  static final class StructSerializer implements JsonSerializer<Struct> {
    @Override
    public JsonElement serialize(Struct s, Type t, JsonSerializationContext ctx) {
      Map<String, Data> typed = StructToJsonMap.toJsonMap(s); // field -> Data
      JsonObject obj = new JsonObject();
      for (Map.Entry<String, Data> e : typed.entrySet()) {
        obj.add(e.getKey(), ctx.serialize(e.getValue())); // recurse into DataSerializer
      }
      return obj;
    }
  }
}