package com.clickhouse.kafka.connect.util;

import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.StructToJsonMap;
import com.google.gson.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

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