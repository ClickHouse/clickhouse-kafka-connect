package com.clickhouse.kafka.connect.sink.data.convert.gson;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Base64;

public class ByteBufferTypeAdapter implements JsonDeserializer<ByteBuffer>, JsonSerializer<ByteBuffer>{
    @Override
    public JsonElement serialize(ByteBuffer src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(Base64.getEncoder().encodeToString(src.array()));
    }

    @Override
    public ByteBuffer deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        byte[] bytes = Base64.getDecoder().decode(json.getAsString());
        return ByteBuffer.wrap(bytes);
    }
}
