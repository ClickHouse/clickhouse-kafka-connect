package com.clickhouse.kafka.connect.sink.data;

import org.apache.kafka.connect.data.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StructToJsonMap {
    public static Map<String, Object> toJsonMap(Struct struct) {
        if (struct == null) {
            return null;
        }
        Map<String, Object> jsonMap = new HashMap<String, Object>(0);
        List<Field> fields = struct.schema().fields();
        for (Field field : fields) {
            String fieldName = field.name();
            Schema.Type fieldType = field.schema().type();
            String schemaName = field.schema().name();
            switch (fieldType) {
                case STRING:
                    jsonMap.put(fieldName, struct.getString(fieldName));
                    break;
                case INT32:
                    if (Date.LOGICAL_NAME.equals(schemaName) || Time.LOGICAL_NAME.equals(schemaName)) {
                        jsonMap.put(fieldName, (java.util.Date) struct.get(fieldName));
                    } else {
                        jsonMap.put(fieldName, struct.getInt32(fieldName));
                    }
                    break;
                case INT16:
                    jsonMap.put(fieldName, struct.getInt16(fieldName));
                    break;
                case INT64:
                    if (Timestamp.LOGICAL_NAME.equals(schemaName)) {
                        jsonMap.put(fieldName, (java.util.Date) struct.get(fieldName));
                    } else {
                        jsonMap.put(fieldName, struct.getInt64(fieldName));
                    }
                    break;
                case FLOAT32:
                    jsonMap.put(fieldName, struct.getFloat32(fieldName));
                    break;
                case FLOAT64:
                    jsonMap.put(fieldName, struct.getFloat64(fieldName));
                    break;
                case BOOLEAN:
                    jsonMap.put(fieldName, struct.getBoolean(fieldName));
                    break;
                case ARRAY:
                    List<Object> fieldArray = struct.getArray(fieldName);
                    if (fieldArray.get(0) instanceof Struct) {
                        // If Array contains list of Structs
                        List<Object> jsonArray = new ArrayList<>();
                        fieldArray.forEach(item -> {
                            jsonArray.add(toJsonMap((Struct) item));
                        });
                        jsonMap.put(fieldName, jsonArray);
                    } else {
                        jsonMap.put(fieldName, fieldArray);
                    }
                    break;
                case STRUCT:
                    jsonMap.put(fieldName, toJsonMap(struct.getStruct(fieldName)));
                    break;
                default:
                    jsonMap.put(fieldName, struct.get(fieldName));
                    break;
            }
        }
        return jsonMap;
    }
}
