package com.clickhouse.kafka.connect.sink.data;

import com.clickhouse.kafka.connect.sink.db.ClickHouseWriter;
import org.apache.kafka.connect.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StructToJsonMap {

    private static final Logger LOGGER = LoggerFactory.getLogger(StructToJsonMap.class);
    public static Map<String, Data> toJsonMap(Struct struct) {
        if (struct == null) {
            return null;
        }
        Map<String, Data> jsonMap = new HashMap<String, Data>(0);
        List<Field> fields = struct.schema().fields();
        for (Field field : fields) {
            String fieldName = field.name();
            Schema.Type fieldType = field.schema().type();
            String schemaName = field.schema().name();
            LOGGER.debug(String.format("fieldName [%s] fieldType [%s] schemaName [%s]" , fieldName, fieldType.name(), schemaName));
            switch (fieldType) {
                case STRING:
                    jsonMap.put(fieldName, new Data(fieldType, struct.getString(fieldName)));
                    break;
                case BYTES:
                    jsonMap.put(fieldName, new Data(fieldType, struct.getBytes(fieldName)));
                    break;
                case INT32:
                    if (Date.LOGICAL_NAME.equals(schemaName) || Time.LOGICAL_NAME.equals(schemaName)) {
                        jsonMap.put(fieldName, new Data(fieldType, (java.util.Date) struct.get(fieldName)));
                    } else {
                        jsonMap.put(fieldName, new Data(fieldType, struct.getInt32(fieldName)));
                    }
                    break;
                case INT16:
                    jsonMap.put(fieldName, new Data(fieldType, struct.getInt16(fieldName)));
                    break;
                case INT64:
                    if (Timestamp.LOGICAL_NAME.equals(schemaName)) {
                        jsonMap.put(fieldName, new Data(fieldType, (java.util.Date) struct.get(fieldName)));
                    } else {
                        jsonMap.put(fieldName, new Data(fieldType, struct.getInt64(fieldName)));
                    }
                    break;
                case FLOAT32:
                    jsonMap.put(fieldName, new Data(fieldType, struct.getFloat32(fieldName)));
                    break;
                case FLOAT64:
                    jsonMap.put(fieldName, new Data(fieldType, struct.getFloat64(fieldName)));
                    break;
                case BOOLEAN:
                    jsonMap.put(fieldName, new Data(fieldType, struct.getBoolean(fieldName)));
                    break;
                case ARRAY:
                    List<Object> fieldArray = struct.getArray(fieldName);
                    if (fieldArray.size() > 0 && fieldArray.get(0) instanceof Struct) {
                        // If Array contains list of Structs
                        List<Object> jsonArray = new ArrayList<>();
                        fieldArray.forEach(item -> {
                            jsonArray.add(toJsonMap((Struct) item));
                        });
                        jsonMap.put(fieldName, new Data(fieldType, jsonArray));
                    } else {
                        jsonMap.put(fieldName, new Data(fieldType, fieldArray));
                    }
                    break;
                case STRUCT:
                    jsonMap.put(fieldName, new Data(fieldType, toJsonMap(struct.getStruct(fieldName))));
                    break;
                case MAP:
                    jsonMap.put(fieldName, new Data(fieldType, struct.getMap(fieldName)));
                    break;
                default:
                    jsonMap.put(fieldName, new Data(fieldType, struct.get(fieldName)));
                    break;
            }
        }
        return jsonMap;
    }
}
