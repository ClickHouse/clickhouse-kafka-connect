package com.clickhouse.kafka.connect.sink.data;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
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
                    jsonMap.put(fieldName, new Data(field.schema(), struct.getString(fieldName)));
                    break;
                case BYTES:
                    if (Decimal.LOGICAL_NAME.equals(schemaName)) {
                        jsonMap.put(fieldName, new Data(field.schema(), (BigDecimal) struct.get(fieldName)));
                    } else {
                        jsonMap.put(fieldName, new Data(field.schema(), struct.getBytes(fieldName)));
                    }
                    break;
                case INT32:
                    if (Date.LOGICAL_NAME.equals(schemaName) || Time.LOGICAL_NAME.equals(schemaName)) {
                        jsonMap.put(fieldName, new Data(field.schema(), (java.util.Date) struct.get(fieldName)));
                    } else {
                        jsonMap.put(fieldName, new Data(field.schema(), struct.getInt32(fieldName)));
                    }
                    break;
                case INT16:
                    jsonMap.put(fieldName, new Data(field.schema(), struct.getInt16(fieldName)));
                    break;
                case INT64:
                    if (Timestamp.LOGICAL_NAME.equals(schemaName)) {
                        jsonMap.put(fieldName, new Data(field.schema(), (java.util.Date) struct.get(fieldName)));
                    } else {
                        jsonMap.put(fieldName, new Data(field.schema(), struct.getInt64(fieldName)));
                    }
                    break;
                case FLOAT32:
                    jsonMap.put(fieldName, new Data(field.schema(), struct.getFloat32(fieldName)));
                    break;
                case FLOAT64:
                    jsonMap.put(fieldName, new Data(field.schema(), struct.getFloat64(fieldName)));
                    break;
                case BOOLEAN:
                    jsonMap.put(fieldName, new Data(field.schema(), struct.getBoolean(fieldName)));
                    break;
                case ARRAY:
                    List<Object> fieldArray = struct.getArray(fieldName);
                    if (fieldArray != null && !fieldArray.isEmpty() && fieldArray.get(0) instanceof Struct) {
                        // If Array contains list of Structs
                        List<Object> jsonArray = new ArrayList<>();
                        fieldArray.forEach(item -> {
                            jsonArray.add(toJsonMap((Struct) item));
                        });
                        jsonMap.put(fieldName, new Data(field.schema(), jsonArray));
                    } else {
                        jsonMap.put(fieldName, new Data(field.schema(), fieldArray));
                    }
                    break;
                case STRUCT:
                    jsonMap.put(fieldName, new Data(field.schema(), toJsonMap(struct.getStruct(fieldName))));
                    break;
                case MAP:
                    Map<Object, Object> fieldMap = new HashMap<>(struct.getMap(fieldName));
                    if (!fieldMap.isEmpty() && fieldMap.values().iterator().next() instanceof Struct) {
                        // Map values are `Struct`

                        for (Map.Entry<Object, Object> entry : fieldMap.entrySet()) {
                            entry.setValue(toJsonMap((Struct) entry.getValue()));
                        }
                    }
                    jsonMap.put(fieldName, new Data(field.schema(), fieldMap));
                    break;
                default:
                    jsonMap.put(fieldName, new Data(field.schema(), struct.get(fieldName)));
                    break;
            }
        }
        return jsonMap;
    }
}
