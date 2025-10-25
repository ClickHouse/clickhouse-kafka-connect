package com.clickhouse.kafka.connect.sink.db.helper;

import java.util.HashMap;
import java.util.Map;

public class ClickHouseTableSchema {
    private final Map<String, String> columnTypes;
    
    public ClickHouseTableSchema(Map<String, String> columnTypes) {
        this.columnTypes = new HashMap<>(columnTypes);
    }
    
    public String getColumnType(String columnName) {
        return columnTypes.get(columnName);
    }
    
    public Map<String, String> getAllColumnTypes() {
        return new HashMap<>(columnTypes);
    }
} 
