package com.clickhouse.kafka.connect.sink.db.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

public class Table {
    private String name;
    private List<Column> columnsList = null;
    private Map<String, Column> columnsMap = null;

    public Table(String name) {
        this.name = name;
        this.columnsList = new ArrayList<>();
        this.columnsMap = new HashMap<>();
    }

    public String getName() {
        return name;
    }

    public void addColumn(Column column) {
        columnsList.add(column);
        columnsMap.put(column.getName(), column);
    }

    public Column getColumnByName(String name) {
        return columnsMap.get(name);
    }

    public List<Column> getColumns() { return columnsList; }
}
