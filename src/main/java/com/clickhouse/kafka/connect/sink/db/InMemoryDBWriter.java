package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.data.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryDBWriter implements DBWriter {



    private Map<Long, Record> recordMap = null;


    public InMemoryDBWriter() {
        this.recordMap = new HashMap<>();
    }
    @Override
    public boolean start(ClickHouseSinkConfig csc) {
        return true;
    }

    @Override
    public void stop() {

    }

    @Override
    public void doInsert(List<Record> records) {
        records.stream().forEach( r -> this.recordMap.put(r.getRecordOffsetContainer().getOffset(), r) );
    }

    @Override
    public long recordsInserted() {
        return this.recordMap.size();
    }


}
