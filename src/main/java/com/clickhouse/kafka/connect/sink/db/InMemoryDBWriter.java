package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import com.clickhouse.kafka.connect.util.QueryIdentifier;

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
    public void doInsert(List<Record> records, QueryIdentifier queryId) {
        records.stream().forEach( r -> this.recordMap.put(r.getRecordOffsetContainer().getOffset(), r) );
    }

    @Override
    public void doInsert(List<Record> records, QueryIdentifier queryId, ErrorReporter errorReporter) {
        doInsert(records, queryId);
    }

    @Override
    public long recordsInserted() {
        return this.recordMap.size();
    }


}
