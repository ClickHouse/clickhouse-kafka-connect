package com.clickhouse.kafka.connect.sink.dedup;

import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;

public class DeDup {

    private OffsetContainer currentOffset;
    private OffsetContainer previousOffset;
    private DeDupStrategy deDupStrategy;

    public DeDup(DeDupStrategy deDupStrategy, OffsetContainer currentOffset) {
        this.currentOffset = currentOffset;
        this.deDupStrategy = deDupStrategy;
        previousOffset = null;
    }

    public boolean isNew(int recordOffset) {
        return true;
    }
}
