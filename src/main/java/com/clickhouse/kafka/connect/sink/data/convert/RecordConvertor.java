package com.clickhouse.kafka.connect.sink.data.convert;

import org.apache.kafka.connect.data.Schema;
import com.clickhouse.kafka.connect.sink.data.Record;
import org.apache.kafka.connect.sink.SinkRecord;

public interface RecordConvertor {
    Record convert(SinkRecord sinkRecord);
}
