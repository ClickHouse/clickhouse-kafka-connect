package com.clickhouse.kafka.connect.sink.data.convert;

import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.data.SchemaType;
import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class StringRecordConvertor implements RecordConvertor {
    @Override
    public Record convert(SinkRecord sinkRecord) {
        if (sinkRecord.value() == null) {
            throw new DataException("Value was null for JSON conversion");
        }
        String topic = sinkRecord.topic();
        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();
        return new Record(SchemaType.STRING_SCHEMA, new OffsetContainer(topic, partition, offset), null, null, sinkRecord);
    }
}
