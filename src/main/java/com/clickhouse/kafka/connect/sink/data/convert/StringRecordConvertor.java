package com.clickhouse.kafka.connect.sink.data.convert;

import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.data.SchemaType;
import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class StringRecordConvertor extends RecordConvertor {
    @Override
    public Record doConvert(SinkRecord sinkRecord, String topic,String configurationDatabase) {
        String database = configurationDatabase;
        if (sinkRecord.value() == null) {
            throw new DataException("Value was null for JSON conversion");
        }
        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();
        return new Record(SchemaType.STRING_SCHEMA, new OffsetContainer(topic, partition, offset), null, null, database, sinkRecord);
    }
}
