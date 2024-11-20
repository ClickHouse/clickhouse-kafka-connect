package com.clickhouse.kafka.connect.sink.data.convert;

import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.data.SchemaType;
import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;

public class EmptyRecordConvertor extends RecordConvertor {
    @Override
    public Record doConvert(SinkRecord sinkRecord, String topic, String configurationDatabase) {
        String database = configurationDatabase;
        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();
        List<Field> fields = new ArrayList<>();
        return new Record(SchemaType.SCHEMA_LESS, new OffsetContainer(topic, partition, offset), fields, null, database, sinkRecord);
    }
}
