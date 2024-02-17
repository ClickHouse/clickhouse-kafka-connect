package com.clickhouse.kafka.connect.sink.data.convert;

import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.data.SchemaType;
import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class StringRecordConvertor implements RecordConvertor {
    @Override
    public Record convert(SinkRecord sinkRecord, boolean splitDBTopic, String dbTopicSeparatorChar,String configurationDatabase) {
        String database = configurationDatabase;
        String topic = sinkRecord.topic();
        if (splitDBTopic) {
            String[] parts = topic.split(dbTopicSeparatorChar);
            if (parts.length == 2) {
                database = parts[0];
                topic = parts[1];
            }
        }
        if (sinkRecord.value() == null) {
            throw new DataException("Value was null for JSON conversion");
        }

        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();
        return new Record(SchemaType.STRING_SCHEMA, new OffsetContainer(topic, partition, offset), null, null, database, sinkRecord);
    }
}
