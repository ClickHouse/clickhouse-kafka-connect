package com.clickhouse.kafka.connect.sink.data.convert;

import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.data.SchemaType;
import com.clickhouse.kafka.connect.sink.data.StructToJsonMap;
import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SchemaRecordConvertor implements RecordConvertor{

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
        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();
        Struct struct = (Struct) sinkRecord.value();
        Map<String, Data> data = StructToJsonMap.toJsonMap((Struct) sinkRecord.value());
        return new Record(SchemaType.SCHEMA, new OffsetContainer(topic, partition, offset), struct.schema().fields(), data, database, sinkRecord);
    }
}
