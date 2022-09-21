package com.clickhouse.kafka.connect.sink.data.convert;

import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SchemalessRecordConvertor implements RecordConvertor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemalessRecordConvertor.class);

    @Override
    public Record convert(SinkRecord sinkRecord) {
        String topic = sinkRecord.topic();
        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();

        Map<?,?> map = (Map) sinkRecord.value();
        Map<String, Data> data = new HashMap<>();
        map.forEach((key,val) -> {
                    data.put(key.toString(), new Data(Schema.Type.STRING, val));
                }
        );
        return new Record(new OffsetContainer(topic, partition, offset), null, data, sinkRecord);
    }
}
