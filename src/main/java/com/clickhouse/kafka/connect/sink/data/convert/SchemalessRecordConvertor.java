package com.clickhouse.kafka.connect.sink.data.convert;

import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.data.SchemaType;
import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemalessRecordConvertor extends RecordConvertor {

    @Override
    public Record doConvert(SinkRecord sinkRecord, String topic,String configurationDatabase) {
        String database = configurationDatabase;
        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();
        List<Field> fields = new ArrayList<>();
        Map<?,?> map = (Map) sinkRecord.value();
        Map<String, Data> data = new HashMap<>();
        int index = 0;
        map.forEach((key,val) -> {
                    fields.add(new Field(key.toString(), index, Schema.STRING_SCHEMA));
                    data.put(key.toString(), new Data(Schema.STRING_SCHEMA, val == null ? null : val.toString()));
                });
        return new Record(SchemaType.SCHEMA_LESS, new OffsetContainer(topic, partition, offset), fields, data, database, sinkRecord);
    }
}
