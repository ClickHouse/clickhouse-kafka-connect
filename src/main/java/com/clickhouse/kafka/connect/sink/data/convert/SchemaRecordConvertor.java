package com.clickhouse.kafka.connect.sink.data.convert;

import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.data.SchemaType;
import com.clickhouse.kafka.connect.sink.data.StructToJsonMap;
import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public class SchemaRecordConvertor extends RecordConvertor{

    @Override
    public Record doConvert(SinkRecord sinkRecord, String topic,String configurationDatabase) {
        String database = configurationDatabase;
        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();
        Struct struct = (Struct) sinkRecord.value();
        Map<String, Data> data = StructToJsonMap.toJsonMap((Struct) sinkRecord.value());
        return new Record(SchemaType.SCHEMA, new OffsetContainer(topic, partition, offset), struct.schema().fields(), data, database, sinkRecord);
    }
}
