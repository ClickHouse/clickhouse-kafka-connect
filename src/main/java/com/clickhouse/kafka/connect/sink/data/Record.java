package com.clickhouse.kafka.connect.sink.data;

import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Record {
    private OffsetContainer recordOffsetContainer = null;
    private Object value;
    private Map<String, Object> jsonMap = null;
    private List<Field> fields = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(Record.class);

    public Record(OffsetContainer recordOffsetContainer, List<Field> fields, Map<String, Object> jsonMap) {
        this.recordOffsetContainer = recordOffsetContainer;
        this.fields = fields;
        this.jsonMap = jsonMap;
    }

    public String getTopicAndPartition() {
        return recordOffsetContainer.getTopicAndPartitionKey();
    }

    public OffsetContainer getRecordOffsetContainer() {
        return recordOffsetContainer;
    }

    public Map<String, Object> getJsonMap() {
        return jsonMap;
    }

    public List<Field> getFields() {
        return fields;
    }

    public static Record convert(SinkRecord sinkRecord) {
        String topic = sinkRecord.topic();
        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();
        Struct struct = (Struct) sinkRecord.value();
        Map<String, Object> data = StructToJsonMap.toJsonMap((Struct) sinkRecord.value());
        return new Record(new OffsetContainer(topic, partition, offset), struct.schema().fields(), data);
    }

    public static Record newRecord(String topic, int partition, long offset, List<Field> fields, Map<String, Object> jsonMap) {
        return new Record(new OffsetContainer(topic, partition, offset), fields, jsonMap);
    }
}
