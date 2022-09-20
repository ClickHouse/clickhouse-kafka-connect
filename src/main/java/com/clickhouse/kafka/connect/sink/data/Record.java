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
    private Map<String, Data> jsonMap = null;
    private List<Field> fields = null;

    private SinkRecord sinkRecord = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(Record.class);

    public Record(OffsetContainer recordOffsetContainer, List<Field> fields, Map<String, Data> jsonMap, SinkRecord sinkRecord) {
        this.recordOffsetContainer = recordOffsetContainer;
        this.fields = fields;
        this.jsonMap = jsonMap;
        this.sinkRecord = sinkRecord;
    }

    public String getTopicAndPartition() {
        return recordOffsetContainer.getTopicAndPartitionKey();
    }

    public OffsetContainer getRecordOffsetContainer() {
        return recordOffsetContainer;
    }

    public Map<String, Data> getJsonMap() {
        return jsonMap;
    }

    public List<Field> getFields() {
        return fields;
    }

    public SinkRecord getSinkRecord() {
        return sinkRecord;
    }

    public String getTopic() {
        return recordOffsetContainer.getTopic();
    }

    public static Record convert(SinkRecord sinkRecord) {
        String topic = sinkRecord.topic();
        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();
        Struct struct = (Struct) sinkRecord.value();
        Map<String, Data> data = StructToJsonMap.toJsonMap((Struct) sinkRecord.value());
        return new Record(new OffsetContainer(topic, partition, offset), struct.schema().fields(), data, sinkRecord);
    }

    public static Record newRecord(String topic, int partition, long offset, List<Field> fields, Map<String, Data> jsonMap, SinkRecord sinkRecord) {
        return new Record(new OffsetContainer(topic, partition, offset), fields, jsonMap, sinkRecord);
    }

}
