package com.clickhouse.kafka.connect.sink.data;

import com.clickhouse.kafka.connect.sink.data.convert.RecordConvertor;
import com.clickhouse.kafka.connect.sink.data.convert.SchemalessRecordConvertor;
import com.clickhouse.kafka.connect.sink.data.convert.SchemaRecordConvertor;
import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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



    private static RecordConvertor schemaRecordConvertor = new SchemaRecordConvertor();
    private static RecordConvertor schemalessRecordConvertor = new SchemalessRecordConvertor();
    private static RecordConvertor getConvertor(Schema schema, Object data) {
        if (schema != null && data instanceof Struct) {
            return schemaRecordConvertor;
        }
        if (data instanceof Map) {
            return schemalessRecordConvertor;
        }
        throw new DataException(String.format("No converter was found due to unexpected object type %s", data.getClass().getName()));
    }

    public static Record convert(SinkRecord sinkRecord) {
        RecordConvertor recordConvertor = getConvertor(sinkRecord.valueSchema(), sinkRecord.value());
        return recordConvertor.convert(sinkRecord);
    }

    public static Record newRecord(String topic, int partition, long offset, List<Field> fields, Map<String, Data> jsonMap, SinkRecord sinkRecord) {
        return new Record(new OffsetContainer(topic, partition, offset), fields, jsonMap, sinkRecord);
    }

}
