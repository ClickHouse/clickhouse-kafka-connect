package com.clickhouse.kafka.connect.sink.data;

import com.clickhouse.kafka.connect.sink.data.convert.EmptyRecordConvertor;
import com.clickhouse.kafka.connect.sink.data.convert.RecordConvertor;
import com.clickhouse.kafka.connect.sink.data.convert.SchemaRecordConvertor;
import com.clickhouse.kafka.connect.sink.data.convert.SchemalessRecordConvertor;
import com.clickhouse.kafka.connect.sink.data.convert.StringRecordConvertor;
import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;
import lombok.Getter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

public class Record {
    @Getter
    private OffsetContainer recordOffsetContainer = null;
    private Object value;
    @Getter
    private Map<String, Data> jsonMap = null;
    @Getter
    private List<Field> fields = null;
    @Getter
    private SchemaType schemaType;
    @Getter
    private SinkRecord sinkRecord = null;
    @Getter
    private String database = null;

    public Record(SchemaType schemaType, OffsetContainer recordOffsetContainer, List<Field> fields, Map<String, Data> jsonMap, String database, SinkRecord sinkRecord) {
        this.recordOffsetContainer = recordOffsetContainer;
        this.fields = fields;
        this.jsonMap = jsonMap;
        this.sinkRecord = sinkRecord;
        this.schemaType = schemaType;
        this.database = database;
    }

    public String getTopicAndPartition() {
        return recordOffsetContainer.getTopicAndPartitionKey();
    }

    public String getTopic() {
        return recordOffsetContainer.getTopic();
    }

    private static final RecordConvertor schemaRecordConvertor = new SchemaRecordConvertor();
    private static final RecordConvertor schemalessRecordConvertor = new SchemalessRecordConvertor();
    private static final RecordConvertor emptyRecordConvertor = new EmptyRecordConvertor();
    private static final RecordConvertor stringRecordConvertor = new StringRecordConvertor();
    private static RecordConvertor getConvertor(Schema schema, Object data) {
        if (data == null ) {
            return emptyRecordConvertor;
        }
        if (schema != null && data instanceof Struct) {
            return schemaRecordConvertor;
        }
        if (data instanceof Map) {
            return schemalessRecordConvertor;
        }
        if (data instanceof String) {
            return stringRecordConvertor;
        }
        throw new DataException(String.format("No converter was found due to unexpected object type %s", data.getClass().getName()));
    }

    public static Record convert(SinkRecord sinkRecord, boolean splitDBTopic, String dbTopicSeparatorChar,String database) {
        RecordConvertor recordConvertor = getConvertor(sinkRecord.valueSchema(), sinkRecord.value());
        return recordConvertor.convert(sinkRecord, splitDBTopic, dbTopicSeparatorChar, database);
    }

    public static Record newRecord(SchemaType schemaType, String topic, int partition, long offset, List<Field> fields, Map<String, Data> jsonMap, String database, SinkRecord sinkRecord) {
        return new Record(schemaType, new OffsetContainer(topic, partition, offset), fields, jsonMap, database, sinkRecord);
    }

}
