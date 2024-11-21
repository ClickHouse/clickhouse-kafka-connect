package com.clickhouse.kafka.connect.sink.data.convert;

import com.clickhouse.kafka.connect.sink.data.Record;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.regex.Pattern;

public abstract class RecordConvertor {
    public Record convert(SinkRecord sinkRecord, boolean splitDBTopic, String dbTopicSeparatorChar, String configurationDatabase) {
        String database = configurationDatabase;
        String topic = sinkRecord.topic();
        if (splitDBTopic) {
            String[] parts = topic.split(Pattern.quote(dbTopicSeparatorChar));
            if (parts.length == 2) {
                database = parts[0];
                topic = parts[1];
            }
        }
        return doConvert(sinkRecord, topic, database);
    }
    public abstract Record doConvert(SinkRecord sinkRecord, String topic,String database);
}
