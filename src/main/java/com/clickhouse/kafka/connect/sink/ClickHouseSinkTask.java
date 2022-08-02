package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.ClickHouseWriter;
import com.clickhouse.kafka.connect.sink.db.DBWriter;
import com.clickhouse.kafka.connect.sink.db.InMemoryDBWriter;
import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import com.clickhouse.kafka.connect.sink.kafka.RangeContainer;
import com.clickhouse.kafka.connect.sink.processing.Processing;
import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.StateRecord;
import com.clickhouse.kafka.connect.sink.state.provider.InMemoryState;
import com.clickhouse.kafka.connect.sink.state.provider.RedisStateProvider;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ClickHouseSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkTask.class);
    private Processing processing = null;
    private StateProvider stateProvider = null;
    private DBWriter dbWriter = null;


    private boolean singleTopic = true;

    @Override
    public String version() {
        return "0.0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("start SinkTask: ");

        stateProvider = new InMemoryState();
        dbWriter = new ClickHouseWriter();
        // Add dead letter queue
        boolean isStarted = dbWriter.start(props);
        if (!isStarted)
            throw new RuntimeException("Connection to ClickHouse is not active.");
        processing = new Processing(stateProvider, dbWriter, createErrorReporter());
    }


    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            LOGGER.info("No records in put API");
            return;
        }
        // Group by topic & partition
        LOGGER.info(String.format("Got %d records from put API.", records.size()));
        Map<String, List<Record>> dataRecords = records.stream()
                .map(v -> Record.convert(v))
                .collect(Collectors.groupingBy(Record::getTopicAndPartition));

        // TODO - Multi process
        for (String topicAndPartition : dataRecords.keySet()) {
            // Running on etch topic & partition
            List<Record> rec = dataRecords.get(topicAndPartition);
            processing.doLogic(rec);
        }
    }

    // TODO: can be removed ss
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        LOGGER.trace("Test");
    }

    @Override
    public void stop() {

    }


    private ErrorReporter createErrorReporter() {
        ErrorReporter result = devNullErrorReporter();
        if (context != null) {
            try {
                ErrantRecordReporter errantRecordReporter = context.errantRecordReporter();
                if (errantRecordReporter != null) {
                    result = errantRecordReporter::report;
                } else {
                    LOGGER.info("Errant record reporter not configured.");
                }
            } catch (NoClassDefFoundError | NoSuchMethodError e) {
                // Will occur in Connect runtimes earlier than 2.6
                LOGGER.info("Kafka versions prior to 2.6 do not support the errant record reporter.");
            }
        }
        return result;
    }

    static ErrorReporter devNullErrorReporter() {
        return (record, e) -> {
        };
    }

}
