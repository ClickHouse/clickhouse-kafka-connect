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
import com.clickhouse.kafka.connect.util.Utils;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ClickHouseSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkTask.class);

    private ProxySinkTask proxySinkTask;
    private ClickHouseSinkConfig clickHouseSinkConfig;
    private ErrorReporter errorReporter;

    @Override
    public String version() {
        return "0.0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("Start SinkTask: ");
        try {
            clickHouseSinkConfig = new ClickHouseSinkConfig(props);
            errorReporter = createErrorReporter();
        } catch (Exception e) {
            throw new ConnectException("Failed to start new task" , e);
        }

        this.proxySinkTask = new ProxySinkTask(clickHouseSinkConfig, errorReporter);
    }


    @Override
    public void put(Collection<SinkRecord> records) {
        try {
            this.proxySinkTask.put(records);
        } catch (Exception e) {
            LOGGER.trace("Passing the exception to the exception handler.");
            boolean errorTolerance = clickHouseSinkConfig != null && clickHouseSinkConfig.getErrorsTolerance();
            Utils.handleException(e, errorTolerance, records);
            if (errorTolerance && errorReporter != null) {
                LOGGER.warn("Sending [{}] records to DLQ for exception: {}", records.size(), e.getLocalizedMessage());
                records.forEach(r -> Utils.sendTODlq(errorReporter, r, e));
            }
        }
    }


    // TODO: can be removed ss
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        LOGGER.trace("Test");
    }

    @Override
    public void stop() {
        if (this.proxySinkTask != null) {
            this.proxySinkTask.stop();
        }
    }

    public void setErrorReporter(ErrorReporter errorReporter) {
        this.errorReporter = errorReporter;
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
