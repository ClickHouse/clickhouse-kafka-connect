package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.ClickHouseWriter;
import com.clickhouse.kafka.connect.sink.db.DBWriter;
import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import com.clickhouse.kafka.connect.sink.processing.Processing;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.provider.InMemoryState;
import com.clickhouse.kafka.connect.sink.state.provider.KeeperStateProvider;
import com.clickhouse.kafka.connect.util.jmx.MBeanServerUtils;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import com.clickhouse.kafka.connect.util.jmx.Timer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ProxySinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxySinkTask.class);
    private static final AtomicInteger NEXT_ID = new AtomicInteger();
    private Processing processing = null;
    private StateProvider stateProvider = null;
    private DBWriter dbWriter = null;


    private final SinkTaskStatistics statistics;
    private int id = NEXT_ID.getAndAdd(1);

    public ProxySinkTask(final ClickHouseSinkConfig clickHouseSinkConfig, final ErrorReporter errorReporter) {
        LOGGER.info("Enable ExactlyOnce? {}", clickHouseSinkConfig.getExactlyOnce());
        if ( clickHouseSinkConfig.getExactlyOnce() ) {
            this.stateProvider = new KeeperStateProvider(clickHouseSinkConfig);
        } else {
            this.stateProvider = new InMemoryState();
        }
        this.dbWriter = new ClickHouseWriter();

        // Add dead letter queue
        boolean isStarted = dbWriter.start(clickHouseSinkConfig);
        if (!isStarted)
            throw new RuntimeException("Connection to ClickHouse is not active.");
        processing = new Processing(stateProvider, dbWriter, errorReporter);

        this.statistics = MBeanServerUtils.registerMBean(new SinkTaskStatistics(), getMBeanNAme());
    }

    private String getMBeanNAme() {
        return "com.clickhouse:type=ClickHouseKafkaConnector,name=SinkTask" + id;
    }

    public void stop() {
        MBeanServerUtils.unregisterMBean(getMBeanNAme());
    }

    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            LOGGER.trace("No records sent to SinkTask");
            return;
        }
        // Group by topic & partition
        Timer taskTime = Timer.start();
        statistics.receivedRecords(records.size());
        LOGGER.trace(String.format("Got %d records from put API.", records.size()));
        Timer processingTime = Timer.start();
        Map<String, List<Record>> dataRecords = records.stream()
                .map(v -> Record.convert(v))
                .collect(Collectors.groupingBy(Record::getTopicAndPartition));
        statistics.recordProcessingTime(processingTime);
        // TODO - Multi process???
        for (String topicAndPartition : dataRecords.keySet()) {
            // Running on etch topic & partition
            List<Record> rec = dataRecords.get(topicAndPartition);
            processing.doLogic(rec);
        }
        statistics.taskProcessingTime(taskTime);
    }
}
