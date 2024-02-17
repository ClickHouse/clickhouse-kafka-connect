package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.ClickHouseWriter;
import com.clickhouse.kafka.connect.sink.db.DBWriter;
import com.clickhouse.kafka.connect.sink.db.TableMappingRefresher;
import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import com.clickhouse.kafka.connect.sink.processing.Processing;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.provider.InMemoryState;
import com.clickhouse.kafka.connect.sink.state.provider.KeeperStateProvider;
import com.clickhouse.kafka.connect.util.jmx.MBeanServerUtils;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import com.clickhouse.kafka.connect.util.jmx.ExecutionTimer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.Timer;

public class ProxySinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxySinkTask.class);
    private static final AtomicInteger NEXT_ID = new AtomicInteger();
    private Processing processing = null;
    private StateProvider stateProvider = null;
    private DBWriter dbWriter = null;
    private ClickHouseSinkConfig clickHouseSinkConfig = null;


    private final SinkTaskStatistics statistics;
    private int id = NEXT_ID.getAndAdd(1);

    public ProxySinkTask(final ClickHouseSinkConfig clickHouseSinkConfig, final ErrorReporter errorReporter) {
        this.clickHouseSinkConfig = clickHouseSinkConfig;
        LOGGER.info("Enable ExactlyOnce? {}", clickHouseSinkConfig.getExactlyOnce());
        if ( clickHouseSinkConfig.getExactlyOnce() ) {
            this.stateProvider = new KeeperStateProvider(clickHouseSinkConfig);
        } else {
            this.stateProvider = new InMemoryState();
        }

        ClickHouseWriter chWriter = new ClickHouseWriter();
        this.dbWriter = chWriter;

        // Add table mapping refresher
        if (clickHouseSinkConfig.getTableRefreshInterval() > 0) {
            TableMappingRefresher tableMappingRefresher = new TableMappingRefresher(chWriter);
            Timer tableRefreshTimer = new Timer();
            tableRefreshTimer.schedule(tableMappingRefresher, clickHouseSinkConfig.getTableRefreshInterval(), clickHouseSinkConfig.getTableRefreshInterval());
        }

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

    public void put(final Collection<SinkRecord> records) throws IOException, ExecutionException, InterruptedException {
        if (records.isEmpty()) {
            LOGGER.trace("No records sent to SinkTask");
            return;
        }
        // Group by topic & partition
        ExecutionTimer taskTime = ExecutionTimer.start();
        statistics.receivedRecords(records.size());
        LOGGER.trace(String.format("Got %d records from put API.", records.size()));
        ExecutionTimer processingTime = ExecutionTimer.start();

        Map<String, List<Record>> dataRecords = records.stream()
                .map(v -> Record.convert(v,
                        clickHouseSinkConfig.getEnableDbTopicSplit(),
                        clickHouseSinkConfig.getDbTopicSplitChar(),
                        clickHouseSinkConfig.getDatabase() ))
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
