package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.ClickHouseWriter;
import com.clickhouse.kafka.connect.sink.db.DBWriter;
import com.clickhouse.kafka.connect.sink.db.InMemoryDBWriter;
import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import com.clickhouse.kafka.connect.sink.hashing.RecordHash;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ClickHouseSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkTask.class);

    private List<ProxySinkTask> proxySinkTasks = new ArrayList<>();
    private int endpoints;
    private String hashFunctionName;

    @Override
    public String version() {
        return "0.0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("start SinkTask: ");
        ClickHouseSinkConfig clickHouseSinkConfig;
        try {
            clickHouseSinkConfig = new ClickHouseSinkConfig(props);
        } catch (Exception e) {
            throw new ConnectException("Failed to start new task" , e);
        }
        if (!Objects.equals(clickHouseSinkConfig.getEndpoints(), ClickHouseSinkConfig.endpointsDefault)) {
            this.endpoints=0;
            for (String ep : clickHouseSinkConfig.getEndpoints_array()) {
                clickHouseSinkConfig.updateHostNameAndPort(ep);
                LOGGER.info("connecting to endpoint: "+ep);
                this.proxySinkTasks.add(new ProxySinkTask(clickHouseSinkConfig, createErrorReporter()));
                this.endpoints++;
            }
            this.hashFunctionName = clickHouseSinkConfig.getHashFunctionName();
        } else {
            this.proxySinkTasks.add(new ProxySinkTask(clickHouseSinkConfig, createErrorReporter()));
            this.endpoints=1;
        }
    }


    @Override
    public void put(Collection<SinkRecord> records) {
        if (this.endpoints==1){
            this.proxySinkTasks.get(this.proxySinkTasks.size()-1).put(records);
        } else if (this.endpoints>1) {
            List<Collection<SinkRecord>> split_records = consistentSplitting(records, this.proxySinkTasks.size());
            for (int i = 0; i < this.proxySinkTasks.size(); i++) {
                this.proxySinkTasks.get(i).put(split_records.get(i));
            }
        } else {
            LOGGER.error("no sink connections are found");
        }
    }

    // TODO: can be removed ss
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        LOGGER.trace("Test");
    }

    @Override
    public void stop() {
        for (ProxySinkTask task : this.proxySinkTasks) { task.stop(); }
    }

    public int numEndpoints() {
        return this.endpoints;
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

    private List<Collection<SinkRecord>> consistentSplitting(Collection<SinkRecord> records, int n_splits) {
        assert n_splits > 0;
        List<Collection<SinkRecord>> buckets = new ArrayList<>();
        List<ReentrantLock> locks = new ArrayList<>();
        for (int i=0;i<n_splits; i++) {
            buckets.add(new ArrayList<>());
            locks.add(new ReentrantLock());
        }
        records.parallelStream().forEach(record -> {
            RecordHash rh = new RecordHash(record,n_splits);
            if (!rh.setFunctionName(this.hashFunctionName)) {
                LOGGER.error(String.format("hash function %s can not be used, available list of functions include %s ",
                        this.hashFunctionName, rh.availableHashAlgorithms()));
                return;
            }
            int index = rh.getBucketIndex();
            if (index < 0){
                LOGGER.error("Hash buket return "+index+" record hash failed");
                return;
            }
            try {
                if (locks.get(index).tryLock(5, TimeUnit.SECONDS)) {
                    buckets.get(index).add(record);
                    locks.get(index).unlock();
                }
            } catch (InterruptedException exception) {
                LOGGER.warn("Record failed to write to bucket: "+ record);
            }
        });
        return buckets;
    }

}
