package com.clickhouse.kafka.connect.sink.processing;

import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.data.SchemaType;
import com.clickhouse.kafka.connect.sink.db.DBWriter;
import com.clickhouse.kafka.connect.sink.db.InMemoryDBWriter;
import com.clickhouse.kafka.connect.sink.dlq.InMemoryDLQ;
import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.StateRecord;
import com.clickhouse.kafka.connect.sink.state.provider.InMemoryState;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProcessingTest {


    private List<Record> createRecords(String topic, int partition) {
        return createRecords("default", topic, partition);
    }
    private List<Record> createRecords(String database, String topic, int partition) {
        // create records
        List<Record> records = new ArrayList<>(1000);
        LongStream.range(0, 1000).forEachOrdered(n -> {
            SinkRecord sr = new SinkRecord(topic,
                    partition,
                    null,
                    null,
                    null,
                    null,
                    0);
            Record record = Record.newRecord(SchemaType.SCHEMA, topic, partition, n, null, Collections.singletonMap("off", new Data(Schema.INT8_SCHEMA, n)), database, sr);
            records.add(record);
        });
        return records;
    }

    @Test
    @DisplayName("ProcessAllAtOnceNewTest")
    public void ProcessAllAtOnceNewTest() throws IOException, ExecutionException, InterruptedException {
        List<Record> records = createRecords("test", 1);
        StateProvider stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()));
        processing.doLogic(records);
        assertEquals(records.size(), dbWriter.recordsInserted());
    }

    @Test
    @DisplayName("ProcessSplitNewTest")
    public void ProcessSplitNewTest() throws IOException, ExecutionException, InterruptedException {
        List<Record> records = createRecords("test", 1);
        int splitPoint = 11;
        List<Record> recordsHead = records.subList(0, splitPoint);
        List<Record> recordsTail = records.subList(splitPoint, records.size());
        assertEquals(records.size(), recordsHead.size() + recordsTail.size());
        StateProvider stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()));
        processing.doLogic(recordsHead);
        assertEquals(recordsHead.size(), dbWriter.recordsInserted());
        processing.doLogic(recordsTail);
        assertEquals(records.size(), dbWriter.recordsInserted());
    }

    @Test
    @DisplayName("ProcessAllNewTwiceTest")
    public void ProcessAllNewTwiceTest() throws IOException, ExecutionException, InterruptedException {
        List<Record> records = createRecords("test", 1);
        StateProvider stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()));
        processing.doLogic(records);
        assertEquals(records.size(), dbWriter.recordsInserted());
        processing.doLogic(records);
        assertEquals(records.size(), dbWriter.recordsInserted());
    }

    @Test
    @DisplayName("ProcessAllNewFailedSetStateAfterProcessingTest")
    public void ProcessAllNewFailedSetStateAfterProcessingTest() throws IOException, ExecutionException, InterruptedException {
        List<Record> records = createRecords("test", 1);
        int splitPoint = 11;
        List<Record> recordsHead = records.subList(0, splitPoint);
        //List<Record> recordsTail = records.subList(splitPoint, records.size());
        StateProvider stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()));
        processing.doLogic(recordsHead);
        assertEquals(recordsHead.size(), dbWriter.recordsInserted());
        StateRecord stateRecord = stateProvider.getStateRecord("test", 1);
        stateRecord.setState(State.BEFORE_PROCESSING);
        processing.doLogic(records);
        assertEquals(records.size(), dbWriter.recordsInserted());
    }

    @Test
    @DisplayName("ProcessContainsBeforeProcessingTest")
    public void ProcessContainsBeforeProcessingTest() throws IOException, ExecutionException, InterruptedException {
        List<Record> records = createRecords("test", 1);
        List<Record> containsRecords = records.subList(345,850);
        StateProvider stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()));
        processing.doLogic(records);
        assertEquals(records.size(), dbWriter.recordsInserted());
        StateRecord stateRecord = stateProvider.getStateRecord("test", 1);
        stateRecord.setState(State.BEFORE_PROCESSING);
        assertThrows(RuntimeException.class, () -> processing.doLogic(containsRecords));
    }

    @Test
    @DisplayName("ProcessContainsAfterProcessingTest")
    public void ProcessContainsAfterProcessingTest() throws IOException, ExecutionException, InterruptedException {
        List<Record> records = createRecords("test", 1);
        List<Record> containsRecords = records.subList(345,850);
        StateProvider stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()));
        processing.doLogic(records);
        assertEquals(records.size(), dbWriter.recordsInserted());
        processing.doLogic(containsRecords);
        assertEquals(records.size(), dbWriter.recordsInserted());
    }


    @Test
    @DisplayName("ProcessContainsAfterProcessingTest")
    public void ProcessOverlappingBeforeProcessingTest() throws IOException, ExecutionException, InterruptedException {
        List<Record> records = createRecords("test", 1);
        List<Record> containsRecords = records.subList(345,850);
        StateProvider stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()));
        processing.doLogic(records);
        assertEquals(records.size(), dbWriter.recordsInserted());
        processing.doLogic(containsRecords);
        assertEquals(records.size(), dbWriter.recordsInserted());

    }

    @Test
    @DisplayName("ProcessSplitNewWithBeforeProcessingTest")
    public void ProcessSplitNewWithBeforeProcessingTest() throws IOException, ExecutionException, InterruptedException {
        List<Record> records = createRecords("test", 1);
        int splitPoint = 11;
        List<Record> recordsHead = records.subList(0, splitPoint);
        List<Record> recordsTail = records.subList(splitPoint, records.size());
        assertEquals(records.size(), recordsHead.size() + recordsTail.size());
        StateProvider stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()));
        processing.doLogic(recordsHead);
        assertEquals(recordsHead.size(), dbWriter.recordsInserted());
        StateRecord stateRecord = stateProvider.getStateRecord("test", 1);
        stateRecord.setState(State.BEFORE_PROCESSING);
        processing.doLogic(recordsTail);
        assertEquals(records.size(), dbWriter.recordsInserted());
    }


    @Test
    @DisplayName("ProcessDeletedTopicBeforeProcessingTest")
    public void ProcessDeletedTopicBeforeProcessingTest() throws IOException, ExecutionException, InterruptedException {
        List<Record> records = createRecords("test", 1);
        List<Record> containsRecords = records.subList(0,150);
        StateProvider stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()));
        processing.doLogic(records);
        assertEquals(records.size(), dbWriter.recordsInserted());
        StateRecord stateRecord = stateProvider.getStateRecord("test", 1);
        stateRecord.setState(State.BEFORE_PROCESSING);
        assertThrows(RuntimeException.class, () -> processing.doLogic(containsRecords));
    }

    @Test
    @DisplayName("Processing with dlq test")
    public void ProcessingWithDLQTest() throws IOException, ExecutionException, InterruptedException {
        InMemoryDLQ er = new InMemoryDLQ();
        List<Record> records = createRecords("test", 1);
        List<Record> containsRecords = records.subList(345,850);
        StateProvider stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        ClickHouseSinkConfig clickHouseSinkConfig = new ClickHouseSinkConfig(new HashMap<>());
        Processing processing = new Processing(stateProvider, dbWriter, er, clickHouseSinkConfig);
        processing.doLogic(records);
        assertEquals(records.size(), dbWriter.recordsInserted());
        StateRecord stateRecord = stateProvider.getStateRecord("test", 1);
        stateRecord.setState(State.BEFORE_PROCESSING);
        assertThrows(RuntimeException.class, () -> processing.doLogic(containsRecords));
    }

    @Test
    @DisplayName("ProcessPartialOverlappingBeforeProcessingTest")
    public void ProcessPartialOverlappingBeforeProcessingTest() throws IOException, ExecutionException, InterruptedException {
        List<Record> records = createRecords("test", 1);
        int splitPointHigh = 600;
        int splitPointLow = 350;
        List<Record> recordsHead = records.subList(0, splitPointHigh);
        List<Record> recordsTail = records.subList(splitPointLow, records.size());
        StateProvider stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()));
        processing.doLogic(recordsHead);
        assertEquals(recordsHead.size(), dbWriter.recordsInserted());
        StateRecord stateRecord = stateProvider.getStateRecord("test", 1);
        stateRecord.setState(State.BEFORE_PROCESSING);
        processing.doLogic(recordsTail);
        assertEquals(records.size(), dbWriter.recordsInserted());
    }

    @Test
    @DisplayName("ProcessPartialOverlappingAfterProcessingTest")
    public void ProcessPartialOverlappingAfterProcessingTest() throws IOException, ExecutionException, InterruptedException {
        List<Record> records = createRecords("test", 1);
        int splitPointHigh = 600;
        int splitPointLow = 350;
        List<Record> recordsHead = records.subList(0, splitPointHigh);
        List<Record> recordsTail = records.subList(splitPointLow, records.size());
        StateProvider stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()));
        processing.doLogic(recordsHead);
        assertEquals(recordsHead.size(), dbWriter.recordsInserted());
        processing.doLogic(recordsTail);
        assertEquals(records.size(), dbWriter.recordsInserted());
    }

    @Test
    @DisplayName("ProcessOldRecordsTest")
    public void ProcessOldRecordsTest() throws IOException, ExecutionException, InterruptedException {
        List<Record> records = createRecords("test", 1);
        List<Record> recordsHead = records.subList(1, 2);
        StateProvider stateProvider = new InMemoryState();
        stateProvider.setStateRecord(new StateRecord("test", 1, 5000, 4000, State.AFTER_PROCESSING));
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processingWithoutConfig = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()));
        Assert.assertThrows(RuntimeException.class, () -> processingWithoutConfig.doLogic(recordsHead));

        HashMap<String, String> config = new HashMap<>();
        config.put(ClickHouseSinkConfig.TOLERATE_STATE_MISMATCH, "true");
        ClickHouseSinkConfig clickHouseConfig = new ClickHouseSinkConfig(config);
        Processing processing = new Processing(stateProvider, dbWriter, null, clickHouseConfig);
        processing.doLogic(recordsHead);
        assertEquals(0, dbWriter.recordsInserted());
    }

}
