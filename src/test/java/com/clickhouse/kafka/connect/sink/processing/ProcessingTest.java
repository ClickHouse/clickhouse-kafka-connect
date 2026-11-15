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
import com.clickhouse.kafka.connect.util.QueryIdentifier;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    SinkTaskStatistics sinkTaskStatistics;

    @BeforeEach
    void setupEach() {
        sinkTaskStatistics = new SinkTaskStatistics(0);
        sinkTaskStatistics.registerMBean();
    }

    @AfterEach
    void tearDownEach() {
        sinkTaskStatistics.unregisterMBean();
        sinkTaskStatistics = null;
    }

    @Test
    @DisplayName("ProcessAllAtOnceNewTest")
    public void ProcessAllAtOnceNewTest() throws IOException, ExecutionException, InterruptedException {
        List<Record> records = createRecords("test", 1);
        StateProvider stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()), sinkTaskStatistics);
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
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()), sinkTaskStatistics);
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
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()), sinkTaskStatistics);
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
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()), sinkTaskStatistics);
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
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()), sinkTaskStatistics);
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
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()), sinkTaskStatistics);
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
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()), sinkTaskStatistics);
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
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()), sinkTaskStatistics);
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
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()), sinkTaskStatistics);
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
        Processing processing = new Processing(stateProvider, dbWriter, er, clickHouseSinkConfig, sinkTaskStatistics);
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
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()), sinkTaskStatistics);
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
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()), sinkTaskStatistics);
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
        String topic = "test";
        stateProvider.setStateRecord(new StateRecord(topic, 1, 5000, 4000, State.AFTER_PROCESSING, topic));
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processingWithoutConfig = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(new HashMap<>()), sinkTaskStatistics);
        Assert.assertThrows(RuntimeException.class, () -> processingWithoutConfig.doLogic(recordsHead));

        HashMap<String, String> config = new HashMap<>();
        config.put(ClickHouseSinkConfig.TOLERATE_STATE_MISMATCH, "true");
        ClickHouseSinkConfig clickHouseConfig = new ClickHouseSinkConfig(config);
        Processing processing = new Processing(stateProvider, dbWriter, null, clickHouseConfig, sinkTaskStatistics);
        processing.doLogic(recordsHead);
        assertEquals(0, dbWriter.recordsInserted());
    }

    @Test
    @DisplayName("Db topic split stores processed topic key in state offsets")
    public void dbTopicSplitUsesProcessedTopicForStateOffsets() throws IOException, ExecutionException, InterruptedException {
        String kafkaTopic = "orders";
        String database = "defaultdb";
        int partition = 1;
        List<Record> records = createRecords(database, kafkaTopic, partition);

        HashMap<String, String> config = new HashMap<>();
        config.put(ClickHouseSinkConfig.ENABLE_DB_TOPIC_SPLIT, "true");
        config.put(ClickHouseSinkConfig.DB_TOPIC_SPLIT_CHAR, ".");

        InMemoryState stateProvider = new InMemoryState();
        DBWriter dbWriter = new InMemoryDBWriter();
        Processing processing = new Processing(stateProvider, dbWriter, null, new ClickHouseSinkConfig(config), sinkTaskStatistics);
        processing.doLogic(records);

        Map<TopicPartition, OffsetAndMetadata> offsets = stateProvider.getLastInsertedOffsetsSnapshot();
        TopicPartition originalTopicPartition = new TopicPartition(kafkaTopic, partition);
        TopicPartition processedTopicPartition = new TopicPartition(database + "." + kafkaTopic, partition);

        assertTrue(offsets.containsKey(originalTopicPartition),
                "State offsets should contain original topic");
        assertFalse(offsets.containsKey(processedTopicPartition),
                "State offsets should not contain processed database.topic key");
    }

    /**
     * Emulates ClickHouse block deduplication: a block whose insert_deduplication_token was already
     * accepted is dropped, anything else is appended, so the same offset can be stored more than once.
     */
    private static class TokenDedupDBWriter extends InMemoryDBWriter {
        private final Set<String> acceptedTokens = new HashSet<>();
        private final List<Long> storedOffsets = new ArrayList<>();

        @Override
        public void doInsert(List<Record> records, QueryIdentifier queryId) {
            if (!acceptedTokens.add(queryId.getDeduplicationToken())) {
                return;
            }
            records.forEach(r -> storedOffsets.add(r.getRecordOffsetContainer().getOffset()));
            super.doInsert(records, queryId);
        }

        List<Long> storedOffsets() {
            return storedOffsets;
        }
    }

    /**
     * Shares the state store of another worker and runs a callback once, right after this worker has
     * read the state - the point where a task frozen past the session timeout loses its partition.
     */
    private static class PausingAfterReadState implements StateProvider {
        private final StateProvider shared;
        private Runnable whilePaused;

        PausingAfterReadState(StateProvider shared) {
            this.shared = shared;
        }

        void pauseAfterNextRead(Runnable whilePaused) {
            this.whilePaused = whilePaused;
        }

        @Override
        public StateRecord getStateRecord(String topic, int partition) {
            StateRecord read = shared.getStateRecord(topic, partition);
            if (whilePaused != null) {
                Runnable otherWorker = whilePaused;
                whilePaused = null;
                otherWorker.run();
            }
            return read;
        }

        @Override
        public void setStateRecord(StateRecord stateRecord) {
            shared.setStateRecord(stateRecord);
        }

        @Override
        public boolean compareAndSetStateRecord(StateRecord expected, StateRecord update) {
            return shared.compareAndSetStateRecord(expected, update);
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> getLastInsertedOffsetsSnapshot() {
            return shared.getLastInsertedOffsetsSnapshot();
        }

        @Override
        public void onStateUpdate(StateRecord stateRecord) {
            shared.onStateUpdate(stateRecord);
        }

        @Override
        public void onPartitionRemoved(Collection<TopicPartition> removedPartitions) {
            shared.onPartitionRemoved(removedPartitions);
        }
    }

    @Test
    @DisplayName("Worker paused after reading state must not insert a batch the new partition owner already inserted")
    public void pausedWorkerDoesNotDuplicateRowsInsertedByNewPartitionOwner() {
        String topic = "test";
        int partition = 1;
        List<Record> records = createRecords(topic, partition);
        ClickHouseSinkConfig config = new ClickHouseSinkConfig(new HashMap<>());

        StateProvider sharedState = new InMemoryState();
        TokenDedupDBWriter table = new TokenDedupDBWriter();
        PausingAfterReadState pausedWorkerState = new PausingAfterReadState(sharedState);
        Processing pausedWorker = new Processing(pausedWorkerState, table, null, config, sinkTaskStatistics);
        Processing newOwner = new Processing(sharedState, table, null, config, sinkTaskStatistics);

        pausedWorker.doLogic(records.subList(100, 130)); // state: AFTER_PROCESSING[100,129]

        // The new owner resumes from the committed offset 100, so its batch boundaries differ.
        pausedWorkerState.pauseAfterNextRead(() -> newOwner.doLogic(records.subList(100, 181)));
        RuntimeException rejected = null;
        try {
            pausedWorker.doLogic(records.subList(130, 160));
        } catch (RuntimeException e) {
            rejected = e;
        }

        List<Long> offsets100To180 = LongStream.rangeClosed(100, 180).boxed().collect(Collectors.toList());
        assertEquals(offsets100To180, table.storedOffsets(), "every offset must be stored exactly once");
        assertEquals(new StateRecord(topic, partition, 180, 130, State.AFTER_PROCESSING, topic),
                sharedState.getStateRecord(topic, partition),
                "the paused worker must not overwrite the state written by the new owner");
        assertNotNull(rejected, "the paused worker must fail its batch once it sees the state has moved");
    }

    @Test
    @DisplayName("Retry of a BEFORE_PROCESSING batch by a task that polled from an earlier offset reuses the deduplication token of the first attempt")
    public void retryFromEarlierOffsetIsDeduplicatedAgainstFirstAttempt() {
        String topic = "test";
        int partition = 1;
        List<Record> records = createRecords(topic, partition);
        StateProvider state = new InMemoryState();
        TokenDedupDBWriter table = new TokenDedupDBWriter();
        Processing processing = new Processing(state, table, null, new ClickHouseSinkConfig(new HashMap<>()), sinkTaskStatistics);
        processing.doLogic(records.subList(100, 130));

        // First attempt of [130,159] reached ClickHouse, but the task died before AFTER_PROCESSING.
        state.setStateRecord(new StateRecord(topic, partition, 159, 130, State.BEFORE_PROCESSING, topic));
        table.doInsert(records.subList(130, 160), new QueryIdentifier(topic, partition, 130, 159, "first-attempt"));

        processing.doLogic(records.subList(100, 160)); // committed offset was still 100

        List<Long> offsets100To159 = LongStream.rangeClosed(100, 159).boxed().collect(Collectors.toList());
        assertEquals(offsets100To159, table.storedOffsets(), "every offset must be stored exactly once");
    }

}
