package com.clickhouse.kafka.connect.sink.processing;

import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.DBWriter;
import com.clickhouse.kafka.connect.sink.dlq.DuplicateException;
import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import com.clickhouse.kafka.connect.sink.kafka.RangeContainer;
import com.clickhouse.kafka.connect.sink.kafka.RangeState;
import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateConflictException;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.StateRecord;
import com.clickhouse.kafka.connect.util.QueryIdentifier;
import com.clickhouse.kafka.connect.util.Utils;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.clickhouse.kafka.connect.sink.state.State.AFTER_PROCESSING;
import static com.clickhouse.kafka.connect.sink.state.State.BEFORE_PROCESSING;

public class Processing {
    private static final Logger LOGGER = LoggerFactory.getLogger(Processing.class);
    private StateProvider stateProvider = null;
    private DBWriter dbWriter = null;
    private ClickHouseSinkConfig clickHouseSinkConfig;
    private SinkTaskStatistics statistics;

    private ErrorReporter errorReporter = null;

    public Processing(StateProvider stateProvider, DBWriter dbWriter, ErrorReporter errorReporter,
                      ClickHouseSinkConfig clickHouseSinkConfig, SinkTaskStatistics statistics) {
        this.stateProvider = stateProvider;
        this.dbWriter = dbWriter;
        this.errorReporter = errorReporter;
        this.clickHouseSinkConfig = clickHouseSinkConfig;
        this.statistics = statistics;
    }

    /**
     * the logic is only for topic partition scoop
     *
     * @param records
     * @param rangeContainer
     */
    private void doInsert(List<Record> records, RangeContainer rangeContainer) {
        if (records == null || records.isEmpty()) {
            LOGGER.trace("doInsert - No records to insert.");
            return;
        }

        final Record firstRecord = records.get(0);
        long eventReceiveLag = 0;
        if (firstRecord.getSinkRecord().timestamp() != null) {
            eventReceiveLag = System.currentTimeMillis() - firstRecord.getSinkRecord().timestamp();
        }
        final String topic = firstRecord.getRecordOffsetContainer().getTopic();
        QueryIdentifier queryId = new QueryIdentifier(topic, records.get(0).getRecordOffsetContainer().getPartition(),
                rangeContainer.getMinOffset(), rangeContainer.getMaxOffset(),
                UUID.randomUUID().toString());

        try {
            LOGGER.debug("doInsert - Records: [{}] - {}", records.size(), queryId);
            dbWriter.doInsert(records, queryId, errorReporter);
            statistics.recordTopicStats(records.size(), queryId.getTopic(), eventReceiveLag);
        } catch (Exception e) {
            statistics.recordTopicStatsOnFailure(records.size(), queryId.getTopic(), eventReceiveLag);
            throw new RuntimeException(queryId.toString(), e);//This way the queryId will propagate
        }
    }

    private void doInsert(List<Record> records) {
        if (records == null || records.isEmpty()) {
            LOGGER.trace("doInsert - No records to insert.");
            return;
        }

        final Record firstRecord = records.get(0);
        long eventReceiveLag = 0;
        if (firstRecord.getSinkRecord().timestamp() != null) {
            eventReceiveLag = System.currentTimeMillis() - firstRecord.getSinkRecord().timestamp();
        }
        final String topic = firstRecord.getRecordOffsetContainer().getTopic();
        QueryIdentifier queryId = new QueryIdentifier(topic, UUID.randomUUID().toString());

        try {
            LOGGER.info("doInsert - Records: [{}] - {}", records.size(), queryId);
            dbWriter.doInsert(records, queryId, errorReporter);
            statistics.recordTopicStats(records.size(), topic, eventReceiveLag);
        } catch (Exception e) {
            statistics.recordTopicStatsOnFailure(records.size(), topic, eventReceiveLag);
            throw new RuntimeException(queryId.toString(), e);//This way the queryId will propagate
        }
    }




    /**
     * Moves the stored state from {@code expected} to {@code state} over {@code range}, failing if
     * the stored state is no longer {@code expected}.
     */
    private StateRecord writeState(StateRecord expected, RangeContainer range, State state) {
        StateRecord update = new StateRecord(range.getTopic(), range.getPartition(), range.getMaxOffset(), range.getMinOffset(), state, expected.getOriginalTopic());
        if (!stateProvider.compareAndSetStateRecord(expected, update)) {
            throw new StateConflictException(String.format("State of topic: [%s], partition: [%s] is no longer [%s] - another task is processing this partition, not writing [%s]",
                    range.getTopic(), range.getPartition(), expected, update));
        }
        return update;
    }

    private void insertFenced(StateRecord expected, List<Record> records, RangeContainer range) {
        StateRecord before = writeState(expected, range, BEFORE_PROCESSING);
        doInsert(records, range);
        writeState(before, range, AFTER_PROCESSING);
    }

    private RangeContainer extractRange(List<Record> records, String topic, int partition) {
        RangeContainer rangeContainer = new RangeContainer(topic, partition);
        records.stream().forEach(record -> rangeContainer.defineInRange(record.getRecordOffsetContainer().getOffset()));
        return rangeContainer;
    }

    /**
     * Filters out all the records below the fromOffset.
     */
    private List<Record> dropRecords(long fromOffset, List<Record> records) {
        return records.stream().filter(data -> data.getRecordOffsetContainer().getOffset() >= fromOffset).collect(Collectors.toList());
    }

    /**
     * Splits a list of records into two lists, one from the min offset up to the provided offset (inclusive).
     * The second from offset (exclusive) to the max offset (inclusive).
     * This method will not return records below the minOffset.
     */
    private List<List<Record>> splitRecordsByOffset(List<Record> records, long offset, long minOffset) {
        return new ArrayList<>(
                records.stream()
                        .filter(record -> record.getRecordOffsetContainer().getOffset() >= minOffset)
                        .collect(Collectors.partitioningBy(record -> record.getRecordOffsetContainer().getOffset() > offset))
                        .values()
        );
    }


    public void doLogic(List<Record> records) {
        List<Record> trimmedRecords;
        Record record = records.get(0);

        String database = record.getDatabase();
        String topic = record.getRecordOffsetContainer().getTopic();
        String originalTopic = record.getSinkRecord().topic();

        if (this.clickHouseSinkConfig != null && clickHouseSinkConfig.isEnableDbTopicSplit()) {
            topic = database + clickHouseSinkConfig.getDbTopicSplitChar() + topic;
        }

        if (!clickHouseSinkConfig.isExactlyOnce() && clickHouseSinkConfig.isIgnorePartitionsWhenBatching()) {
            // link to com.clickhouse.kafka.connect.sink.ClickHouseSinkTask.preCommit
            LOGGER.debug("doLogic - Topic: [{}], Records: [{}]", topic, records.size());
            doInsert(records);
            return;
        }

        int partition = record.getRecordOffsetContainer().getPartition();
        RangeContainer rangeContainer = extractRange(records, topic, partition);
        LOGGER.info("doLogic - Topic: [{}], Partition: [{}], MinOffset: [{}], MaxOffset: [{}], Records: [{}]",
                topic, partition, rangeContainer.getMinOffset(), rangeContainer.getMaxOffset(), records.size());
        // State                 Actual
        // [10, 19]              [10, 19] ==> same
        // [10, 19]              [09, 30] ==> overlapping [10, 19], [20, 30]
        // [10, 19]              [15, 30] ==> partial overlapping x, [20, 30]
        // [10, 19]              [15, 18] ==> exception
        // [10, 19]              [10, 15] ==> contains


        // first, let get last topic partition range & state
        StateRecord stateRecord = stateProvider.getStateRecord(topic, partition);
        stateRecord.setOriginalTopic(originalTopic); // restore original topic
        switch (stateRecord.getState()) {
            case NONE:
                // this is the first time we see this topic and partition; or we had a previous failure setting the state.
                LOGGER.debug("NONE - First time seeing {}", stateRecord);
                insertFenced(stateRecord, records, rangeContainer);
                break;
            case BEFORE_PROCESSING:
                int bpBeforeDrop = records.size();
                trimmedRecords = dropRecords(stateRecord.getMinOffset(), records);
                int bpAfterDrop = trimmedRecords.size();
                LOGGER.debug("BEFORE_PROCESSING - Before drop total {} After drop total {} state {}", bpBeforeDrop, bpAfterDrop, stateRecord.getOverLappingState(rangeContainer, BEFORE_PROCESSING));
                // Here there are several options
                switch (stateRecord.getOverLappingState(rangeContainer, BEFORE_PROCESSING)) {
                    case ZERO: // Reset if we're at a 0 state
                        LOGGER.warn(String.format("The topic seems to be deleted. Resetting state for topic [%s] partition [%s].", topic, partition));
                        insertFenced(stateRecord, records, rangeContainer);
                        break;
                    case SAME:
                        // The stored range is the range of the first attempt. The batch may start earlier
                        // (re-polled from an older committed offset), and its own range would yield a
                        // different deduplication token, so ClickHouse would not drop the retry.
                        doInsert(trimmedRecords, stateRecord);
                        writeState(stateRecord, rangeContainer, AFTER_PROCESSING);
                        break;
                    case NEW:
                        insertFenced(stateRecord, trimmedRecords, rangeContainer);
                        break;
                    case CONTAINS: // The state contains the given records
                        // Do nothing - write to dead letter queue
                        throw new RuntimeException(String.format("BEFORE_PROCESSING: State CONTAINS given [%s] records for topic: [%s], partition: [%s], minOffset: [%s], maxOffset: [%s]",
                                records.size(), topic, partition, rangeContainer.getMinOffset(), rangeContainer.getMaxOffset()));
                    case OVER_LAPPING:
                        // spit it to 2 inserts
                        List<List<Record>> rightAndLeft = splitRecordsByOffset(trimmedRecords, stateRecord.getMaxOffset(), stateRecord.getMinOffset());
                        List<Record> leftRecords = rightAndLeft.get(0);
                        RangeContainer leftRangeContainer = extractRange(leftRecords, topic, partition);
                        // (Re-)Insert the first chunk only if the boundaries are the same
                        StateRecord afterLeft = stateRecord;
                        if (stateRecord.getOverLappingState(leftRangeContainer, BEFORE_PROCESSING) == RangeState.SAME) {
                            doInsert(leftRecords, leftRangeContainer);
                            afterLeft = writeState(stateRecord, leftRangeContainer, AFTER_PROCESSING);
                        } else {
                            LOGGER.warn(String.format("Records seemingly missing compared to prior batch for topic [%s] partition [%s].", topic, partition));
                            // Do nothing - write to dead letter queue
                            leftRecords.forEach( r ->
                                    Utils.sendTODlq(errorReporter, r, new DuplicateException(String.format(record.getTopicAndPartition())))
                            );
                        }
                        List<Record> rightRecords = rightAndLeft.get(1);
                        RangeContainer rightRangeContainer = extractRange(rightRecords, topic, partition);
                        insertFenced(afterLeft, rightRecords, rightRangeContainer);
                        break;
                    default: //case ERROR:
                        throw new RuntimeException(String.format("BEFORE_PROCESSING: ERROR State given [%s] records for topic: [%s], partition: [%s], minOffset: [%s], maxOffset: [%s], expectedMinOffset: [%s], expectedMaxOffset: [%s]",
                                records.size(), topic, partition, rangeContainer.getMinOffset(), rangeContainer.getMaxOffset(), stateRecord.getMinOffset(), stateRecord.getMaxOffset()));
                }
                break;
            case AFTER_PROCESSING:
                int apBeforeDrop = records.size();
                trimmedRecords = dropRecords(stateRecord.getMinOffset(), records);
                int apAfterDrop = trimmedRecords.size();
                LOGGER.debug("AFTER_PROCESSING - Before drop total {} After drop total {} state {}", apBeforeDrop, apAfterDrop, stateRecord.getOverLappingState(rangeContainer, AFTER_PROCESSING));
                switch (stateRecord.getOverLappingState(rangeContainer, AFTER_PROCESSING)) {
                    case SAME:
                    case CONTAINS:
                        stateProvider.onStateUpdate(stateRecord);
                        break;
                    case ZERO:
                        LOGGER.warn(String.format("It seems you deleted the topic - resetting state for topic [%s] partition [%s].", topic, partition));
                        insertFenced(stateRecord, records, rangeContainer);
                        break;
                    case NEW:
                        insertFenced(stateRecord, trimmedRecords, rangeContainer);
                        break;
                    case OVER_LAPPING:
                        // spit it to 2 inserts we will ignore one and insert the other
                        List<List<Record>> rightAndLeft = splitRecordsByOffset(trimmedRecords, stateRecord.getMaxOffset(), stateRecord.getMinOffset());
                        List<Record> rightRecords = rightAndLeft.get(1);
                        RangeContainer rightRangeContainer = extractRange(rightRecords, topic, partition);
                        insertFenced(stateRecord, rightRecords, rightRangeContainer);
                        break;
                    case PREVIOUS:
                        if (clickHouseSinkConfig.isTolerateStateMismatch()) {
                            LOGGER.warn("AFTER_PROCESSING: State MISMATCH as batch already processed - skipping [{}] records for topic: [{}], partition: [{}], minOffset: [{}], maxOffset: [{}], storedMinOffset: [{}], storedMaxOffset: [{}]",
                                    records.size(), topic, partition, rangeContainer.getMinOffset(), rangeContainer.getMaxOffset(), stateRecord.getMinOffset(), stateRecord.getMaxOffset());
                        } else {
                            throw new RuntimeException(String.format("AFTER_PROCESSING: State MISMATCH as batch already processed - skipping [%s] records for topic: [%s], partition: [%s], minOffset: [%s], maxOffset: [%s], storedMinOffset: [%s], storedMaxOffset: [%s]",
                                    records.size(), topic, partition, rangeContainer.getMinOffset(), rangeContainer.getMaxOffset(), stateRecord.getMinOffset(), stateRecord.getMaxOffset()));
                        }
                        break;
                    default: //case ERROR:
                        throw new RuntimeException(String.format("AFTER_PROCESSING: ERROR State given [%s] records for topic: [%s], partition: [%s], minOffset: [%s], maxOffset: [%s], expectedMinOffset: [%s], expectedMaxOffset: [%s]",
                                records.size(), topic, partition, rangeContainer.getMinOffset(), rangeContainer.getMaxOffset(), stateRecord.getMinOffset(), stateRecord.getMaxOffset()));
                }
        }
    }
}
