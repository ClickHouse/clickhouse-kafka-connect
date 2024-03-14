package com.clickhouse.kafka.connect.sink.processing;

import com.clickhouse.kafka.connect.sink.ClickHouseSinkTask;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.DBWriter;
import com.clickhouse.kafka.connect.sink.dlq.DuplicateException;
import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import com.clickhouse.kafka.connect.sink.kafka.RangeContainer;
import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.StateRecord;
import com.clickhouse.kafka.connect.util.QueryIdentifier;
import com.clickhouse.kafka.connect.util.Utils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Processing {
    private static final Logger LOGGER = LoggerFactory.getLogger(Processing.class);
    private StateProvider stateProvider = null;
    private DBWriter dbWriter = null;

    private ErrorReporter errorReporter = null;

    public Processing(StateProvider stateProvider, DBWriter dbWriter) {
        this.stateProvider = stateProvider;
        this.dbWriter = dbWriter;
        this.errorReporter = null;
    }

    public Processing(StateProvider stateProvider, DBWriter dbWriter, ErrorReporter errorReporter) {
        this.stateProvider = stateProvider;
        this.dbWriter = dbWriter;
        this.errorReporter = errorReporter;
    }
    /**
     * the logic is only for topic partition scoop
     *
     * @param records
     * @param rangeContainer
     */
    private void doInsert(List<Record> records, RangeContainer rangeContainer) {
        if (records == null || records.isEmpty()) {
            LOGGER.debug("doInsert - No records to insert.");
            return;
        }
        QueryIdentifier queryId = new QueryIdentifier(records.get(0).getRecordOffsetContainer().getTopic(), records.get(0).getRecordOffsetContainer().getPartition(),
                rangeContainer.getMinOffset(), rangeContainer.getMaxOffset(),
                UUID.randomUUID().toString());

        try {
            LOGGER.debug("doInsert - Records: [{}] - {}", records.size(), queryId);
            dbWriter.doInsert(records, queryId, errorReporter);
        } catch (Exception e) {
            throw new RuntimeException(queryId.toString(), e);//This way the queryId will propagate
        }
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


    public void doLogic(List<Record> records) throws IOException, ExecutionException, InterruptedException {
        List<Record> trimmedRecords;
        Record record = records.get(0);

        String topic = record.getRecordOffsetContainer().getTopic();
        int partition = record.getRecordOffsetContainer().getPartition();
        RangeContainer rangeContainer = extractRange(records, topic, partition);
        LOGGER.info("doLogic - Topic: [{}], Partition: [{}], MinOffset: [{}], MaxOffset: [{}], Records: [{}]",
                topic, partition, rangeContainer.getMinOffset(), rangeContainer.getMaxOffset(), records.size());
        // State                  Actual
        // [10 , 19]              [10, 19] ==> same
        // [10 , 19]              [10, 30] ==> overlapping [10,19], [20, 30]
        // [10 , 19]              [15, 30] ==> exception
        // [10 , 19]              [10, 15] ==> contains


        // first, let get last topic partition range & state
        StateRecord stateRecord = stateProvider.getStateRecord(topic, partition);
        switch (stateRecord.getState()) {
            case NONE:
                // this is the first time we see this topic and partition; or we had a previous failure setting the state.
                LOGGER.debug("NONE - First time seeing {}", stateRecord);
                stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.BEFORE_PROCESSING));
                doInsert(records, rangeContainer);
                stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.AFTER_PROCESSING));
                break;
            case BEFORE_PROCESSING:
                int bpBeforeDrop = records.size();
                trimmedRecords = dropRecords(stateRecord.getMinOffset(), records);
                int bpAfterDrop = trimmedRecords.size();
                LOGGER.debug("BEFORE_PROCESSING - Before drop total {} After drop total {} state {}", bpBeforeDrop, bpAfterDrop, stateRecord.getOverLappingState(rangeContainer));
                // Here there are several options
                switch (stateRecord.getOverLappingState(rangeContainer)) {
                    case ZERO: // Reset if we're at a 0 state
                        LOGGER.warn(String.format("The topic seems to be deleted. Resetting state for topic [%s] partition [%s].", topic, partition));
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.BEFORE_PROCESSING));//RESET
                        doInsert(records, rangeContainer);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.AFTER_PROCESSING));
                        break;
                    case SAME: // Dedupe in clickhouse will fix it
                    case NEW:
                        doInsert(trimmedRecords, rangeContainer);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.AFTER_PROCESSING));
                        break;
                    case CONTAINS: // The state contains the given records
                        LOGGER.warn(String.format("Records seemingly missing compared to prior batch for topic [%s] partition [%s].", topic, partition));
                        // Do nothing - write to dead letter queue
                        records.forEach( r ->
                                Utils.sendTODlq(errorReporter, r, new DuplicateException(String.format(record.getTopicAndPartition())))
                        );
                        break;
                    case OVER_LAPPING:
                        // spit it to 2 inserts
                        List<List<Record>> rightAndLeft = splitRecordsByOffset(trimmedRecords, stateRecord.getMaxOffset(), stateRecord.getMinOffset());
                        doInsert(rightAndLeft.get(0), stateRecord.getRangeContainer());
                        stateProvider.setStateRecord(new StateRecord(
                                topic, partition, stateRecord.getRangeContainer().getMaxOffset(),
                                stateRecord.getRangeContainer().getMinOffset(), State.AFTER_PROCESSING));
                        List<Record> rightRecords = rightAndLeft.get(1);
                        RangeContainer rightRangeContainer = extractRange(rightRecords, topic, partition);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rightRangeContainer.getMaxOffset(), rightRangeContainer.getMinOffset(), State.BEFORE_PROCESSING));
                        doInsert(rightRecords, rightRangeContainer);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rightRangeContainer.getMaxOffset(), rightRangeContainer.getMinOffset(), State.AFTER_PROCESSING));
                        break;
                    case ERROR:
                        LOGGER.warn(String.format("State mismatch for topic [%s] partition [%s].", topic, partition));
                        break;
                }
            case AFTER_PROCESSING:
                int apBeforeDrop = records.size();
                trimmedRecords = dropRecords(stateRecord.getMinOffset(), records);
                int apAfterDrop = trimmedRecords.size();
                LOGGER.debug("AFTER_PROCESSING - Before drop total {} After drop total {} state {}", apBeforeDrop, apAfterDrop, stateRecord.getOverLappingState(rangeContainer));
                switch (stateRecord.getOverLappingState(rangeContainer)) {
                    case SAME:
                    case CONTAINS:
                        break;
                    case ZERO:
                        LOGGER.warn(String.format("It seems you deleted the topic - resetting state for topic [%s] partition [%s].", topic, partition));
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.BEFORE_PROCESSING));
                        doInsert(records, rangeContainer);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.AFTER_PROCESSING));
                        break;
                    case NEW:
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.BEFORE_PROCESSING));
                        doInsert(trimmedRecords, rangeContainer);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.AFTER_PROCESSING));
                        break;
                    case OVER_LAPPING:
                        // spit it to 2 inserts we will ignore one and insert the other
                        List<List<Record>> rightAndLeft = splitRecordsByOffset(trimmedRecords, stateRecord.getMaxOffset(), stateRecord.getMinOffset());
                        List<Record> rightRecords = rightAndLeft.get(1);
                        RangeContainer rightRangeContainer = extractRange(rightRecords, topic, partition);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rightRangeContainer.getMaxOffset(), rightRangeContainer.getMinOffset(), State.BEFORE_PROCESSING));
                        doInsert(rightRecords, rightRangeContainer);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rightRangeContainer.getMaxOffset(), rightRangeContainer.getMinOffset(), State.AFTER_PROCESSING));
                        break;
                    case ERROR:
                        LOGGER.warn(String.format("State mismatch for topic [%s] partition [%s]", topic, partition));
                }
        }
    }

}
