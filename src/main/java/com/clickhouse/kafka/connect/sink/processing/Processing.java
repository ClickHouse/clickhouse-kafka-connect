package com.clickhouse.kafka.connect.sink.processing;

import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.DBWriter;
import com.clickhouse.kafka.connect.sink.kafka.RangeContainer;
import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.StateRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Processing {
    private StateProvider stateProvider = null;
    private DBWriter dbWriter = null;

    private SinkTaskContext context;

    public Processing(StateProvider stateProvider, DBWriter dbWriter) {
        this.stateProvider = stateProvider;
        this.dbWriter = dbWriter;
        this.context = null;
    }

    public Processing(StateProvider stateProvider, DBWriter dbWriter, SinkTaskContext context) {
        this.stateProvider = stateProvider;
        this.dbWriter = dbWriter;
        this.context = context;
    }
    /**
     * the logic is only for topic partition scoop
     *
     * @param records
     */
    private void doInsert(List<Record> records) {
        dbWriter.doInsert(records);
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
                        .collect(Collectors.partitioningBy(record -> record.getRecordOffsetContainer().getOffset() <= offset))
                        .values()
        );
    }
    private void sendTODlq(SinkRecord record, RuntimeException exception) {
        if (context != null && context.errantRecordReporter() != null) {
            context.errantRecordReporter().report(record, exception);
        }
    }

    public void doLogic(List<Record> records) {

        Record record = records.get(0);

        String topic = record.getRecordOffsetContainer().getTopic();
        int partition = record.getRecordOffsetContainer().getPartition();
        RangeContainer rangeContainer = extractRange(records, topic, partition);

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
                stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.BEFORE_PROCESSING));
                doInsert(records);
                stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.AFTER_PROCESSING));
                break;
            case BEFORE_PROCESSING:
                records = dropRecords(stateRecord.getMinOffset(), records);
                // Here there are several options
                switch (stateRecord.getOverLappingState(rangeContainer)) {
                    case SAME: // Dedupe in clickhouse will fix it
                    case NEW:
                        doInsert(records);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.AFTER_PROCESSING));
                        break;
                    case CONTAINS: // The state contains the given records
                        // Do nothing - write to dead letter queue
                        records.forEach( r -> sendTODlq(record.getSinkRecord(), new RuntimeException()));
                        break;
                    case OVER_LAPPING:
                        // spit it to 2 inserts
                        List<List<Record>> rightAndLeft = splitRecordsByOffset(records, stateRecord.getMaxOffset(), stateRecord.getMinOffset());
                        doInsert(rightAndLeft.get(0));
                        stateProvider.setStateRecord(new StateRecord(
                                topic, partition, stateRecord.getRangeContainer().getMaxOffset(),
                                stateRecord.getRangeContainer().getMinOffset(), State.AFTER_PROCESSING));
                        List<Record> rightRecords = rightAndLeft.get(1);
                        RangeContainer rightRangeContainer = extractRange(rightRecords, topic, partition);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rightRangeContainer.getMaxOffset(), rightRangeContainer.getMinOffset(), State.BEFORE_PROCESSING));
                        doInsert(rightRecords);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rightRangeContainer.getMaxOffset(), rightRangeContainer.getMinOffset(), State.AFTER_PROCESSING));
                        break;
                }
            case AFTER_PROCESSING:
                records = dropRecords(stateRecord.getMinOffset(), records);
                switch (stateRecord.getOverLappingState(rangeContainer)) {
                    case SAME:
                    case CONTAINS:
                        break;
                    case NEW:
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.BEFORE_PROCESSING));
                        doInsert(records);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rangeContainer.getMaxOffset(), rangeContainer.getMinOffset(), State.AFTER_PROCESSING));
                        break;
                    case OVER_LAPPING:
                        // spit it to 2 inserts we will ignore one and insert the other
                        List<List<Record>> rightAndLeft = splitRecordsByOffset(records, stateRecord.getMaxOffset(), stateRecord.getMinOffset());
                        List<Record> rightRecords = rightAndLeft.get(1);
                        RangeContainer rightRangeContainer = extractRange(rightRecords, topic, partition);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rightRangeContainer.getMaxOffset(), rightRangeContainer.getMinOffset(), State.BEFORE_PROCESSING));
                        doInsert(rightRecords);
                        stateProvider.setStateRecord(new StateRecord(topic, partition, rightRangeContainer.getMaxOffset(), rightRangeContainer.getMinOffset(), State.AFTER_PROCESSING));
                        break;
                }
        }
    }

}
