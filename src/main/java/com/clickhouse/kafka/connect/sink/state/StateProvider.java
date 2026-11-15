package com.clickhouse.kafka.connect.sink.state;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

public interface StateProvider {

    StateRecord getStateRecord(String topic, int partition );


    void setStateRecord(StateRecord stateRecord);

    /**
     * Writes {@code update} only if the stored state still equals {@code expected}, the record the
     * caller based its decision on (a record in state {@code NONE} means "nothing stored yet").
     * This fences a task that lost its partition while it was paused: its decision is stale, so it
     * must not overwrite the state of the new owner nor insert its batch.
     *
     * @return false if the stored state differs from {@code expected}; nothing is written then.
     */
    boolean compareAndSetStateRecord(StateRecord expected, StateRecord update);

    /**
     * Implementation should keep mapping of last inserted offset and keep track of removed partitions.
     *
     * @return mutable copy of internal map.
     */
    Map<TopicPartition, OffsetAndMetadata> getLastInsertedOffsetsSnapshot();

    /**
     * Implementation should update partition offsets only. It is called when state is the same and
     * should not be written to DB.
     * @param stateRecord record with offset update
     */
    void onStateUpdate(StateRecord stateRecord);

    /**
     * This method is called while partition rebalance and should remove listed partitions.
     * It is important to remove partition to avoid it out-dated offset appear in preCommit result.
     * @param removedPartitions list of removed partitions.
     */
    void onPartitionRemoved(Collection<TopicPartition> removedPartitions);
}
