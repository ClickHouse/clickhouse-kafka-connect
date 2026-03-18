package com.clickhouse.kafka.connect.sink.state;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

public interface StateProvider {

    StateRecord getStateRecord(String topic, int partition );


    void setStateRecord(StateRecord stateRecord);

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
