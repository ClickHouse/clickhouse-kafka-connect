package com.clickhouse.kafka.connect.sink.state;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.clickhouse.kafka.connect.sink.state.State.AFTER_PROCESSING;

public abstract class BaseStateProviderImpl implements StateProvider {

    // Topic_Partition Key, Last Max offset inserted
    private final Map<TopicPartition, OffsetAndMetadata> lastInsertedOffsets = new ConcurrentHashMap<>();


    @Override
    public void setStateRecord(StateRecord stateRecord) {
        onStateUpdate(stateRecord);
    }

    @Override
    public void onStateUpdate(StateRecord stateRecord) {
        if (stateRecord.getState() == AFTER_PROCESSING) {
            lastInsertedOffsets.put(new TopicPartition(stateRecord.getTopic(), stateRecord.getPartition()),
                    new OffsetAndMetadata(stateRecord.getMaxOffset() + 1)); // +1 to store next record to send
        }
    }

    public void onPartitionRemoved(Collection<TopicPartition> removedPartitions) {
        removedPartitions.stream().forEach(lastInsertedOffsets::remove);
    }

    public Map<TopicPartition, OffsetAndMetadata> getLastInsertedOffsetsSnapshot() {
        return new HashMap<>(lastInsertedOffsets);
    }
}
