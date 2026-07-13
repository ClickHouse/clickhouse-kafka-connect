package com.clickhouse.kafka.connect.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.Map;

/**
 * Encapsulates a single delivery semantic for {@link ClickHouseSinkTask}. Each
 * implementation owns its own buffering state (if any) and the offset-commit
 * contract that goes with it, so the three delivery modes — direct,
 * at-least-once buffered, exactly-once buffered — live in separate, cohesive
 * units rather than interleaved branches on the task.
 *
 * <p>Lifecycle ownership: {@code ClickHouseSinkTask} still owns the
 * {@code ProxySinkTask} (start/stop/onPartitionRemoved). A strategy is only
 * responsible for record handling, the {@code preCommit} offset contract, and
 * cleanup of its own buffered state on rebalance/stop.
 */
interface DeliveryStrategy {

    void put(Collection<SinkRecord> records);

    Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets);

    /** Drop any buffered state for revoked partitions. */
    void close(Collection<TopicPartition> partitions);

    /** Discard any remaining buffered state on task shutdown. */
    void stop();
}