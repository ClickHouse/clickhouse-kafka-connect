package com.clickhouse.kafka.connect.sink.state;

import org.apache.kafka.connect.errors.RetriableException;

/**
 * The stored state of a topic partition changed after this task read it, i.e. another task is
 * processing the partition. Retriable so that the framework polls again, which is where a task
 * that was paused past the session timeout learns that its partitions were revoked.
 */
public class StateConflictException extends RetriableException {
    public StateConflictException(String message) {
        super(message);
    }
}
