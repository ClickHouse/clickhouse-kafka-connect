package com.clickhouse.kafka.connect.sink.state.provider;

import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.StateRecord;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.Optional;

public class RedisStateProvider implements StateProvider {

    private RedisClient redisClient = null;
    private RedisCommands<String, String> syncCommands = null;
    public RedisStateProvider(String host, int port, Optional<String> password) {
        String url = null;
        if (password.isPresent())
            url = String.format("redis://%s@%s:%d/0", password.get(), host, port);
        else
            url = String.format("redis://%s:%d/0", host, port);

        redisClient = RedisClient.create(url);
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        syncCommands = connection.sync();
    }

    @Override
    public StateRecord getStateRecord(String topic, int partition) {
        String value = syncCommands.get(String.format("%s-%d", topic, partition));

        String [] values = value.split("-");

        long maxOffset = Long.valueOf(values[0]).longValue();
        long minOffset = Long.valueOf(values[1]).longValue();
        State state = State.valueOf(values[2]);
        return new StateRecord(topic, partition, maxOffset, minOffset, state);

    }
    @Override
    public void setStateRecord(StateRecord stateRecord) {
        String key = String.format("%s-%d", stateRecord.getTopic(), stateRecord.getPartition());
        String value = String.format("%d-%d-%s", stateRecord.getMaxOffset(), stateRecord.getMinOffset(), stateRecord.getState().toString());
        syncCommands.set(key,value);
    }

}
