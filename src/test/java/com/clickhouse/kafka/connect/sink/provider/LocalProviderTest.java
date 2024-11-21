package com.clickhouse.kafka.connect.sink.provider;


import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.StateRecord;
import com.clickhouse.kafka.connect.sink.state.provider.InMemoryState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class LocalProviderTest {

    @Test
    @DisplayName("Set & get state record")
    public void setAndGet() {
        StateProvider stateProvider = new InMemoryState();
        stateProvider.setStateRecord(new StateRecord("test", 1, 10, 0, State.BEFORE_PROCESSING));
        StateRecord stateRecord = stateProvider.getStateRecord("test", 1);
        assertEquals(10 , stateRecord.getMaxOffset());
        assertEquals(0 , stateRecord.getMinOffset());
        assertEquals(State.BEFORE_PROCESSING , stateRecord.getState());
    }

}
