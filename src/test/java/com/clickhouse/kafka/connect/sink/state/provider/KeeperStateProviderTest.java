package com.clickhouse.kafka.connect.sink.state.provider;

import com.clickhouse.kafka.connect.sink.ClickHouseBase;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.StateRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KeeperStateProviderTest extends ClickHouseBase {
    private static final String TOPIC = "orders";
    private static final int PARTITION = 3;

    // Two tasks that both believe they own the partition share one state table.
    private StateProvider pausedWorker;
    private StateProvider newOwner;

    @BeforeEach
    void createWorkersSharingOneStateTable() {
        Map<String, String> props = getBaseProps();
        String suffix = UUID.randomUUID().toString().replace("-", "");
        props.put(ClickHouseSinkConfig.ZK_PATH, "/kafka-connect-" + suffix);
        props.put(ClickHouseSinkConfig.ZK_DATABASE, "connect_state_" + suffix);
        pausedWorker = new KeeperStateProvider(new ClickHouseSinkConfig(props));
        newOwner = new KeeperStateProvider(new ClickHouseSinkConfig(props));
    }

    private static StateRecord state(long minOffset, long maxOffset, State state) {
        return new StateRecord(TOPIC, PARTITION, maxOffset, minOffset, state, TOPIC);
    }

    @Test
    @DisplayName("compareAndSet stores the first state of a partition when nothing is stored yet")
    void compareAndSetStoresFirstStateWhenNothingIsStored() {
        StateRecord nothingStored = pausedWorker.getStateRecord(TOPIC, PARTITION);
        assertEquals(State.NONE, nothingStored.getState());

        assertTrue(pausedWorker.compareAndSetStateRecord(nothingStored, state(100, 129, State.BEFORE_PROCESSING)));

        assertEquals(state(100, 129, State.BEFORE_PROCESSING), newOwner.getStateRecord(TOPIC, PARTITION));
    }

    @Test
    @DisplayName("compareAndSet replaces the stored state when it still equals the expected one")
    void compareAndSetReplacesStateThatStillEqualsExpected() {
        pausedWorker.setStateRecord(state(100, 129, State.AFTER_PROCESSING));

        assertTrue(pausedWorker.compareAndSetStateRecord(
                state(100, 129, State.AFTER_PROCESSING), state(130, 159, State.BEFORE_PROCESSING)));

        assertEquals(state(130, 159, State.BEFORE_PROCESSING), newOwner.getStateRecord(TOPIC, PARTITION));
    }

    @Test
    @DisplayName("compareAndSet writes nothing when another task changed the state since it was read")
    void compareAndSetWritesNothingWhenStateChangedSinceItWasRead() {
        pausedWorker.setStateRecord(state(100, 129, State.AFTER_PROCESSING));
        StateRecord readBeforePause = pausedWorker.getStateRecord(TOPIC, PARTITION);
        newOwner.setStateRecord(state(130, 180, State.AFTER_PROCESSING));

        assertFalse(pausedWorker.compareAndSetStateRecord(readBeforePause, state(130, 159, State.BEFORE_PROCESSING)));

        assertEquals(state(130, 180, State.AFTER_PROCESSING), newOwner.getStateRecord(TOPIC, PARTITION));
    }

    @Test
    @DisplayName("compareAndSet writes nothing when another task stored the first state since it was read")
    void compareAndSetWritesNothingWhenFirstStateWasStoredSinceItWasRead() {
        StateRecord nothingStored = pausedWorker.getStateRecord(TOPIC, PARTITION);
        newOwner.setStateRecord(state(0, 50, State.AFTER_PROCESSING));

        assertFalse(pausedWorker.compareAndSetStateRecord(nothingStored, state(0, 29, State.BEFORE_PROCESSING)));

        assertEquals(state(0, 50, State.AFTER_PROCESSING), newOwner.getStateRecord(TOPIC, PARTITION));
    }
}
