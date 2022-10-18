package com.clickhouse.kafka.connect.sink.state.provider;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.db.ClickHouseWriter;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.StateRecord;
import com.clickhouse.kafka.connect.util.Mask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KeeperStateProvider implements StateProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeeperStateProvider.class);
    private ClickHouseNode server = null;
    private int pingTimeOut = 100;


    private ClickHouseHelperClient chc = null;

    /*
    create table connect_state (`key` String,  minOffset BIGINT, maxOffset BIGINT, state String) ENGINE=KeeperMap('/kafka-coonect', 'localhost:9181') PRIMARY KEY `key`;
     */
    public KeeperStateProvider(ClickHouseSinkConfig csc) {

        String hostname = csc.getHostname();
        int port = csc.getPort();
        String database = csc.getDatabase();
        String username = csc.getUsername();
        String password = csc.getPassword();
        boolean sslEnabled = csc.isSslEnabled();
        int timeout = csc.getTimeout();

        LOGGER.info(String.format("hostname: [%s] port [%d] database [%s] username [%s] password [%s] sslEnabled [%s] timeout [%d]", hostname, port, database, username, Mask.passwordMask(password), sslEnabled, timeout));

        chc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port)
                .setDatabase(database)
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .build();

        if (!chc.ping()) {
            LOGGER.error("Unable to ping Clickhouse server.");
            // TODO: exit
        }
        LOGGER.info("Ping is successful.");
        init();
    }

    public KeeperStateProvider(ClickHouseHelperClient chc) {
        if (!chc.ping())
            throw new RuntimeException("ping");
        this.chc = chc;
        init();
    }

    private boolean init() {
        return true;
    }

    @Override
    public StateRecord getStateRecord(String topic, int partition) {
        //SELECT * from connect_state where `key`= ''
        String key = String.format("%s-%d", topic, partition);
        String selectStr = String.format("SELECT * from connect_state where `key`= '%s'", key);
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(chc.getServer()) // or client.connect(endpoints)
                     .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                     .query(selectStr)
                     .executeAndWait()) {
            LOGGER.debug("return size: " + response.getSummary().getReadRows());
            if ( response.getSummary().getReadRows() == 0) {
                LOGGER.info(String.format("read state record: topic %s partition %s with NONE state", topic, partition));
                return new StateRecord(topic, partition, 0, 0, State.NONE);
            }
            ClickHouseRecord r = response.firstRecord();
            long minOffset = r.getValue(1).asLong();
            long maxOffset = r.getValue(2).asLong();
            State state = State.valueOf(r.getValue(3).asString());
            LOGGER.debug(String.format("read state record: topic %s partition %s with %s state max %d min %d", topic, partition, state, maxOffset, minOffset));
            return new StateRecord(topic, partition, maxOffset, minOffset, state);
        } catch (ClickHouseException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setStateRecord(StateRecord stateRecord) {
        long minOffset = stateRecord.getMinOffset();
        long maxOffset = stateRecord.getMaxOffset();
        String key = stateRecord.getTopicAndPartitionKey();
        String state = stateRecord.getState().toString();
        String insertStr = String.format("INSERT INTO connect_state values ('%s', %d, %d, '%s');", key, minOffset, maxOffset, state);
        ClickHouseResponse response = this.chc.query(insertStr);
        LOGGER.info(String.format("write state record: topic %s partition %s with %s state max %d min %d", stateRecord.getTopic(), stateRecord.getPartition(), state, maxOffset, minOffset));
        LOGGER.debug(String.format("Number of written rows [%d]", response.getSummary().getWrittenRows()));
    }
}
