package com.clickhouse.kafka.connect.sink.state.provider;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
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
    public KeeperStateProvider(Map<String, String> props) {
        String hostname = props.get(ClickHouseSinkConnector.HOSTNAME);
        int port = Integer.valueOf(props.get(ClickHouseSinkConnector.PORT)).intValue();
        String database = props.get(ClickHouseSinkConnector.DATABASE);
        String username = props.get(ClickHouseSinkConnector.USERNAME);
        String password = props.get(ClickHouseSinkConnector.PASSWORD);
        String sslEnabled = props.get(ClickHouseSinkConnector.SSL_ENABLED);

        LOGGER.info(String.format("hostname: [%s] port [%d] database [%s] username [%s] password [%s]", hostname, port, database, username, password));

        String protocol = "http";
        if (Boolean.getBoolean(sslEnabled) == true )
            protocol += "s";

        String url = String.format("%s://%s:%d/%s", protocol, hostname, port, database);

        LOGGER.info("url: " + url);

        if (username != null && password != null) {
            LOGGER.info(String.format("Adding username [%s] password [%s]  ", username, Mask.passwordMask(password)));
            Map<String, String> options = new HashMap<>();
            options.put("user", username);
            options.put("password", password);
            server = ClickHouseNode.of(url, options);
        } else {
            server = ClickHouseNode.of(url);
        }

        ClickHouseClient clientPing = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);

        if (clientPing.ping(server, pingTimeOut)) {
            LOGGER.info("Ping is successful.");
        }


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
            ClickHouseRecord r = response.firstRecord();
            long minOffset = r.getValue(1).asLong();
            long maxOffset = r.getValue(2).asLong();
            State state = State.valueOf(r.getValue(3).asString());
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
        LOGGER.debug(String.format("Number of written rows [%d]", response.getSummary().getWrittenRows()));
    }
}
