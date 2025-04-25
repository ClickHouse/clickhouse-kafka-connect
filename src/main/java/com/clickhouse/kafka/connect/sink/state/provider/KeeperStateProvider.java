package com.clickhouse.kafka.connect.sink.state.provider;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.StateRecord;
import com.clickhouse.kafka.connect.util.Mask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeeperStateProvider implements StateProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeeperStateProvider.class);
    private ClickHouseNode server = null;
    private int pingTimeOut = 100;


    private ClickHouseHelperClient chc = null;
    private ClickHouseSinkConfig csc = null;

    private Map<String, StateRecord> stateMap = null;

    public KeeperStateProvider(ClickHouseSinkConfig csc) {
        this.csc = csc;
        this.stateMap = new ConcurrentHashMap<>();

        String hostname = csc.getHostname();
        int port = csc.getPort();
        String database = csc.getDatabase();
        String username = csc.getUsername();
        String password = csc.getPassword();
        boolean sslEnabled = csc.isSslEnabled();
        String jdbcConnectionProperties = csc.getJdbcConnectionProperties();
        int timeout = csc.getTimeout();
        String clientVersion = csc.getClientVersion();
        boolean useClientV2 = clientVersion.equals("V1") ? false : true;
        LOGGER.info(String.format("hostname: [%s] port [%d] database [%s] username [%s] password [%s] sslEnabled [%s] timeout [%d]", hostname, port, database, username, Mask.passwordMask(password), sslEnabled, timeout));

        chc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port, csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
                .setDatabase(database)
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setJdbcConnectionProperties(jdbcConnectionProperties)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .useClientV2(useClientV2)
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

    private void init() {
        String createTable = String.format("CREATE TABLE IF NOT EXISTS `%s`%s " +
                "(`key` String, minOffset BIGINT, maxOffset BIGINT, state String)" +
                " ENGINE=KeeperMap('%s') PRIMARY KEY `key`;",
                csc.getZkDatabase(),
                csc.getKeeperOnCluster().isEmpty() ? "" : " ON CLUSTER " + csc.getKeeperOnCluster(),
                csc.getZkPath());
        // TODO: exec instead of query
        if (chc.isUseClientV2()) {
            chc.queryV2(createTable);
        } else {
            ClickHouseResponse r = chc.queryV1(createTable);
            r.close();
        }
    }

    @Override
    public StateRecord getStateRecord(String topic, int partition) {
        String key = String.format("%s-%d", topic, partition);
        String selectStr = String.format("SELECT * FROM `%s` WHERE `key`= '%s'", csc.getZkDatabase(), key);
        try (ClickHouseClient client = ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
             ClickHouseResponse response = client.read(chc.getServer()) // or client.connect(endpoints)
                     .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                     .query(selectStr)
                     .executeAndWait()) {
            LOGGER.debug("return size: {}", response.getSummary().getReadRows());
            long totalResultsFound = response.getSummary().getResultRows();
            if ( totalResultsFound == 0) {
                LOGGER.info("Read state record: topic {} partition {} with NONE state", topic, partition);
                return new StateRecord(topic, partition, 0, 0, State.NONE);
            } else if(totalResultsFound > 1){
                LOGGER.warn("There was more than 1 state records for query: {} ({} found)", selectStr, totalResultsFound);
            }

            ClickHouseRecord r = response.firstRecord();
            long minOffset = r.getValue(1).asLong();
            long maxOffset = r.getValue(2).asLong();
            State state = State.valueOf(r.getValue(3).asString());
            LOGGER.debug("read state record: topic {} partition {} with {} state max {} min {}", topic, partition, state, maxOffset, minOffset);

            StateRecord stateRecord = new StateRecord(topic, partition, maxOffset, minOffset, state);
            StateRecord storedRecord = stateMap.get(csc.getZkDatabase() + "-" + key);
            if (storedRecord != null && !stateRecord.equals(storedRecord)) {
                LOGGER.warn("State record is changed: {} -> {}", storedRecord, stateRecord);
            } else {
                LOGGER.debug("State record stored: {}", storedRecord);
            }
            return stateRecord;
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setStateRecord(StateRecord stateRecord) {
        long minOffset = stateRecord.getMinOffset();
        long maxOffset = stateRecord.getMaxOffset();
        String key = stateRecord.getTopicAndPartitionKey();
        String state = stateRecord.getState().toString();
        String insertStr = String.format("INSERT INTO `%s` SETTINGS wait_for_async_insert=1 VALUES ('%s', %d, %d, '%s');", csc.getZkDatabase(), key, minOffset, maxOffset, state);
        LOGGER.info("Write state record: {}", stateRecord);
        if (chc.isUseClientV2()) {
            try (Records records = this.chc.queryV2(insertStr)) {
                LOGGER.debug("Number of written rows (V2) [{}]", records.getWrittenRows());
            } catch (Exception e) {
                LOGGER.error("Failed to write state record: {}", stateRecord, e);
                throw new RuntimeException(e);
            }
        } else {
            ClickHouseResponse response = this.chc.queryV1(insertStr);
            LOGGER.debug("Number of written rows (V1) [{}]", response.getSummary().getWrittenRows());
            response.close();
        }

        stateMap.put(csc.getZkDatabase() + "-" + key, stateRecord);
    }
}
