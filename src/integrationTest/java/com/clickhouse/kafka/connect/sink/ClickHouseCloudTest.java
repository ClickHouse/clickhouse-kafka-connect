package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class ClickHouseCloudTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseCloudTest.class);
    private static final Properties properties = System.getProperties();

    private ClickHouseHelperClient createClient(Map<String,String> props) {
        ClickHouseSinkConfig csc = new ClickHouseSinkConfig(props);

        String hostname = csc.getHostname();
        int port = csc.getPort();
        String database = csc.getDatabase();
        String username = csc.getUsername();
        String password = csc.getPassword();
        boolean sslEnabled = csc.isSslEnabled();
        int timeout = csc.getTimeout();


        return new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port, csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
                .setDatabase(database)
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .build();
    }


    private Map<String, String> getTestProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, String.valueOf(properties.getOrDefault("clickhouse.host", "clickhouse")));
        props.put(ClickHouseSinkConnector.PORT, String.valueOf(properties.getOrDefault("clickhouse.port", ClickHouseProtocol.HTTP.getDefaultPort())));
        props.put(ClickHouseSinkConnector.DATABASE, String.valueOf(properties.getOrDefault("clickhouse.database", "default")));
        props.put(ClickHouseSinkConnector.USERNAME, String.valueOf(properties.getOrDefault("clickhouse.username", "default")));
        props.put(ClickHouseSinkConnector.PASSWORD, String.valueOf(properties.getOrDefault("clickhouse.password", "")));
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "true");
        return props;
    }



    @Test
    public void overlappingDataTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = createClient(props);
        String topic = "schemaless_overlap_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `indexCount` Int64, `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, " +
                "`p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = ReplicatedMergeTree ORDER BY off16");
        Collection<SinkRecord> sr = SchemalessTestData.createPrimitiveTypes(topic, 1);
        Collection<SinkRecord> firstBatch = new ArrayList<>();
        Collection<SinkRecord> secondBatch = new ArrayList<>();
        Collection<SinkRecord> thirdBatch = new ArrayList<>();

        //For the sake of the comments, assume size = 100
        int firstBatchEndIndex = sr.size() / 2; // 0 - 50
        int secondBatchStartIndex = firstBatchEndIndex - sr.size() / 4; // 25
        int secondBatchEndIndex = firstBatchEndIndex + sr.size() / 4; // 75

        for (SinkRecord record : sr) {
            if (record.kafkaOffset() <= firstBatchEndIndex) {
                firstBatch.add(record);
            }

            if (record.kafkaOffset() >= secondBatchStartIndex && record.kafkaOffset() <= secondBatchEndIndex) {
                secondBatch.add(record);
            }

            if (record.kafkaOffset() >= secondBatchStartIndex) {
                thirdBatch.add(record);
            }
        }

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(firstBatch);
        chst.stop();
        chst.start(props);
        chst.put(secondBatch);
        chst.stop();
        chst.start(props);
        chst.put(thirdBatch);
        chst.stop();
        LOGGER.info("Total Records: {}", sr.size());
        LOGGER.info("Row Count: {}", ClickHouseTestHelpers.countRows(chc, topic));
        assertTrue(ClickHouseTestHelpers.countRows(chc, topic) >= sr.size());
        assertTrue(ClickHouseTestHelpers.checkSequentialRows(chc, topic, sr.size()));
        ClickHouseTestHelpers.dropTable(chc, topic);
    }
}
