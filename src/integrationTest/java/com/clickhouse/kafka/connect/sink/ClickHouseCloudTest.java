package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseDeploymentType;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.sink.helper.SchemalessTestData;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Assumptions;

/**
 * NOTE: this test does NOT run against cluster or standalone ClickHouse.
 */
public class ClickHouseCloudTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseCloudTest.class);
    private static final Properties properties = System.getProperties();
    private static final boolean isCluster = ClickHouseTestHelpers.isCluster();
    private static final boolean isCloud = ClickHouseTestHelpers.isCloud();

    private Map<String, String> getTestProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, properties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_HOST_SYSTEM_PROP));
        props.put(ClickHouseSinkConnector.PORT, properties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PORT_SYSTEM_PROP));
        props.put(ClickHouseSinkConnector.DATABASE, ClickHouseTestHelpers.DATABASE_DEFAULT);
        props.put(ClickHouseSinkConnector.USERNAME, ClickHouseTestHelpers.USERNAME_DEFAULT);
        props.put(ClickHouseSinkConnector.PASSWORD, properties.getProperty(ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PASSWORD_SYSTEM_PROP));
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "true");
        return props;
    }

    private static boolean checkSequentialRows(ClickHouseHelperClient chc, String tableName, int totalRecords) {
        String queryCount = String.format("SELECT DISTINCT `off16` FROM `%s` ORDER BY `off16` ASC", tableName);
        try (Records records = chc.queryV2(queryCount)) {
            int expectedIndexCount = 0;
            for (GenericRecord record : records) {
                int currentIndexCount = record.getInteger(1);
                if (currentIndexCount != expectedIndexCount) {
                    LOGGER.error("currentIndexCount: {}, expectedIndexCount: {}", currentIndexCount, expectedIndexCount);
                    return false;
                }
                expectedIndexCount++;
            }

            LOGGER.info("Total Records: {}, expectedIndexCount: {}", totalRecords, expectedIndexCount);
            return totalRecords == expectedIndexCount;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    public static void checkPropsExist() {
        Assumptions.assumeFalse(isCluster, "Cloud tests are not supported in cluster mode");
        Assumptions.assumeTrue(isCloud, "Cloud tests are not supported in standalone mode");
        ClickHouseTestHelpers.logAndThrowIfCloudPropNotExists(LOGGER, properties, ClickHouseTestHelpers.CLICKHOUSE_CLOUD_HOST_SYSTEM_PROP);
        ClickHouseTestHelpers.logAndThrowIfCloudPropNotExists(LOGGER, properties, ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PORT_SYSTEM_PROP);
        ClickHouseTestHelpers.logAndThrowIfCloudPropNotExists(LOGGER, properties, ClickHouseTestHelpers.CLICKHOUSE_CLOUD_PASSWORD_SYSTEM_PROP);
    }

    @Test
    public void overlappingDataTest() {
        Map<String, String> props = getTestProperties();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = "schemaless_overlap_table_test";
        ClickHouseTestHelpers.dropTable(chc, topic, ClickHouseDeploymentType.CLOUD);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16").column("str", "String")
                .column("p_int8", "Int8").column("p_int16", "Int16").column("p_int32", "Int32")
                .column("p_int64", "Int64").column("p_float32", "Float32")
                .column("p_float64", "Float64").column("p_bool", "Bool")
                .orderByColumn("off16")
                .deploymentType(ClickHouseDeploymentType.CLOUD)
                .execute(chc);
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
        LOGGER.info("Row Count: {}", ClickHouseTestHelpers.countRows(chc, topic, ClickHouseDeploymentType.CLOUD));
        Assertions.assertTrue(ClickHouseTestHelpers.countRows(chc, topic, ClickHouseDeploymentType.CLOUD) >= sr.size());
        Assertions.assertTrue(checkSequentialRows(chc, topic, sr.size()));
        ClickHouseTestHelpers.dropTable(chc, topic, ClickHouseDeploymentType.CLOUD);
    }
}
