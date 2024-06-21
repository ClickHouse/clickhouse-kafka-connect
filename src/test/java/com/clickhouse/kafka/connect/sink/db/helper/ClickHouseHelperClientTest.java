package com.clickhouse.kafka.connect.sink.db.helper;

import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.kafka.connect.sink.ClickHouseBase;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.junit.extension.FromVersionConditionExtension;
import com.clickhouse.kafka.connect.sink.junit.extension.SinceClickHouseVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@ExtendWith(FromVersionConditionExtension.class)
public class ClickHouseHelperClientTest extends ClickHouseBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseHelperClientTest.class);
    ClickHouseHelperClient chc = null;

    @BeforeEach
    public void setUp() {
        LOGGER.info("Setting up...");
        Map<String, String> props = createProps();
        chc = createClient(props);
    }

    @Test
    public void ping() {
        Assertions.assertTrue(chc.ping());
    }

    @Test
    public void describeNestedFlattenedTable() {
        String topic = createTopicName("nested_flattened_table_test");
        ClickHouseTestHelpers.createTable(chc, topic,
                "CREATE TABLE %s ( `num` String, " +
                        "`nested` Nested (innerInt Int32, innerString String)) " +
                        "Engine = MergeTree ORDER BY num");

        try {
            Table table = chc.describeTable(topic);
            Assertions.assertEquals(3, table.getRootColumnsList().size());
        } finally {
            ClickHouseTestHelpers.dropTable(chc, topic);
        }
    }

    @Test
    @SinceClickHouseVersion("24.1")
    public void describeNestedUnFlattenedTable() {
        String nestedTopic = createTopicName("nested_unflattened_table_test");
        String normalTopic = createTopicName("normal_unflattened_table_test");
        ClickHouseTestHelpers.query(chc, "CREATE USER unflatten IDENTIFIED BY '' SETTINGS flatten_nested=0");
        ClickHouseTestHelpers.query(chc, "GRANT CURRENT GRANTS ON *.* TO unflatten");

        Map<String, String> props = createProps();
        props.put("username", "unflatten");
        chc = createClient(props);

        ClickHouseTestHelpers.createTable(chc, nestedTopic,
                "CREATE TABLE %s ( `num` String, " +
                        "`nested` Nested (innerInt Int32, innerString String)) " +
                        "Engine = MergeTree ORDER BY num");
        ClickHouseTestHelpers.createTable(chc, normalTopic,
                "CREATE TABLE %s ( `num` String ) " +
                        "Engine = MergeTree ORDER BY num");

        try {
            Table nestedTable = chc.describeTable(nestedTopic);
            Assertions.assertNull(nestedTable);

            Table normalTable = chc.describeTable(normalTopic);
            Assertions.assertEquals(1, normalTable.getRootColumnsList().size());
        } finally {
            ClickHouseTestHelpers.dropTable(chc, nestedTopic);
            ClickHouseTestHelpers.dropTable(chc, normalTopic);
        }
    }
}
