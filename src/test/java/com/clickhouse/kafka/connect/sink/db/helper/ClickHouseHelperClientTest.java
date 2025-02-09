package com.clickhouse.kafka.connect.sink.db.helper;

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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    public void showTables() {
        String topic = createTopicName("simple_table_test");
        ClickHouseTestHelpers.createTable(chc, topic,
                "CREATE TABLE %s ( `num` String ) Engine = MergeTree ORDER BY num");
        try {
            List<Table> table = chc.showTables(chc.getDatabase());
            List<String> tableNames = table.stream().map(item -> item.getCleanName()).collect(Collectors.toList());
            Assertions.assertTrue(tableNames.contains(topic));
        } finally {
            ClickHouseTestHelpers.dropTable(chc, topic);
        }
    }

    @Test
    public void describeNestedFlattenedTable() {
        String topic = createTopicName("nested_flattened_table_test");
        ClickHouseTestHelpers.createTable(chc, topic,
                "CREATE TABLE %s ( `num` String, " +
                        "`nested` Nested (innerInt Int32, innerString String)) " +
                        "Engine = MergeTree ORDER BY num");

        try {
            Table table = chc.describeTable(chc.getDatabase(), topic);
            Assertions.assertEquals(3, table.getRootColumnsList().size());
        } finally {
            ClickHouseTestHelpers.dropTable(chc, topic);
        }
    }

    @Test
    public void ignoreArrayWithNestedTable() {
        String topic = createTopicName("nested_table_test");
        ClickHouseTestHelpers.createTable(chc, topic,
                "CREATE TABLE %s ( `num` String, " +
                        "`nested` Array(Nested (innerInt Int32, innerString String))) " +
                        "Engine = MergeTree ORDER BY num");

        try {
            Table table = chc.describeTable(chc.getDatabase(), topic);
            Assertions.assertNull(table);
        } finally {
            ClickHouseTestHelpers.dropTable(chc, topic);
        }
    }

    @Test
    @SinceClickHouseVersion("24.1")
    public void describeNestedUnFlattenedTable() {
        String nestedTopic = createTopicName("nested_unflattened_table_test");
        String normalTopic = createTopicName("normal_unflattened_table_test");
        ClickHouseTestHelpers.query(chc, "CREATE USER IF NOT EXISTS unflatten IDENTIFIED BY '123FOURfive^&*91011' SETTINGS flatten_nested=0");
        ClickHouseTestHelpers.query(chc, "GRANT CURRENT GRANTS ON *.* TO unflatten");

        Map<String, String> props = createProps();
        props.put("username", "unflatten");
        props.put("password", "123FOURfive^&*91011");
        chc = createClient(props);

        ClickHouseTestHelpers.createTable(chc, nestedTopic,
                "CREATE TABLE %s ( `num` String, " +
                        "`nested` Nested (innerInt Int32, innerString String)) " +
                        "Engine = MergeTree ORDER BY num");
        ClickHouseTestHelpers.createTable(chc, normalTopic,
                "CREATE TABLE %s ( `num` String ) " +
                        "Engine = MergeTree ORDER BY num");

        try {
            Table nestedTable = chc.describeTable(chc.getDatabase(), nestedTopic);
            Assertions.assertNull(nestedTable);

            Table normalTable = chc.describeTable(chc.getDatabase(), normalTopic);
            Assertions.assertEquals(1, normalTable.getRootColumnsList().size());
        } finally {
            ClickHouseTestHelpers.dropTable(chc, nestedTopic);
            ClickHouseTestHelpers.dropTable(chc, normalTopic);
            ClickHouseTestHelpers.query(chc, "DROP USER IF EXISTS unflatten");
        }
    }
}
