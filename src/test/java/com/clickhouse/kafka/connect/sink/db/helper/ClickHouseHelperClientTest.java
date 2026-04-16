package com.clickhouse.kafka.connect.sink.db.helper;

import com.clickhouse.kafka.connect.sink.ClickHouseBase;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseDeploymentType;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.test.junit.extension.FromVersionConditionExtension;
import com.clickhouse.kafka.connect.test.junit.extension.SinceClickHouseVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ExtendWith(FromVersionConditionExtension.class)
public class ClickHouseHelperClientTest extends ClickHouseBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseHelperClientTest.class);

    private static final CreateTableStatement SINGLE_NUM_TABLE = new CreateTableStatement()
            .column("num", "String")
            .orderByColumn("num");

    ClickHouseHelperClient chc = null;

    @BeforeEach
    public void setUp() {
        LOGGER.info("Setting up...");
        Map<String, String> props = getBaseProps();
        chc = ClickHouseTestHelpers.createClient(props);
    }

    @Test
    public void ping() {
        Assertions.assertTrue(chc.ping());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void showTables(ClickHouseDeploymentType deploymentType) {
        String topic = createTopicName("simple_table_test");
        new CreateTableStatement(SINGLE_NUM_TABLE).tableName(topic).deploymentType(deploymentType).execute(chc);
        try {
            List<Table> table = chc.showTables(chc.getDatabase());
            List<String> tableNames = table.stream().map(Table::getCleanName).collect(Collectors.toList());
            Assertions.assertTrue(tableNames.contains(topic));
        } finally {
            ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void describeNestedFlattenedTable(ClickHouseDeploymentType deploymentType) {
        String topic = createTopicName("nested_flattened_table_test");
        new CreateTableStatement()
                .tableName(topic)
                .column("num", "String")
                .column("nested", "Nested (innerInt Int32, innerString String)")
                .deploymentType(deploymentType)
                .orderByColumn("num").execute(chc);

        try {
            Table table = chc.describeTable(chc.getDatabase(), topic);
            Assertions.assertEquals(3, table.getRootColumnsList().size());
        } finally {
            ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void ignoreArrayWithNestedTable(ClickHouseDeploymentType deploymentType) {
        String topic = createTopicName("nested_table_test");
        new CreateTableStatement()
                .tableName(topic)
                .column("num", "String")
                .column("nested", "Array(Nested (innerInt Int32, innerString String))")
                .deploymentType(deploymentType)
                .orderByColumn("num").execute(chc);

        try {
            Table table = chc.describeTable(chc.getDatabase(), topic);
            Assertions.assertNull(table);
        } finally {
            ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    @SinceClickHouseVersion("24.1")
    public void describeNestedUnFlattenedTable(ClickHouseDeploymentType deploymentType) {
        String nestedTopic = createTopicName("nested_unflattened_table_test");
        String normalTopic = createTopicName("normal_unflattened_table_test");
        String testUsername = createTestUsername("unflatten");
        ClickHouseHelperClient adminChc = chc;
        ClickHouseTestHelpers.executeQueryIgnoreResult(adminChc, String.format("CREATE USER IF NOT EXISTS `%s` IDENTIFIED BY '123FOURfive^&*91011' SETTINGS flatten_nested=0", testUsername));
        ClickHouseTestHelpers.executeQueryIgnoreResult(adminChc, String.format("GRANT CURRENT GRANTS ON *.* TO `%s`", testUsername));

        Map<String, String> props = getBaseProps();
        props.put("username", testUsername);
        props.put("password", "123FOURfive^&*91011");
        chc = ClickHouseTestHelpers.createClient(props);

        new CreateTableStatement()
                .tableName(nestedTopic)
                .column("num", "String")
                .column("nested", "Nested (innerInt Int32, innerString String)")
                .deploymentType(deploymentType)
                .orderByColumn("num").execute(chc);
        new CreateTableStatement(SINGLE_NUM_TABLE).tableName(normalTopic).deploymentType(deploymentType).execute(chc);

        try {
            Table nestedTable = chc.describeTable(chc.getDatabase(), nestedTopic);
            Assertions.assertNull(nestedTable);

            Table normalTable = chc.describeTable(chc.getDatabase(), normalTopic);
            Assertions.assertEquals(1, normalTable.getRootColumnsList().size());
        } finally {
            ClickHouseTestHelpers.dropTable(chc, nestedTopic, deploymentType);
            ClickHouseTestHelpers.dropTable(chc, normalTopic, deploymentType);
            ClickHouseTestHelpers.executeQueryIgnoreResult(adminChc, String.format("DROP USER IF EXISTS `%s`", testUsername));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void ignoreSubColumnsOfAliasEphemeralAndMaterialized(ClickHouseDeploymentType deploymentType) {
        String topic = createTopicName("alias_ephemeral_subcol_test");

        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("null_str_alias", "Nullable(String) ALIAS formatReadableSize(`off16`)")
                .column("null_str_eph", "Nullable(String) EPHEMERAL")
                .column("null_str_mat", "Nullable(String) MATERIALIZED formatReadableSize(`off16`)")
                .column("arr_eph", "Array(Array(Array(UInt32))) EPHEMERAL")
                .column("tuple_eph", "Tuple(s String, i Int64) EPHEMERAL")
                .column("map_eph", "Map(String, UInt64) EPHEMERAL")
                .column("nested_eph", "Nested(ID UInt32, Serial UInt32, InnerNested Nested(InnerId UInt32)) EPHEMERAL")
                .deploymentType(deploymentType)
                .orderByColumn("off16").execute(chc);

        try {
            Table table = chc.describeTable(chc.getDatabase(), topic);
            Assertions.assertEquals(1, table.getAllColumnsMap().size());
            Assertions.assertEquals(1, table.getAllColumnsList().size());
            Assertions.assertEquals(1, table.getRootColumnsList().size());
            Assertions.assertEquals(1, table.getRootColumnsMap().size());
            Assertions.assertEquals("off16", table.getAllColumnsList().get(0).getName());
            Assertions.assertEquals("off16", table.getRootColumnsList().get(0).getName());
        } finally {
            ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        }
    }
}
