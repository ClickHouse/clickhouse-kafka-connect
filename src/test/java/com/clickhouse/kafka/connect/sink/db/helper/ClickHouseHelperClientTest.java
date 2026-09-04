package com.clickhouse.kafka.connect.sink.db.helper;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.kafka.connect.sink.ClickHouseBase;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseCluster;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.test.junit.extension.FromVersionConditionExtension;
import com.clickhouse.kafka.connect.test.junit.extension.SinceClickHouseVersion;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

@ExtendWith(FromVersionConditionExtension.class)
public class ClickHouseHelperClientTest extends ClickHouseBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseHelperClientTest.class);

    private static final CreateTableStatement SINGLE_NUM_TABLE = new CreateTableStatement()
            .column("num", "String")
            .engine("MergeTree")
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

    @Test
    public void showTables() {
        String topic = createTopicName("simple_table_test");
        new CreateTableStatement(SINGLE_NUM_TABLE).tableName(topic).execute(chc);
        try {
            List<Table.TableDesc> tableDescs = chc.showTables(chc.getDatabase());
            List<String> tableNames = tableDescs.stream().map(Table.TableDesc::getCleanName).collect(Collectors.toList());
            Assertions.assertTrue(tableNames.contains(topic));
        } finally {
            ClickHouseTestHelpers.dropTable(chc, topic);
        }
    }

    @Test
    public void describeNestedFlattenedTable() {
        String topic = createTopicName("nested_flattened_table_test");
        new CreateTableStatement()
                .tableName(topic)
                .column("num", "String")
                .column("nested", "Nested (innerInt Int32, innerString String)")
                .engine("MergeTree").orderByColumn("num").execute(chc);

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
        new CreateTableStatement()
                .tableName(topic)
                .column("num", "String")
                .column("nested", "Array(Nested (innerInt Int32, innerString String))")
                .engine("MergeTree").orderByColumn("num").execute(chc);

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
        String testUsername = createTestUsername("unflatten");

        String randomNums = new Random().ints(5, 0, 9).mapToObj(String::valueOf).collect(Collectors.joining(""));
        String randomSpecialChar = List.of("!", "?", "^", "&", "*").get(new Random().nextInt(5));
        String testPassword = randomSpecialChar + RandomStringUtils.secure().nextAlphanumeric(15) + randomNums;
        String clusterClause = ClickHouseTestHelpers.getClusterClauseOrEmpty();
        ClickHouseHelperClient adminChc = chc;
        ClickHouseTestHelpers.executeQueryIgnoreResult(adminChc, String.format("CREATE USER IF NOT EXISTS `%s`%s IDENTIFIED BY '%s' SETTINGS flatten_nested=0", testUsername, clusterClause, testPassword));
        if (isCluster) {
            ClickHouseTestHelpers.executeQueryIgnoreResult(adminChc, String.format("GRANT%s CREATE ON *.* TO `%s`", clusterClause, testUsername));
            ClickHouseTestHelpers.executeQueryIgnoreResult(adminChc, String.format("GRANT%s DROP ON *.* TO `%s`", clusterClause, testUsername));
            ClickHouseTestHelpers.executeQueryIgnoreResult(adminChc, String.format("GRANT%s SHOW ON *.* TO `%s`", clusterClause, testUsername));
        } else {
            ClickHouseTestHelpers.executeQueryIgnoreResult(adminChc, String.format("GRANT CURRENT GRANTS ON *.* TO `%s`", testUsername));
        }

        Map<String, String> props = getBaseProps();
        props.put("username", testUsername);
        props.put("password", testPassword);
        chc = ClickHouseTestHelpers.createClient(props);

        new CreateTableStatement()
                .tableName(nestedTopic)
                .column("num", "String")
                .column("nested", "Nested (innerInt Int32, innerString String)")
                .engine("MergeTree").orderByColumn("num").execute(chc);
        new CreateTableStatement(SINGLE_NUM_TABLE).tableName(normalTopic).execute(chc);

        try {
            Table nestedTable = chc.describeTable(chc.getDatabase(), nestedTopic);
            Assertions.assertNull(nestedTable);

            Table normalTable = chc.describeTable(chc.getDatabase(), normalTopic);
            Assertions.assertEquals(1, normalTable.getRootColumnsList().size());
        } finally {
            ClickHouseTestHelpers.dropTable(adminChc, nestedTopic);
            ClickHouseTestHelpers.dropTable(adminChc, normalTopic);
            ClickHouseTestHelpers.executeQueryIgnoreResult(adminChc, String.format("DROP USER IF EXISTS `%s`%s", testUsername, clusterClause));
        }
    }

    /**
     * DESCRIBE TABLE drops alias, materialized and ephemeral columns and adds subcolumns that
     * {@code system.columns} does not report, so neither of a described {@link Table}'s column lists
     * matches the count {@link ClickHouseHelperClient#showTables} returns. Since
     * {@code extractTablesMapping} compares those two counts to decide whether a cached description is
     * stale, {@link Table#getNumColumns()} has to stay aligned with the listed count.
     */
    @Test
    public void describedColumnCountMatchesListedColumnCount() {
        String plainTopic = createTopicName("described_count_plain_test");
        String skippedColsTopic = createTopicName("described_count_skipped_cols_test");
        String subColsTopic = createTopicName("described_count_subcols_test");

        new CreateTableStatement()
                .tableName(plainTopic)
                .column("off16", "Int16")
                .column("str", "String")
                .engine("MergeTree").orderByColumn("off16").execute(chc);
        new CreateTableStatement()
                .tableName(skippedColsTopic)
                .column("off16", "Int16")
                .column("null_str_alias", "Nullable(String) ALIAS formatReadableSize(`off16`)")
                .column("null_str_eph", "Nullable(String) EPHEMERAL")
                .column("null_str_mat", "Nullable(String) MATERIALIZED formatReadableSize(`off16`)")
                .engine("MergeTree").orderByColumn("off16").execute(chc);
        new CreateTableStatement()
                .tableName(subColsTopic)
                .column("off16", "Int16")
                .column("null_str", "Nullable(String)")
                .column("map", "Map(String, UInt64)")
                .column("tuple", "Tuple(s String, i Int64)")
                .engine("MergeTree").orderByColumn("off16").execute(chc);

        try {
            Map<String, Integer> listedNumColumns = chc.showTables(chc.getDatabase()).stream()
                    .collect(Collectors.toMap(Table.TableDesc::getCleanName, Table.TableDesc::getNumColumns));

            for (String topic : List.of(plainTopic, skippedColsTopic, subColsTopic)) {
                Table table = chc.describeTable(chc.getDatabase(), topic);
                Assertions.assertNotNull(table, topic);
                Assertions.assertEquals(listedNumColumns.get(topic).intValue(), table.getNumColumns(),
                        String.format("Described and listed column counts disagree for %s, so extractTablesMapping "
                                + "cannot tell a stale description from a current one", topic));
            }

            // Neither column list can stand in for the count: one is short of it, the other overshoots
            Table skippedCols = chc.describeTable(chc.getDatabase(), skippedColsTopic);
            Assertions.assertEquals(1, skippedCols.getRootColumnsList().size());
            Assertions.assertEquals(4, skippedCols.getNumColumns());

            Table subCols = chc.describeTable(chc.getDatabase(), subColsTopic);
            Assertions.assertEquals(4, subCols.getNumColumns());
            Assertions.assertTrue(subCols.getAllColumnsList().size() > subCols.getNumColumns(),
                    "Expected subcolumns to make the full column list longer than the listed column count");
        } finally {
            ClickHouseTestHelpers.dropTable(chc, plainTopic);
            ClickHouseTestHelpers.dropTable(chc, skippedColsTopic);
            ClickHouseTestHelpers.dropTable(chc, subColsTopic);
        }
    }

    @Test
    public void ignoreSubColumnsOfAliasEphemeralAndMaterialized() {
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
                .engine("MergeTree").orderByColumn("off16").execute(chc);

        try {
            Table table = chc.describeTable(chc.getDatabase(), topic);
            Assertions.assertEquals(1, table.getAllColumnsMap().size());
            Assertions.assertEquals(1, table.getAllColumnsList().size());
            Assertions.assertEquals(1, table.getRootColumnsList().size());
            Assertions.assertEquals(1, table.getRootColumnsMap().size());
            Assertions.assertEquals("off16", table.getAllColumnsList().get(0).getName());
            Assertions.assertEquals("off16", table.getRootColumnsList().get(0).getName());
        } finally {
            ClickHouseTestHelpers.dropTable(chc, topic);
        }
    }

    private ClickHouseRequest<?> createMockRequest() {
        ClickHouseNode node = ClickHouseNode.builder()
                .host("localhost")
                .port(ClickHouseProtocol.HTTP, 8123)
                .build();
        return ClickHouseClient.newInstance(ClickHouseProtocol.HTTP).read(node);
    }

    @Test
    public void testSetReplicaTagHeaderV1_DisabledFeatureFlag() {
        ClickHouseHelperClient client = new ClickHouseHelperClient.ClickHouseClientBuilder("localhost", 8123, null, null, -1)
                .enableReplicaPinning(false)
                .build();
        client.pinReplica();
        ClickHouseRequest<?> req = createMockRequest();
        client.setReplicaTagHeaderV1(req);
        Assertions.assertFalse(req.hasOption(ClickHouseHttpOption.CUSTOM_HEADERS));
    }

    @Test
    public void testSetReplicaTagHeaderV1_NoPriorCustomHeaders() {
        ClickHouseHelperClient client = new ClickHouseHelperClient.ClickHouseClientBuilder("localhost", 8123, null, null, -1)
                .enableReplicaPinning(true)
                .build();
        client.pinReplica();
        ClickHouseRequest<?> req = createMockRequest();
        client.setReplicaTagHeaderV1(req);
        Assertions.assertTrue(req.hasOption(ClickHouseHttpOption.CUSTOM_HEADERS));
        String customHeaders = (String) req.getConfig().getOption(ClickHouseHttpOption.CUSTOM_HEADERS);
        Assertions.assertTrue(customHeaders.startsWith(ClickHouseHelperClient.REPLICA_TAG_HEADER + "="), "Got customHeaders: " + customHeaders);
    }

    @Test
    public void testSetReplicaTagHeaderV1_EmptyCustomHeaders() {
        ClickHouseHelperClient client = new ClickHouseHelperClient.ClickHouseClientBuilder("localhost", 8123, null, null, -1)
                .enableReplicaPinning(true)
                .build();
        client.pinReplica();
        ClickHouseRequest<?> req = createMockRequest();
        req.option(ClickHouseHttpOption.CUSTOM_HEADERS, "");
        client.setReplicaTagHeaderV1(req);
        String customHeaders = (String) req.getConfig().getOption(ClickHouseHttpOption.CUSTOM_HEADERS);
        Assertions.assertTrue(customHeaders.startsWith(ClickHouseHelperClient.REPLICA_TAG_HEADER + "="), "Got customHeaders: " + customHeaders);
    }

    @Test
    public void testSetReplicaTagHeaderV1_PreExistingCustomHeadersWithoutTrailingComma() {
        ClickHouseHelperClient client = new ClickHouseHelperClient.ClickHouseClientBuilder("localhost", 8123, null, null, -1)
                .enableReplicaPinning(true)
                .build();
        client.pinReplica();
        ClickHouseRequest<?> req = createMockRequest();
        req.option(ClickHouseHttpOption.CUSTOM_HEADERS, "Header1=Val1,Header2=Val2");
        client.setReplicaTagHeaderV1(req);
        String customHeaders = (String) req.getConfig().getOption(ClickHouseHttpOption.CUSTOM_HEADERS);
        Assertions.assertEquals("Header1=Val1,Header2=Val2," + ClickHouseHelperClient.REPLICA_TAG_HEADER + "=" + client.getReplicaTag(), customHeaders);
    }

    @Test
    public void testSetReplicaTagHeaderV1_PreExistingCustomHeadersWithTrailingComma() {
        ClickHouseHelperClient client = new ClickHouseHelperClient.ClickHouseClientBuilder("localhost", 8123, null, null, -1)
                .enableReplicaPinning(true)
                .build();
        client.pinReplica();
        ClickHouseRequest<?> req = createMockRequest();
        req.option(ClickHouseHttpOption.CUSTOM_HEADERS, "Header1=Val1,");
        client.setReplicaTagHeaderV1(req);
        String customHeaders = (String) req.getConfig().getOption(ClickHouseHttpOption.CUSTOM_HEADERS);
        Assertions.assertEquals("Header1=Val1," + ClickHouseHelperClient.REPLICA_TAG_HEADER + "=" + client.getReplicaTag(), customHeaders);
    }

    @Test
    public void testSetReplicaTagHeaderV1_AppendsToExistingReplicaTag() {
        ClickHouseHelperClient client = new ClickHouseHelperClient.ClickHouseClientBuilder("localhost", 8123, null, null, -1)
                .enableReplicaPinning(true)
                .build();
        client.pinReplica();
        ClickHouseRequest<?> req = createMockRequest();
        req.option(ClickHouseHttpOption.CUSTOM_HEADERS, "Header1=Val1,X-ClickHouse-Replica-Tag=oldTag123,Header2=Val2");
        client.setReplicaTagHeaderV1(req);
        String customHeaders = (String) req.getConfig().getOption(ClickHouseHttpOption.CUSTOM_HEADERS);
        Assertions.assertEquals("Header1=Val1,X-ClickHouse-Replica-Tag=oldTag123,Header2=Val2," + ClickHouseHelperClient.REPLICA_TAG_HEADER + "=" + client.getReplicaTag(), customHeaders);
    }

    @Test
    public void testSetReplicaTagHeaderV1_UnpinReset() {
        ClickHouseHelperClient client = new ClickHouseHelperClient.ClickHouseClientBuilder("localhost", 8123, null, null, -1)
                .enableReplicaPinning(true)
                .build();
        client.pinReplica();
        client.unpinReplica();
        ClickHouseRequest<?> req = createMockRequest();
        client.setReplicaTagHeaderV1(req);
        Assertions.assertFalse(req.hasOption(ClickHouseHttpOption.CUSTOM_HEADERS));
    }

    @Test
    public void testSetReplicaTagHeaderV2() {
        ClickHouseHelperClient client = new ClickHouseHelperClient.ClickHouseClientBuilder("localhost", 8123, null, null, -1)
                .enableReplicaPinning(true)
                .build();
        client.pinReplica();
        QuerySettings qs = new QuerySettings();
        InsertSettings is = new InsertSettings();
        client.setReplicaTagHeaderV2(qs);
        client.setReplicaTagHeaderV2(is);
        String expectedOptionKey = "http_header_" + ClickHouseHelperClient.REPLICA_TAG_HEADER.toUpperCase(java.util.Locale.US);
        Assertions.assertNotNull(qs.getAllSettings().get(expectedOptionKey));
        Assertions.assertNotNull(is.getAllSettings().get(expectedOptionKey));

        client.unpinReplica();
        QuerySettings qs2 = new QuerySettings();
        client.setReplicaTagHeaderV2(qs2);
        Assertions.assertNull(qs2.getAllSettings().get(expectedOptionKey));
    }
}
