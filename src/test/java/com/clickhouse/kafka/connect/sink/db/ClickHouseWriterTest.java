package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.kafka.connect.sink.ClickHouseBase;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.sink.db.mapping.Type;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseDeploymentType;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.test.junit.extension.FromVersionConditionExtension;
import com.clickhouse.kafka.connect.util.QueryIdentifier;
import com.clickhouse.kafka.connect.util.Utils;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers.newDescriptor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(FromVersionConditionExtension.class)
public class ClickHouseWriterTest extends ClickHouseBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseWriterTest.class);

    private static final CreateTableStatement SINGLE_INT16_TABLE = new CreateTableStatement()
            .column("off16", "Int16")
            .orderByColumn("off16");

    ClickHouseHelperClient chc = null;

    @BeforeEach
    public void setUp() {
        LOGGER.info("Setting up...");
        Map<String, String> props = getBaseProps();
        chc = ClickHouseTestHelpers.createClient(props);
    }

    @Test
    public void writeUTF8StringPrimitive() throws IOException {
        ClickHouseWriter writer = new ClickHouseWriter(new SinkTaskStatistics(0));
        Column column = Column.extractColumn(newDescriptor("utf8String", "String"));
        ClickHousePipedOutputStream out = new ClickHousePipedOutputStream(null) {
            List<Byte> bytes = new ArrayList<>();

            @Override
            public ClickHouseOutputStream transferBytes(byte[] bytes, int i, int i1) throws IOException {
                for (int j = i; j < i1; j++) {
                    this.bytes.add(bytes[j]);
                }
                return this;
            }

            @Override
            public ClickHouseOutputStream writeByte(byte b) throws IOException {
                this.bytes.add(b);
                return this;
            }

            @Override
            public ClickHouseOutputStream writeBytes(byte[] bytes, int i, int i1) throws IOException {
                for (int j = i; j < i1; j++) {
                    this.bytes.add(bytes[j]);
                }
                return this;
            }

            @Override
            public ClickHouseOutputStream writeCustom(ClickHouseDataUpdater clickHouseDataUpdater) throws IOException {
                return this;
            }

            @Override
            public ClickHouseInputStream getInputStream(Runnable runnable) {
                return null;
            }

            @Override
            public String toString() {
                byte[] bytes = new byte[this.bytes.size()];
                for (int i = 0; i < this.bytes.size(); i++) {
                    bytes[i] = this.bytes.get(i);
                }
                return new String(bytes, StandardCharsets.UTF_8);
            }
        };
        byte[] originalBytes = "שלום".getBytes(StandardCharsets.UTF_8);
        writer.doWritePrimitive(Type.STRING, Schema.Type.STRING, out,"שלום", column);
        byte[] newBytes = out.toString().getBytes(StandardCharsets.UTF_8);
        assertTrue(Arrays.equals(originalBytes, Arrays.copyOfRange(newBytes, 1, newBytes.length)));//We add a length before the string
    }

    private void runWithWriter(Map<String, String> props, Consumer<ClickHouseWriter> test) {
        ClickHouseWriter writer = new ClickHouseWriter(new SinkTaskStatistics(0));
        writer.start(new ClickHouseSinkConfig(props));
        try {
            test.accept(writer);
        } finally {
            writer.stop();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void updateMapping(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("missing_table_mapping_test");

        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);

        runWithWriter(props, (chw) -> {

                chw.updateMapping(chc.getDatabase());
                Map<String, Table> tables = chw.getMapping();
                assertNull(tables.get(Utils.escapeTableName(chc.getDatabase(), topic)));


                new CreateTableStatement(SINGLE_INT16_TABLE).tableName(topic).deploymentType(deploymentType).execute(chc);

                Table table = chw.getTable(chc.getDatabase(), topic);
                assertNotNull(table);
                assertEquals(Utils.escapeTableName(chc.getDatabase(), topic), table.getFullName());

                tables = chw.getMapping();
                assertNotNull(tables.get(Utils.escapeTableName(chc.getDatabase(), topic)));
            });

        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void getTableUsesTopicToTableMapping(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        String topicWithoutBackticks = createTopicName("mapped_source_topic_plain_test");
        String mappedTableWithoutBackticks = createTopicName("mapped_target_table_plain_test");
        String topicWithBackticks = createTopicName("mapped_source_topic_backtick_test");
        String mappedTableWithBackticksRaw = createTopicName("mapped_target_table_backtick_test");
        String mappedTableWithBackticks = String.format("`%s`", mappedTableWithBackticksRaw);
        props.put(ClickHouseSinkConfig.TABLE_MAPPING,
                topicWithoutBackticks + "=" + mappedTableWithoutBackticks + ","
                        + topicWithBackticks + "=" + mappedTableWithBackticks);
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        ClickHouseTestHelpers.dropTable(chc, topicWithoutBackticks, deploymentType);
        ClickHouseTestHelpers.dropTable(chc, topicWithBackticks, deploymentType);
        ClickHouseTestHelpers.dropTable(chc, mappedTableWithoutBackticks, deploymentType);
        ClickHouseTestHelpers.dropTable(chc, mappedTableWithBackticksRaw, deploymentType);
        new CreateTableStatement(SINGLE_INT16_TABLE).tableName(mappedTableWithoutBackticks).deploymentType(deploymentType).execute(chc);
        new CreateTableStatement(SINGLE_INT16_TABLE).tableName(mappedTableWithBackticksRaw).deploymentType(deploymentType).execute(chc);

        runWithWriter(props, (chw) -> {
            Table plainMappingTable = chw.getTable(chc.getDatabase(), topicWithoutBackticks);
            assertNotNull(plainMappingTable);
            assertEquals(Utils.escapeTableName(chc.getDatabase(), mappedTableWithoutBackticks), plainMappingTable.getFullName());

            Table backtickedMappingTable = chw.getTable(chc.getDatabase(), topicWithBackticks);
            assertNotNull(backtickedMappingTable);
            assertEquals(Utils.escapeTableName(chc.getDatabase(), mappedTableWithBackticksRaw), backtickedMappingTable.getFullName());
        });
        ClickHouseTestHelpers.dropTable(chc, mappedTableWithoutBackticks, deploymentType);
        ClickHouseTestHelpers.dropTable(chc, mappedTableWithBackticksRaw, deploymentType);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void getTableThrowsWhenMissingAndSuppressionDisabled(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("missing_table_get_table_throw_test");

        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);

        runWithWriter(props, (chw) -> {
            RuntimeException ex = assertThrows(RuntimeException.class, () -> chw.getTable(chc.getDatabase(), topic));
            assertTrue(ex.getMessage().contains("does not exist"));
        });
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void getTableReturnsNullWhenMissingAndSuppressionEnabled(ClickHouseDeploymentType deploymentType) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.SUPPRESS_TABLE_EXISTENCE_EXCEPTION, "true");
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("missing_table_get_table_suppressed_test");

        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);

        runWithWriter(props, (chw) -> {
            Table table = chw.getTable(chc.getDatabase(), topic);
            assertNull(table);
        });
        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deploymentTypesForTests")
    public void doWriteColValue_Tuples(ClickHouseDeploymentType deploymentType) throws Exception {
        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("do_insert_tuple_order_mismatch_test");

        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
        new CreateTableStatement()
                .tableName(topic)
                .deploymentType(deploymentType)
                .column("_id", "String")
                .column("result", "Tuple(`id` String, `isanswered` Int32, `relevancescore` Float64, `subject` String, `istextanswered` Int32)")
                .deploymentType(deploymentType)
                .orderByColumn("_id").execute(chc);

        Schema tupleSchema = SchemaBuilder.struct()
                .field("isanswered", Schema.INT32_SCHEMA)
                .field("id", Schema.STRING_SCHEMA)
                .field("subject", Schema.STRING_SCHEMA)
                .field("relevancescore", Schema.FLOAT64_SCHEMA)
                .field("istextanswered", Schema.INT32_SCHEMA)
                .build();
        Schema recordSchema = SchemaBuilder.struct()
                .field("_id", Schema.STRING_SCHEMA)
                .field("result", tupleSchema)
                .build();

        Struct tupleValue = new Struct(tupleSchema)
                .put("isanswered", 1)
                .put("id", "24554770")
                .put("subject", "SUBJECT")
                .put("relevancescore", 84.7)
                .put("istextanswered", 1);
        Struct value = new Struct(recordSchema)
                .put("_id", "id-1")
                .put("result", tupleValue);

        SinkRecord sinkRecord = new SinkRecord(
                topic,
                0,
                null,
                null,
                recordSchema,
                value,
                0,
                System.currentTimeMillis(),
                TimestampType.CREATE_TIME);
        Record record = Record.convert(sinkRecord, false, ".", chc.getDatabase());

        runWithWriter(props, (chw) -> {
            try {
                chw.doInsert(List.of(record), new QueryIdentifier(topic, "tuple-order-mismatch-" + System.nanoTime()));
            } catch (Exception e) {
                fail("Failed to insert", e);
            }
        });

        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic, deploymentType);
        assertEquals(1, rows.size());
        JSONObject row = rows.get(0);
        assertEquals("id-1", row.getString("_id"));
        JSONObject tuple = row.getJSONObject("result");
        assertEquals("24554770", tuple.getString("id"));
        assertEquals(1, tuple.getInt("isanswered"));
        assertEquals(84.7d, tuple.getDouble("relevancescore"), 0.001d);
        assertEquals("SUBJECT", tuple.getString("subject"));
        assertEquals(1, tuple.getInt("istextanswered"));

        ClickHouseTestHelpers.dropTable(chc, topic, deploymentType);
    }
}
