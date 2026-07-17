package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.sink.helper.SchemaTestData;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage for {@code useRowBinaryWithNamesAndTypes=true}. The key scenario is inserting
 * records that carry fewer fields than the destination table: because the connector sends a
 * names-and-types header, ClickHouse can map by name and fill defaults for the columns that are not
 * present in the stream, which is how the format addresses schema evolution.
 */
public class RowBinaryWithNamesAndTypesTest extends ClickHouseBase {

    private static final int RECORD_COUNT = 10;

    private Map<String, String> propsWithRowBinaryNamesAndTypes() {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConfig.USE_ROW_BINARY_WITH_NAMES_AND_TYPES, "true");
        return props;
    }

    @Test
    public void insertsMatchingRecordsWithNamesAndTypesHeader() {
        Map<String, String> props = propsWithRowBinaryNamesAndTypes();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("rbwnat-basic-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("string", "String")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createSimpleData(topic, 1, RECORD_COUNT);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
        assertEquals(sr.size(), rows.size());
        for (JSONObject row : rows) {
            assertEquals("test string", row.getString("string"));
        }
        ClickHouseTestHelpers.dropTable(chc, topic);
    }

    @Test
    public void recordsWithFewerFieldsThanTableFillNullableColumn() {
        // The table has a column the records never provide. With the names-and-types header the
        // connector still describes every column, and the absent field lands as NULL.
        Map<String, String> props = propsWithRowBinaryNamesAndTypes();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("rbwnat-fewer-fields-nullable-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("string", "String")
                .column("extra", "Nullable(Int32)")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);

        Collection<SinkRecord> sr = SchemaTestData.createSimpleData(topic, 1, RECORD_COUNT);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(sr);
        chst.stop();

        assertEquals(sr.size(), ClickHouseTestHelpers.countRows(chc, topic));
        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
        assertEquals(sr.size(), rows.size());
        for (JSONObject row : rows) {
            assertEquals("test string", row.getString("string"));
            assertTrue(row.isNull("extra"), "Absent field should be stored as NULL, was: " + row.opt("extra"));
        }
        ClickHouseTestHelpers.dropTable(chc, topic);
    }

    @Test
    public void oldSchemaRecordsInsertAfterDefaultColumnAdded() {
        // Schema evolution: a non-nullable column with a DEFAULT is added to the table after the
        // connector has been writing. Records keep arriving without that field; they must still be
        // inserted and the new column must fall back to its default.
        Map<String, String> props = propsWithRowBinaryNamesAndTypes();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);

        String topic = createTopicName("rbwnat-schema-evolution-default-test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        new CreateTableStatement()
                .tableName(topic)
                .column("off16", "Int16")
                .column("string", "String")
                .engine("MergeTree")
                .orderByColumn("off16")
                .execute(chc);

        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);

        Collection<SinkRecord> firstBatch = SchemaTestData.createSimpleData(topic, 1, RECORD_COUNT);
        chst.put(firstBatch);
        assertEquals(firstBatch.size(), ClickHouseTestHelpers.countRows(chc, topic));

        ClickHouseTestHelpers.executeQueryIgnoreResult(chc, String.format(
                "ALTER TABLE `%s`%s ADD COLUMN num32_default Int32 DEFAULT 42 AFTER string",
                topic, ClickHouseTestHelpers.getClusterClauseOrEmpty()));

        // Same schema as before (off16, string), but past the previous offsets so the state machine
        // treats them as new data rather than a retry/duplicate.
        Collection<SinkRecord> secondBatch = firstBatch.stream()
                .map(record -> new SinkRecord(
                        record.topic(),
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        record.valueSchema(),
                        record.value(),
                        record.kafkaOffset() + 1000,
                        record.timestamp(),
                        TimestampType.CREATE_TIME))
                .collect(Collectors.toList());
        chst.put(secondBatch);
        chst.stop();

        int totalRows = firstBatch.size() + secondBatch.size();
        assertEquals(totalRows, ClickHouseTestHelpers.countRows(chc, topic));
        // Every row (including the ones written before the column existed) resolves to the default.
        assertEquals(42 * totalRows, ClickHouseTestHelpers.sumRows(chc, topic, "num32_default"));
        ClickHouseTestHelpers.dropTable(chc, topic);
    }
}
