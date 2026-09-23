package com.clickhouse.kafka.connect.sink.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseBase;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkTask;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.transforms.KeyToValue;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class SlashColumnRoundTripTest extends ClickHouseBase {

    @ParameterizedTest
    @ValueSource(strings = {"V1", "V2"})
    public void describePreservesSlashInRootColumn(String clientVersion) {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, clientVersion);
        String topic = createTopicName("slash_describe");
        try (ClickHouseHelperClient client = ClickHouseTestHelpers.createClient(props)) {
            createTable(client, topic);
            try {
                Table table = client.describeTable(database, topic);
                assertNotNull(table);
                assertEquals(4, table.getRootColumnsMap().size());
                assertTrue(table.getRootColumnsMap().containsKey("a__b"));
                assertTrue(table.getRootColumnsMap().containsKey("a/b"),
                        () -> table.getRootColumnsMap().keySet().toString());
                assertTrue(table.getRootColumnsMap().containsKey("café/温度"));
            } finally {
                ClickHouseTestHelpers.dropTable(client, topic);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"V1", "V2"})
    public void sinkTaskStoresSlashAndUnderscoreFields(String clientVersion) throws Exception {
        Map<String, String> props = getBaseProps();
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, clientVersion);
        String topic = createTopicName("slash_insert");
        String id = UUID.randomUUID().toString();
        long offset = System.currentTimeMillis();
        try (ClickHouseHelperClient client = ClickHouseTestHelpers.createClient(props);
             JsonConverter converter = new JsonConverter();
             KeyToValue<SinkRecord> transform = new KeyToValue<>()) {
            createTable(client, topic);
            try {
                converter.configure(Map.of("schemas.enable", "false", "converter.type", "value"));
                String json = "{\"id\":\"" + id + "\",\"a/b\":1.5,\"a__b\":2.5,\"café/温度\":3.5}";
                SchemaAndValue value = converter.toConnectData(topic, json.getBytes(StandardCharsets.UTF_8));
                transform.configure(Map.of("field", "_key"));
                SinkRecord record = transform.apply(new SinkRecord(topic, 0, null, id,
                        value.schema(), value.value(), offset));
                ClickHouseSinkTask task = new ClickHouseSinkTask();
                try {
                    task.start(props);
                    task.put(List.of(record));
                } finally {
                    task.stop();
                }
                List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(client, topic);
                assertEquals(1, rows.size());
                assertEquals(id, rows.get(0).get("id"));
                assertEquals(2.5, rows.get(0).getDouble("a__b"));
                assertFalse(rows.get(0).isNull("a/b"), "a/b was stored as NULL");
                assertEquals(1.5, rows.get(0).getDouble("a/b"));
                assertEquals(3.5, rows.get(0).getDouble("café/温度"));
            } finally {
                ClickHouseTestHelpers.dropTable(client, topic);
            }
        }
    }

    private void createTable(ClickHouseHelperClient client, String topic) {
        new CreateTableStatement()
                .tableName(topic)
                .column("id", "String")
                .column("a/b", "Nullable(Float64) COMMENT '\\\\'")
                .column("a__b", "Nullable(Float64) COMMENT '\\n'")
                .column("café/温度", "Nullable(Float64) COMMENT 'café/温度'")
                .engine("MergeTree")
                .orderByColumn("id")
                .execute(client);
    }
}
