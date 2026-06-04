package com.clickhouse.kafka.connect.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.connect.protobuf.ProtobufConverterConfig;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Feature test that exercises the connector's Protobuf ingest path (schema registry -> {@link
 * ProtobufConverter} -> connector) against a real ClickHouse instance.
 *
 * <p>It reuses the same compatible schema fixtures as {@code
 * ClickHouseSinkConnectorIntegrationTest#protoSchemaTest}, located under {@code
 * src/testFixtures/proto/schemas/compatible}. Unlike the integration test, this test does not
 * require the Confluent REST proxy / Schema Registry containers: it parses the {@code .proto} IDL at
 * runtime, builds {@link DynamicMessage}s from the fixture's protobuf-JSON records, and serializes
 * them through a {@link MockSchemaRegistryClient}.
 */
public class ProtobufIngestTest extends ClickHouseBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufIngestTest.class);

    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://protobuf-ingest-test";

    // fixture JSON keys
    private static final String DESCRIPTION_KEY = "description";
    private static final String CLICKHOUSE_COLUMNS_KEY = "clickhouse_columns";
    private static final String CLICKHOUSE_ORDER_BY_KEY = "clickhouse_order_by";
    private static final String RECORDS_KEY = "records";
    private static final String EXPECTED_ROW_COUNT_KEY = "expected_row_count";

    private static Stream<Path> getCompatibleProtoSchemaPaths() {
        final File schemaDir =
                new File(Path.of("src", "testFixtures", "proto", "schemas", "compatible").toString());
        assertTrue(schemaDir.exists(), "Missing compatible proto schema fixtures directory: " + schemaDir);
        return Stream.of(Objects.requireNonNull(schemaDir.listFiles((dir, name) -> name.endsWith(".json"))))
                .map(file -> Path.of(schemaDir.toPath().toString(), file.getName()));
    }

    private static Stream<Path> getComplexProtoSchemaPaths() {
        final File schemaDir =
                new File(Path.of("src", "testFixtures", "proto", "schemas", "complex_nested").toString());
        assertTrue(schemaDir.exists(), "Missing complex_nested proto schema fixtures directory: " + schemaDir);
        return Stream.of(Objects.requireNonNull(schemaDir.listFiles((dir, name) -> name.endsWith(".json"))))
                .map(file -> Path.of(schemaDir.toPath().toString(), file.getName()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getCompatibleProtoSchemaPaths")
    public void protoIngestTest(Path jsonPath) throws Exception {
        runProtoIngestTest(jsonPath);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getComplexProtoSchemaPaths")
    public void complexProtoIngestTest(Path jsonPath) throws Exception {
        runProtoIngestTest(jsonPath);
    }

    private void runProtoIngestTest(Path jsonPath) throws Exception {
        JSONObject fixture = new JSONObject(String.join("", Files.readAllLines(jsonPath)));

        int expectedRowCount = fixture.getInt(EXPECTED_ROW_COUNT_KEY);

        String fileName = jsonPath.getFileName().toString();
        String baseName = fileName.replace(".json", "");
        Path protoPath = jsonPath.resolveSibling(baseName + ".proto");
        String protoSchema = String.join("\n", Files.readAllLines(protoPath));

        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("proto_ingest_" + baseName.replace("-", "_"));

        LOGGER.info(
                "Running protobuf ingest test: {} (topic: {}, expected rows: {})",
                fixture.getString(DESCRIPTION_KEY),
                topic,
                expectedRowCount);

        // 1. Create the ClickHouse table from the fixture column definitions.
        ClickHouseTestHelpers.dropTable(chc, topic);
        CreateTableStatement tableStmt =
                new CreateTableStatement()
                        .tableName(topic)
                        .engine("MergeTree")
                        .orderByColumn(fixture.getString(CLICKHOUSE_ORDER_BY_KEY));
        JSONObject clickhouseColumns = fixture.getJSONObject(CLICKHOUSE_COLUMNS_KEY);
        for (String colName : clickhouseColumns.keySet()) {
            tableStmt.column(colName, clickhouseColumns.getString(colName));
        }
        tableStmt.execute(chc);

        // 2. Convert the fixture's protobuf-JSON records into SinkRecords via the Protobuf converter.
        List<SinkRecord> records = buildSinkRecords(topic, protoSchema, fixture.getJSONArray(RECORDS_KEY));

        // 3. Run the records through the connector.
        ClickHouseSinkTask chst = new ClickHouseSinkTask();
        chst.start(props);
        chst.put(records);
        chst.stop();

        // 4. Verify all rows landed in ClickHouse.
        assertEquals(expectedRowCount, ClickHouseTestHelpers.countRows(chc, topic));
    }

    private static List<SinkRecord> buildSinkRecords(String topic, String protoSchema, JSONArray records)
            throws Exception {
        ProtobufSchema schema = new ProtobufSchema(protoSchema);
        Descriptor descriptor = schema.toDescriptor();

        MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        schemaRegistry.register(topic + "-value", schema);

        KafkaProtobufSerializer<DynamicMessage> serializer = new KafkaProtobufSerializer<>(schemaRegistry);
        Map<String, Object> serializerConfig = new HashMap<>();
        serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        serializer.configure(serializerConfig, false);

        ProtobufConverter converter = new ProtobufConverter(schemaRegistry);
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put(ProtobufConverterConfig.AUTO_REGISTER_SCHEMAS, true);
        converterConfig.put(ProtobufDataConfig.GENERATE_INDEX_FOR_UNIONS_CONFIG, false);
        converterConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        converter.configure(converterConfig, false);

        JsonFormat.Parser jsonParser = JsonFormat.parser().ignoringUnknownFields();

        List<SinkRecord> sinkRecords = new ArrayList<>(records.length());
        for (int i = 0; i < records.length(); i++) {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
            jsonParser.merge(records.getJSONObject(i).toString(), builder);
            byte[] serialized = serializer.serialize(topic, builder.build());
            SchemaAndValue connectData = converter.toConnectData(topic, serialized);
            sinkRecords.add(
                    new SinkRecord(topic, 0, null, null, connectData.schema(), connectData.value(), i));
        }
        return sinkRecords;
    }
}
