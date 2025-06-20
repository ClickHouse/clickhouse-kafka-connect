package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.data.convert.SchemaRecordConvertor;
import com.clickhouse.kafka.connect.test.TestProtos;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

public class ProtobufConverterTest {

    private static ProtobufData protobufData;
    private static MockSchemaRegistryClient schemaRegistry;


    private static final String TEST_TOPIC = "test-topic";

    @BeforeAll
    public static void setUp() {
        // Initialize mock schema registry
        schemaRegistry = new MockSchemaRegistryClient();

        // Other things
        protobufData = new ProtobufData(0);
    }

    @AfterEach
    public void tearDown() {
    }

    @Test
    public void testConvertingMessage() throws Exception {
        // Register the schema for the test message
        ProtobufSchema schema = new ProtobufSchema(TestProtos.getDescriptor());

        // Create a test Protobuf message with user info
        TestProtos.TestMessage userMessage = createUserMessage();
        testMessageConversion(userMessage, "user_info",  schema);

        // Create a test Protobuf message with product info
//        TestProtos.TestMessage productMessage = createProductMessage();
//        testMessageConversion(productMessage, "product_info");
    }

    private void testMessageConversion(TestProtos.TestMessage message, String expectedContentField, ProtobufSchema schema)
            throws IOException, RestClientException {

        SchemaAndValue connectSchema = new ProtobufData(1).toConnectData(schema, message);
        Object value = connectSchema.value();
        Schema schema1 = connectSchema.schema();
        Schema keySchema = new SchemaBuilder(Schema.Type.STRING).keySchema();

        SinkRecord sinkRecord = new SinkRecord(TEST_TOPIC, 1, keySchema, "test-key", schema1, value, 0);
        SchemaRecordConvertor recordConvertor = new SchemaRecordConvertor();
        Record record = recordConvertor.doConvert(sinkRecord, TEST_TOPIC, "test");

        Assertions.assertNotNull(record);
    }

    private TestProtos.TestMessage createUserMessage() {
        return TestProtos.TestMessage.newBuilder()
                .setId(123)
                .setName("Test User")
                .setIsActive(true)
                .setScore(95.5)
                .addAllTags(Arrays.asList("tag1", "tag2"))
                .setUserInfo(
                        TestProtos.UserInfo.newBuilder()
                                .setEmail("user@example.com")
                                .setAge(30)
                                .setUserType(TestProtos.UserInfo.UserType.PREMIUM)
                                .build()
                )
                .build();
    }

    private TestProtos.TestMessage createProductMessage() {
        return TestProtos.TestMessage.newBuilder()
                .setId(456)
                .setName("Test Product")
                .setIsActive(true)
                .setScore(4.5)
                .addAllTags(Arrays.asList("electronics", "gadgets"))
                .setProductInfo(
                        TestProtos.ProductInfo.newBuilder()
                                .setSku("PROD-123")
                                .setPrice(99.99)
                                .setInStock(true)
                                .addAllCategories(Arrays.asList("Electronics", "Gadgets"))
                                .build()
                )
                .build();
    }
}
