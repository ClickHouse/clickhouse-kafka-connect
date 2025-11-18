package com.clickhouse.kafka.connect.sink;

import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.kafka.connect.sink.helper.ConfluentKafkaPlatform;
import com.clickhouse.kafka.connect.sink.helper.DatasetGenerator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ClickHouseSinkTest  extends ConfluentKafkaPlatform {


    @BeforeAll
    public void setup() {
        super.setup();
    }

    @AfterAll
    public void teardown() {
        super.teardown();
    }

    @Test
    void printConnectors() {
        Object response = httpQuery(getConnectPort(), "/connector-plugins");
        System.out.println(response);

    }

    @Test
    void writeJson() {
        String topic = "test_json_events";

        createDatabase(getDbName(), "default", "");
        DatasetGenerator dg = new DatasetGenerator();
        DatasetGenerator.Dataset dataset = dg.generateDataset(dg.loadDatasetFromResources("/data01.yaml"));
        postDbQuery( dataset.getTableDefinition(topic), "default", "", getDbName());
        final int n = 1000;
        List<ProducerRecord<String, String>> producerRecords = dataset.generateStringProducerRecords(topic, n);
        try (KafkaProducer<String, String> producer = createStringProducer(producerConfig())) {
            final CountDownLatch  countMsgs = new CountDownLatch(n);
            Callback callback = ((metadata, exception) -> {
                countMsgs.countDown();
            });
            for (ProducerRecord<String, String> r : producerRecords) {
                producer.send(r, callback);
            }
            countMsgs.await();
            producer.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        Map<String, String> config = createConnectorConfig();
        config.put("errors.tolerance", "all");
        config.put("exactlyOnce", "false");
        config.put("topics", topic);
        config.put("fetch.max.wait.ms", "5000");
        config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        config.put(ClickHouseSinkConfig.INSERT_FORMAT, ClickHouseFormat.JSONEachRow.name());
        HttpResponse<String> createResponse = createConnector("test-connector", config);
        System.out.print(createResponse.body());


        try {
            Thread.sleep(15000); // time to let fetch.max.wait.ms expire
        } catch (InterruptedException e) {}

        List<ObjectNode> rows = getTableRowsAsJson(topic);
        assertEquals(n, rows.size());
//        System.out.println(rows);
//        printContainerLogs("connect"); // debug connector logs if needed
    }
}
