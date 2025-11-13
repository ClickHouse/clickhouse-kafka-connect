package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.helper.ConfluentKafkaPlatform;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.http.HttpResponse;
import java.util.Map;

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
        Object response = queryConnect(getConnectPort(), "/connector-plugins");
        System.out.println(response);

    }

    @Test
    void writeJson() {
        String topic = "test_json_events";

        createDatabase(getDbName(), "default", "");
        postDbQuery("CREATE TABLE " + getDbName() + "." + topic + "(id Int32, name String) ENGINE = MergeTree() ORDER BY id", "default", "");

        Map<String, String> config = createConnectorConfig();
        config.put("errors.tolerance", "all");
        config.put("exactlyOnce", "false");
        config.put("topics", topic);
        HttpResponse<String> createResponse = createConnector("test-connector", config);
        System.out.print(createResponse.body());

        Object response = queryConnect(getConnectPort(), "/connectors");
        System.out.println(response);
    }
}
