package com.clickhouse.kafka.connect.sink.data.convert;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

class RecordConvertorTest {

    @ParameterizedTest
    @MethodSource("splitDBTopicProvider")
    void splitDBTopic(String topic, String dbTopicSeparatorChar, String database) {

        String[] parts = topic.split(Pattern.quote(dbTopicSeparatorChar));
        String actualDatabase = parts[0];
        String actualTopic = parts[1];
        System.out.println("actual_topic: " + actualTopic);

        assertEquals(database, actualDatabase);
    }

    static Object[][] splitDBTopicProvider() {
        return new Object[][] {
            { "tenant_A__telemetry", "__", "tenant_A" },
        };
    }

}