package com.clickhouse.kafka.connect.util;

public class Utils {

    public static String escapeTopicName(String topic) {
        return String.format("`%s`", topic);
    }
}
