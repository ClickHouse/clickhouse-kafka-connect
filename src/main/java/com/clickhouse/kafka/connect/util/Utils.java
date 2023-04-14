package com.clickhouse.kafka.connect.util;

import com.clickhouse.client.ClickHouseException;
import org.apache.kafka.connect.errors.RetriableException;

import java.net.SocketTimeoutException;

public class Utils {

    public static String escapeTopicName(String topic) {
        return String.format("`%s`", topic);
    }

    public static Exception getRootCause (Exception e) {
        if (e == null)
            return null;

        Throwable runningException = e;
        while (runningException.getCause() != null) {
            runningException = runningException.getCause();
        }

        return runningException instanceof Exception ? (Exception) runningException : null;
    }

}
