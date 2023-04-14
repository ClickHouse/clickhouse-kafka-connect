package com.clickhouse.kafka.connect.util;

import com.clickhouse.client.ClickHouseException;

public class Utils {

    public static String escapeTopicName(String topic) {
        return String.format("`%s`", topic);
    }

    public static Exception getRootCause (Exception e) {
        return getRootCause(e, false);
    }

    /**
     * This will drill down to the first ClickHouseException in the exception chain
     * @param e Exception to drill down
     * @return ClickHouseException or null if none found
     */
    public static Exception getRootCause (Exception e, Boolean prioritizeClickHouseException) {
        if (e == null)
            return null;

        Throwable runningException = e;//We have to use Throwable because of the getCause() signature
        while (runningException.getCause() != null &&
                (!prioritizeClickHouseException || !(runningException instanceof ClickHouseException))) {
            runningException = runningException.getCause();
        }

        return runningException instanceof Exception ? (Exception) runningException : null;
    }

}
