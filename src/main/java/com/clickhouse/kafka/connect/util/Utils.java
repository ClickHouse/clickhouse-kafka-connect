package com.clickhouse.kafka.connect.util;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class Utils {

    public static String escapeName(String topic) {
        String cleanTopic = topic.replace("`", "");
        return String.format("`%s`", cleanTopic);
    }

    public static String escapeTableName(String database, String topicName) {
        return escapeName(database) + "." + escapeName(topicName);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static Exception getRootCause(Exception e) {
        return getRootCause(e, false);
    }

    /**
     * This will drill down to the first ClickHouseException in the exception chain
     *
     * @param e Exception to drill down
     * @return ClickHouseException or null if none found
     */
    public static Exception getRootCause(Exception e, Boolean prioritizeClickHouseException) {
        if (e == null)
            return null;

        Throwable runningException = e;//We have to use Throwable because of the getCause() signature
        while (runningException.getCause() != null &&
                (!prioritizeClickHouseException || !(runningException instanceof ClickHouseException))) {
            LOGGER.trace("Found exception: {}", runningException.getLocalizedMessage());
            runningException = runningException.getCause();
        }

        return runningException instanceof Exception ? (Exception) runningException : null;
    }


    /**
     * This method checks to see if we should retry, otherwise it just throws the exception again
     *
     * @param e Exception to check
     */

    public static void handleException(Exception e, boolean errorsTolerance, Collection<SinkRecord> records) {
        LOGGER.warn("Deciding how to handle exception: {}", e.getLocalizedMessage());

        //Let's check if we have a ClickHouseException to reference the error code
        //https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
        Exception rootCause = Utils.getRootCause(e, true);
        if (rootCause instanceof ClickHouseException) {
            ClickHouseException clickHouseException = (ClickHouseException) rootCause;
            LOGGER.warn("ClickHouseException code: {}", clickHouseException.getErrorCode());
            switch (clickHouseException.getErrorCode()) {
                case 3: // UNEXPECTED_END_OF_FILE
                case 107: // FILE_DOESNT_EXIST
                case 159: // TIMEOUT_EXCEEDED
                case 164: // READONLY
                case 202: // TOO_MANY_SIMULTANEOUS_QUERIES
                case 203: // NO_FREE_CONNECTION
                case 209: // SOCKET_TIMEOUT
                case 210: // NETWORK_ERROR
                case 241: // MEMORY_LIMIT_EXCEEDED
                case 242: // TABLE_IS_READ_ONLY
                case 252: // TOO_MANY_PARTS
                case 285: // TOO_FEW_LIVE_REPLICAS
                case 319: // UNKNOWN_STATUS_OF_INSERT
                case 425: // SYSTEM_ERROR
                case 999: // KEEPER_EXCEPTION
                    throw new RetriableException(e);
                default:
                    LOGGER.error("Error code [{}] wasn't in the acceptable list.", clickHouseException.getErrorCode());
                    break;
            }
        }

        //High-Level Explicit Exception Checking
        if (e instanceof DataException && !errorsTolerance) {
            LOGGER.warn("DataException thrown, wrapping exception: {}", e.getLocalizedMessage());
            throw (DataException) e;
        }

        //Otherwise use Root-Cause Exception Checking
        if (rootCause instanceof SocketTimeoutException) {
            LOGGER.warn("SocketTimeoutException thrown, wrapping exception: {}", e.getLocalizedMessage());
            throw new RetriableException(e);
        } else if (rootCause instanceof UnknownHostException) {
            LOGGER.warn("UnknownHostException thrown, wrapping exception: {}", e.getLocalizedMessage());
            throw new RetriableException(e);
        } else if (rootCause instanceof IOException) {
            final String msg = rootCause.getMessage();
            if (msg.indexOf(CLICKHOUSE_CLIENT_ERROR_READ_TIMEOUT_MSG) == 0 || msg.indexOf(CLICKHOUSE_CLIENT_ERROR_WRITE_TIMEOUT_MSG) == 0) {
                LOGGER.warn("IOException thrown, wrapping exception: {}", e.getLocalizedMessage());
                throw new RetriableException(e);
            }
        }

        if (errorsTolerance) {//Right now this is all exceptions - should we restrict to just ClickHouseExceptions?
            LOGGER.warn("Errors tolerance is enabled, ignoring exception: {}", e.getLocalizedMessage());
        } else {
            LOGGER.error("Errors tolerance is disabled, wrapping exception: {}", e.getLocalizedMessage());
            if (records != null) {
                throw new RuntimeException(String.format("Number of records: %d", records.size()), e);
            } else {
                throw new RuntimeException("Records was null", e);
            }

        }
    }

    private static final String CLICKHOUSE_CLIENT_ERROR_READ_TIMEOUT_MSG = "Read timed out after";
    private static final String CLICKHOUSE_CLIENT_ERROR_WRITE_TIMEOUT_MSG = "Write timed out after";

    public static void sendTODlq(ErrorReporter errorReporter, Record record, Exception exception) {
        sendTODlq(errorReporter, record.getSinkRecord(), exception);
    }

    public static void sendTODlq(ErrorReporter errorReporter, SinkRecord record, Exception exception) {
        if (errorReporter != null && record != null) {
            errorReporter.report(record, exception);
        }
    }

    public static String getTableName(String database, String topicName, Map<String, String> topicToTableMap) {
        String tableName = topicToTableMap.get(topicName);
        LOGGER.debug("Topic name: {}, Table Name: {}", topicName, tableName);
        if (tableName == null) {
            tableName = topicName;
        }

        return escapeTableName(database, tableName);
    }


    public static String getOffsets(Collection<SinkRecord> records) {
        long minOffset = Long.MAX_VALUE;
        long maxOffset = -1;

        for (SinkRecord record : records) {
            if (record.kafkaOffset() > maxOffset) {
                maxOffset = record.kafkaOffset();
            }
            if (record.kafkaOffset() < minOffset) {
                minOffset = record.kafkaOffset();
            }
        }

        return String.format("minOffset: %d, maxOffset: %d", minOffset, maxOffset);
    }

    public static List<String> splitIgnoringQuotes(String input, char separator) {
        List<String> result = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inSingleQuotes = false;
        boolean inDoubleQuotes = false;

        for (char c : input.toCharArray()) {
            if (c == '\'' && !inDoubleQuotes) {
                inSingleQuotes = !inSingleQuotes;
                sb.append(c);
            } else if (c == '"' && !inSingleQuotes) {
                inDoubleQuotes = !inDoubleQuotes;
                sb.append(c);
            } else if (c == separator && !inSingleQuotes && !inDoubleQuotes) {
                result.add(sb.toString().trim());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        result.add(sb.toString().trim());

        return result;
    }
}
