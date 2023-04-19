package com.clickhouse.kafka.connect.util;

import com.clickhouse.client.ClickHouseException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

public class Utils {

    public static String escapeTopicName(String topic) {
        return String.format("`%s`", topic);
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
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


    /**
     * This method checks to see if we should retry, otherwise it just throws the exception again
     * @param e Exception to check
     */

    public static void handleException(Exception e) {
        LOGGER.debug("Exception in doInsert", e);
        //High-Level Explicit Exception Checking
        if (e instanceof DataException) {
            throw (DataException) e;
        }

        //Let's check if we have a ClickHouseException to reference the error code
        //https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
        Exception rootCause = Utils.getRootCause(e, true);
        if (rootCause instanceof ClickHouseException) {
            ClickHouseException clickHouseException = (ClickHouseException) rootCause;
            LOGGER.warn("ClickHouseException: {}", clickHouseException.getErrorCode());
            switch (clickHouseException.getErrorCode()) {
                // TIMEOUT_EXCEEDED
                case 159:
                // READONLY
                case 164:
                // NO_FREE_CONNECTION
                case 203:
                // SOCKET_TIMEOUT
                case 209:
                case 210:
                case 425:
                    throw new RetriableException(e);
                default:
                    LOGGER.error("Error code [{}] wasn't in the acceptable list.", clickHouseException.getErrorCode());
                    break;
            }
        }

        //Otherwise use Root-Cause Exception Checking

        if (rootCause instanceof SocketTimeoutException) {
            throw new RetriableException(e);
        } else if (rootCause instanceof UnknownHostException) {
            throw new RetriableException(e);
        }

        throw new RuntimeException(e);
    }

}
