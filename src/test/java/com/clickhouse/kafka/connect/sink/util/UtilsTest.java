package com.clickhouse.kafka.connect.sink.util;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.kafka.connect.util.Utils;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

public class UtilsTest {

    @Test
    @DisplayName("Test Exception Root Cause")
    public void TestRootCause() {
        Exception e1 = new Exception("The Actual Root Cause");
        Exception e2 = new Exception(e1);
        Exception e3 = new Exception(e2);
        Exception e4 = new Exception(e3);
        assertEquals(e1, Utils.getRootCause(e4));
    }


    @Test
    @DisplayName("Test ClickHouseException Root Cause")
    public void TestClickHouseRootCause() {
        Exception e1 = new Exception();
        Exception e2 = new Exception(e1);
        Exception e3 = new ClickHouseException(123, e2, null);
        Exception e4 = new RuntimeException(e3);
        Exception e5 = new Exception(e4);

        Exception clickHouseException = Utils.getRootCause(e5, true);
        assertEquals(e3, clickHouseException);
        assertTrue(clickHouseException instanceof ClickHouseException);
        assertEquals(123, ((ClickHouseException) clickHouseException).getErrorCode());
    }

    @Test
    @DisplayName("Test ClickHouseClient Timeout Throw Cause")
    public void TestClickHouseClientTimeoutCause(){
        assertThrows(RetriableException.class, () -> {
            Exception timeout = new IOException("Write timed out after 30000 ms");
            Utils.handleException(timeout, false, new ArrayList<>());
        });
    }
}
