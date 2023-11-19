package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.*;

public class ClickHouseSinkTaskTest {

    private static ClickHouseHelperClient chc = null;

    @BeforeAll
    public static void setup() {

    }

    @AfterAll
    protected static void tearDown() {

    }

    @Test
    public void exceptionHandlingTest() {
        ClickHouseSinkTask task = new ClickHouseSinkTask();
        assertThrows(ConnectException.class, () -> task.put(null));
        try {
            task.put(null);
        } catch (Exception e) {
            assertEquals(e.getClass(), ConnectException.class);
            assertTrue(e.getCause() instanceof NullPointerException);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            assertTrue(sw.toString().contains("com.clickhouse.kafka.connect.util.Utils.handleException"));
        }
    }
}
