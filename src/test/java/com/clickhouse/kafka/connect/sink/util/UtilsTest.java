package com.clickhouse.kafka.connect.sink.util;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.util.Utils;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers.newDescriptor;
import static com.clickhouse.kafka.connect.util.Utils.computeColumnDiff;
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

    private static Table tableWith(String tableName, String[]... colSpecs) {
        Table table = new Table("default", tableName);
        for (String[] spec : colSpecs) {
            table.addColumn(Column.extractColumn(newDescriptor(spec[0], spec[1])));
        }
        return table;
    }

    @Test
    public void identicalTablesProduceNoDiffTest() {
        Table oldTable = tableWith("t", new String[]{"a", "Int32"}, new String[]{"b", "String"});
        Table newTable = tableWith("t", new String[]{"a", "Int32"}, new String[]{"b", "String"});

        List<String> diffs = computeColumnDiff(oldTable, newTable);

        assertTrue(diffs.isEmpty(), "Expected no diffs, got: " + diffs);
    }

    @Test
    public void detectTypeChangeAtPositionTest() {
        Table oldTable = tableWith("t", new String[]{"a", "Int32"}, new String[]{"b", "String"});
        Table newTable = tableWith("t", new String[]{"a", "Int32"}, new String[]{"b", "Int64"});

        List<String> diffs = computeColumnDiff(oldTable, newTable);

        assertEquals(1, diffs.size(), "Expected exactly one diff, got: " + diffs);
        String diff = diffs.get(0);
        assertTrue(diff.contains("position 1"), "Diff should mention position 1: " + diff);
        assertTrue(diff.contains("STRING"), "Diff should mention old STRING type: " + diff);
        assertTrue(diff.contains("INT64"), "Diff should mention new INT64 type: " + diff);
    }

    @Test
    public void detectColumnAddedAtEndTest() {
        Table oldTable = tableWith("t", new String[]{"a", "Int32"});
        Table newTable = tableWith("t", new String[]{"a", "Int32"}, new String[]{"new_col", "String"});

        List<String> diffs = computeColumnDiff(oldTable, newTable);

        assertEquals(1, diffs.size(), "Expected exactly one diff, got: " + diffs);
        String diff = diffs.get(0);
        assertTrue(diff.contains("Column added at position 1"), "Diff should describe addition: " + diff);
        assertTrue(diff.contains("new_col"), "Diff should mention the new column name: " + diff);
    }

    @Test
    public void detectColumnRemovedFromEndTest() {
        Table oldTable = tableWith("t", new String[]{"a", "Int32"}, new String[]{"gone", "String"});
        Table newTable = tableWith("t", new String[]{"a", "Int32"});

        List<String> diffs = computeColumnDiff(oldTable, newTable);

        assertEquals(1, diffs.size(), "Expected exactly one diff, got: " + diffs);
        String diff = diffs.get(0);
        assertTrue(diff.contains("Column removed at position 1"), "Diff should describe removal: " + diff);
        assertTrue(diff.contains("gone"), "Diff should mention the removed column name: " + diff);
    }

    @Test
    public void detectNullableChangeTest() {
        Table oldTable = tableWith("t", new String[]{"a", "Int32"});
        Table newTable = tableWith("t", new String[]{"a", "Nullable(Int32)"});

        List<String> diffs = computeColumnDiff(oldTable, newTable);

        assertEquals(1, diffs.size(), "Expected exactly one diff, got: " + diffs);
        String diff = diffs.get(0);
        assertTrue(diff.contains("position 0"), "Diff should mention position 0: " + diff);
        assertTrue(diff.contains("nullable=false") && diff.contains("nullable=true"),
                "Diff should mention nullable transition: " + diff);
    }

    @Test
    public void detectPrecisionAndScaleChangeTest() {
        Table oldTable = tableWith("t", new String[]{"d", "Decimal(5, 2)"});
        Table newTable = tableWith("t", new String[]{"d", "Decimal(7, 3)"});

        List<String> diffs = computeColumnDiff(oldTable, newTable);

        assertEquals(1, diffs.size(), "Expected exactly one diff, got: " + diffs);
        String diff = diffs.get(0);
        assertTrue(diff.contains("precision=5") && diff.contains("precision=7"),
                "Diff should mention precision transition: " + diff);
        assertTrue(diff.contains("scale=2") && diff.contains("scale=3"),
                "Diff should mention scale transition: " + diff);
    }

    @Test
    public void detectMultipleChangesAcrossPositionsTest() {
        Table oldTable = tableWith("t",
                new String[]{"a", "Int32"},
                new String[]{"b", "String"},
                new String[]{"c", "Float64"});
        Table newTable = tableWith("t",
                new String[]{"a", "Int64"},               // pos 0: type change Int32 -> Int64
                new String[]{"renamed_b", "String"},      // pos 1: name change b -> renamed_b
                new String[]{"c", "Float64"},             // pos 2: unchanged
                new String[]{"new_col", "UInt8"});        // pos 3: addition

        List<String> diffs = computeColumnDiff(oldTable, newTable);

        assertEquals(3, diffs.size(), "Expected exactly three diffs, got: " + diffs);

        String posZeroDiff = diffs.get(0);
        assertTrue(posZeroDiff.contains("position 0"), "First diff should mention position 0: " + posZeroDiff);
        assertTrue(posZeroDiff.contains("INT32") && posZeroDiff.contains("INT64"),
                "First diff should mention INT32->INT64: " + posZeroDiff);

        String posOneDiff = diffs.get(1);
        assertTrue(posOneDiff.contains("position 1"), "Second diff should mention position 1: " + posOneDiff);
        assertTrue(posOneDiff.contains("name=b") && posOneDiff.contains("name=renamed_b"),
                "Second diff should mention name transition: " + posOneDiff);

        String posThreeDiff = diffs.get(2);
        assertTrue(posThreeDiff.contains("Column added at position 3"),
                "Third diff should describe addition: " + posThreeDiff);
        assertTrue(posThreeDiff.contains("new_col"),
                "Third diff should mention the new column name: " + posThreeDiff);
    }
}
