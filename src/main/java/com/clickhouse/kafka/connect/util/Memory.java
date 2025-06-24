package com.clickhouse.kafka.connect.util;

public class Memory {
    long free = Runtime.getRuntime().freeMemory();
    long total = Runtime.getRuntime().totalMemory();
    long max = Runtime.getRuntime().maxMemory();
    long used = total - free;

    public static Memory get() {
        return new Memory();
    }

    public String toString() {
        return String.format("Memory: free:[%s] total:[%s] max:[%s] used:[%s]", free, total, max, used);
    }
}
