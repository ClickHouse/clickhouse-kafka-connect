package com.clickhouse.kafka.connect.util.jmx;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Not thread-safe Exponential Moving Average implementation
 */
public class SimpleMovingAverage {

    public static final int DEFAULT_WINDOW_SIZE = 60;

    private final long[] values;
    private final AtomicInteger head;

    public SimpleMovingAverage(int numOfValues) {
        this.values = new long[numOfValues];
        this.head = new AtomicInteger();
    }

    public void add(long value) {
        int insertIndex = head.getAndIncrement() % values.length;
        values[insertIndex] = value;
    }

    public double get() {
       double sum = 0;
       int count = head.get();
       if (count >= values.length) {
            count = values.length;
       }

       for (int i = 0; i < count; i++) {
            sum += values[i];
       }
       return sum / count;
    }
}
