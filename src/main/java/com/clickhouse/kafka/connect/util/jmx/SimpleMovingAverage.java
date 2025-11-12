package com.clickhouse.kafka.connect.util.jmx;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Not thread-safe Exponential Moving Average implementation
 */
public class SimpleMovingAverage {

    public static final int DEFAULT_WINDOW_SIZE = 60;

    private final long[] values;
    private final AtomicInteger head;
    private final AtomicLong sum;
    private final int n;

    public SimpleMovingAverage(int numOfValues) {
        this.values = new long[numOfValues];
        this.n = this.values.length - 1;
        this.head = new AtomicInteger();
        this.sum = new AtomicLong();
    }

    public void add(long value) {
        int insertIndex = head.getAndIncrement() % values.length;
        sum.addAndGet(value - values[insertIndex]);
        values[insertIndex] = value;
    }

    public double get() {
       return (double) sum.get() / values.length;
    }
}
