package com.clickhouse.kafka.connect.util.jmx;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Not thread-safe Simple Moving Average implementation.
 * This class accumulates sum of values and calculates average on demand.
 * Values are stored in circular buffer to guarantee only last N values are used.
 * It is needed to keep metric responsive after long periods of measurement.
 *
 */
public class SimpleMovingAverage {

    public static final int DEFAULT_WINDOW_SIZE = 60;

    private final long[] values;
    private final AtomicInteger head;
    private final AtomicLong sum;

    public SimpleMovingAverage(int numOfValues) {
        this.values = new long[numOfValues];
        this.head = new AtomicInteger();
        this.sum = new AtomicLong();
    }

    public void add(long value) {
        int insertIndex = head.getAndIncrement() % values.length;
        // update sum by subtracting the oldest value (at insertIndex) and adding new value.
        sum.addAndGet(value - values[insertIndex]);
        values[insertIndex] = value;
    }

    public double get() {
       return (double) sum.get() / values.length;
    }
}
