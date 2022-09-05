package com.clickhouse.kafka.connect.util.jmx;

public class Timer {
    private final long startTime;

    private Timer() {
        this.startTime = System.nanoTime();
    }

    public static Timer start() {
        return new Timer();
    }

    public long nanosElapsed() {
        return System.nanoTime() - this.startTime;
    }
}
