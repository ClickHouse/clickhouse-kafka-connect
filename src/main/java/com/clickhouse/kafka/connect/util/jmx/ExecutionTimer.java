package com.clickhouse.kafka.connect.util.jmx;

public class ExecutionTimer {
    private final long startTime;

    private ExecutionTimer() {
        this.startTime = System.nanoTime();
    }

    public static ExecutionTimer start() {
        return new ExecutionTimer();
    }

    public long nanosElapsed() {
        return System.nanoTime() - this.startTime;
    }
}
