package com.clickhouse.kafka.connect.util.jmx;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class SimpleMovingAverageTest {

    @Test
    void testAverage() {
        SimpleMovingAverage sma = new SimpleMovingAverage(60);

        long[] values = new long[60];
        for (int i = 0; i < values.length; i++) {
            values[i] = (i < values.length / 2) ? 500 : 1000;
        }

        for (long value : values) {
            sma.add(value);
        }

        double sma_val1 = sma.get();
        assertTrue(sma_val1 > 500);

        for (int i = 0; i < values.length; i++) {
            values[i] = (i < values.length / 2) ? 1000 : 300;
        }

        for (long value : values) {
            sma.add(value);
        }

        assertTrue(sma.get() < sma_val1);
    }


    @Test
    void testManyValues() {
        SimpleMovingAverage sma = new SimpleMovingAverage(1000);

        long[] values = new long[100000];
        Random random = new Random();
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(1000);
        }

        for (long value : values) {
            sma.add(value);
        }

        double sma_val1 = sma.get();
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(1000);
        }

        for (long value : values) {
            sma.add(value);
        }

        double sma_val2 = sma.get();
        assertTrue(sma_val1 != sma_val2);
    }
}