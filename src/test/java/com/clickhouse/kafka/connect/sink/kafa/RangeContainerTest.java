package com.clickhouse.kafka.connect.sink.kafa;


import com.clickhouse.kafka.connect.sink.kafka.RangeContainer;
import com.clickhouse.kafka.connect.sink.kafka.RangeState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RangeContainerTest {

    private String topic = "test";
    private int partition = 1;

    @Test
    @DisplayName("sameRangeTest")
    public void sameRangeTest() {
        RangeContainer rangeContainerFirst = new RangeContainer(topic, partition, 10, 1);
        RangeContainer rangeContainerSecond01 = new RangeContainer(topic, partition, 10, 1);
        RangeContainer rangeContainerSecond02 = new RangeContainer(topic, partition, 10, 0);
        assertEquals(RangeState.SAME, rangeContainerFirst.getOverLappingState(rangeContainerSecond01));
        assertEquals(RangeState.SAME, rangeContainerFirst.getOverLappingState(rangeContainerSecond02));

    }


    @Test
    @DisplayName("newRangeTest")
    public void newRangeTest() {
        RangeContainer rangeContainerFirst = new RangeContainer(topic, partition, 10, 0);
        RangeContainer rangeContainerSecond = new RangeContainer(topic, partition, 20, 11);
        assertEquals(RangeState.NEW, rangeContainerFirst.getOverLappingState(rangeContainerSecond));

    }


    @Test
    @DisplayName("containsRangeTest")
    public void containsRangeTest() {
        RangeContainer rangeContainerFirst = new RangeContainer(topic, partition, 10, 0);
        RangeContainer rangeContainerSecond01 = new RangeContainer(topic, partition, 9, 1);
        RangeContainer rangeContainerSecond02 = new RangeContainer(topic, partition, 9, 0);
        RangeContainer rangeContainerSecond03 = new RangeContainer(topic, partition, 10, 1);

        assertEquals(RangeState.CONTAINS, rangeContainerFirst.getOverLappingState(rangeContainerSecond01));
        assertEquals(RangeState.CONTAINS, rangeContainerFirst.getOverLappingState(rangeContainerSecond02));
        assertEquals(RangeState.CONTAINS, rangeContainerFirst.getOverLappingState(rangeContainerSecond03));

    }

    @Test
    @DisplayName("errorRangeTest")
    public void errorRangeTest() {
        RangeContainer rangeContainerFirst = new RangeContainer(topic, partition, 10, 4);
        RangeContainer rangeContainerSecond = new RangeContainer(topic, partition, 9, 2);
        assertEquals(RangeState.ERROR, rangeContainerFirst.getOverLappingState(rangeContainerSecond));

    }

    @Test
    @DisplayName("overlapRangeTest")
    public void overlapRangeTest() {
        RangeContainer rangeContainerFirst = new RangeContainer(topic, partition, 10, 2);
        RangeContainer rangeContainerSecond01 = new RangeContainer(topic, partition, 19, 10);
        RangeContainer rangeContainerSecond02 = new RangeContainer(topic, partition, 19, 3);
        RangeContainer rangeContainerSecond03 = new RangeContainer(topic, partition, 20, 6);
        RangeContainer rangeContainerSecond04 = new RangeContainer(topic, partition, 11, 0);
        RangeContainer rangeContainerSecond05 = new RangeContainer(topic, partition, 11, 1);

        assertEquals(RangeState.OVER_LAPPING, rangeContainerFirst.getOverLappingState(rangeContainerSecond01));
        assertEquals(RangeState.OVER_LAPPING, rangeContainerFirst.getOverLappingState(rangeContainerSecond02));
        assertEquals(RangeState.OVER_LAPPING, rangeContainerFirst.getOverLappingState(rangeContainerSecond03));
        assertEquals(RangeState.OVER_LAPPING, rangeContainerFirst.getOverLappingState(rangeContainerSecond04));
        assertEquals(RangeState.OVER_LAPPING, rangeContainerFirst.getOverLappingState(rangeContainerSecond05));

    }

}
