package com.clickhouse.kafka.connect.sink.kafka;

public class RangeContainer extends TopicPartitionContainer {

    private long maxOffset;
    private long minOffset;

    public RangeContainer(String topic, int partition) {
        super(topic, partition);
        this.maxOffset = -1;
        this.minOffset = Long.MAX_VALUE;
    }

    public RangeContainer(String topic, int partition, long maxOffset, long minOffset) {
        super(topic, partition);
        this.maxOffset = maxOffset;
        this.minOffset = minOffset;
    }


    /**
     * This method will set min/max values for offsets
     *
     * @param offset
     */
    public void defineInRange(long offset) {
        maxOffset = Long.max(maxOffset, offset);
        minOffset = Long.min(minOffset, offset);
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public boolean isInRange(long offset) {
        if (offset >= minOffset && offset <= maxOffset)
            return true;
        return false;
    }

    /**
     * This compares the stored state with the actual state
     * @param rangeContainer A container with the actual state
     * @return The state of the comparison
     */
    public RangeState getOverLappingState(RangeContainer rangeContainer) {
        long actualMinOffset = rangeContainer.getMinOffset();
        long actualMaxOffset = rangeContainer.getMaxOffset();

        // SAME State [0, 10] Actual [0, 10]
        if (actualMaxOffset == maxOffset && actualMinOffset <= minOffset)
            return RangeState.SAME;
        // NEW State [0, 10] Actual [11, 20]
        if (actualMinOffset > maxOffset)
            return RangeState.NEW;
        // CONTAINS [0, 10] Actual [1, 10]
        if (actualMaxOffset <= maxOffset && actualMinOffset >= minOffset)
            return RangeState.CONTAINS;
        // OVER_LAPPING
        if (actualMaxOffset > maxOffset)
            return RangeState.OVER_LAPPING;
        // ZEROED [10, 20] Actual [0, 10]
        if (actualMinOffset == 0)
            return RangeState.ZERO;
        // PREVIOUS [10, 20] Actual [5, 8]
        if (actualMaxOffset < minOffset)
            return RangeState.PREVIOUS;
        // ERROR [10, 20] Actual [8, 19]
        return RangeState.ERROR;
    }


    public RangeContainer getRangeContainer() {
        return this;
    }

    public String toString() {
        return "Topic: " + getTopic() + " Partition: " + getPartition() + " MinOffset: " + minOffset + " MaxOffset: " + maxOffset;
    }
}
