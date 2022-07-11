package com.clickhouse.kafka.connect.sink.kafka;

public class RangeContainer extends TopicPartitionContanier {

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

    public RangeState getOverLappingState(RangeContainer rangeContainer) {
        // SAME State [0,10] Actual [0,10]
        if ( maxOffset == rangeContainer.getMaxOffset() && minOffset == rangeContainer.getMinOffset() )
            return RangeState.SAME;
        // SAME State [0,10] Actual [11,20]
        if ( maxOffset < rangeContainer.minOffset )
            return RangeState.NEW;
        if ( minOffset < rangeContainer.getMinOffset())
            return RangeState.ERROR;
        //if ( minOffset > )
        //if ( minOffset == rangeContainer.getMinOffset() )
        return RangeState.OVER_LAPPING;
    }


    public RangeContainer getRangeContainer() {
        return this;
    }

}
