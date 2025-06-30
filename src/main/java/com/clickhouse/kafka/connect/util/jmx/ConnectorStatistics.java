package com.clickhouse.kafka.connect.util.jmx;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ConnectorStatistics implements ConnectorStatisticsMBean {
  private final ConcurrentHashMap<String, AtomicLong> failedRecordsByType = new ConcurrentHashMap<>();

  @Override
  public long getFailedRecords(String errorType) {
    AtomicLong counter = failedRecordsByType.get(errorType);
    return counter != null ? counter.get() : 0L;
  }

  public void failedRecords(final String errorType, final int n) {
    failedRecordsByType.computeIfAbsent(errorType, k -> new AtomicLong(0)).addAndGet(n);
  }
}