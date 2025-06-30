package com.clickhouse.kafka.connect.util.jmx;

public interface ConnectorStatisticsMBean {

  long getFailedRecords(String errorType);
}
