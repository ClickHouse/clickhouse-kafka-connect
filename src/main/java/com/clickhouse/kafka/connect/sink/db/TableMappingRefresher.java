package com.clickhouse.kafka.connect.sink.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

public class TableMappingRefresher extends TimerTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableMappingRefresher.class);
  private ClickHouseWriter chWriter = null;
  private String database = null;

  public TableMappingRefresher(String database, final ClickHouseWriter chWriter) {
    this.chWriter = chWriter;
    this.database = database;
  }

  @Override
  public void run() {
    try {
      chWriter.updateMapping(database);
    } catch (Exception e) {
      LOGGER.error("Update mapping Error: {}", e.getMessage());
    }
    
  }
}
