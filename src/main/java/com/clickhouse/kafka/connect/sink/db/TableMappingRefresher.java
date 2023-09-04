package com.clickhouse.kafka.connect.sink.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.TimerTask;

public class TableMappingRefresher extends TimerTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableMappingRefresher.class);
  private ClickHouseWriter chWriter = null;

  public TableMappingRefresher(final ClickHouseWriter chWriter) {
    this.chWriter = chWriter;
  }

  @Override
  public void run() {
    try {
      chWriter.updateMapping();
    } catch (Exception e) {
      LOGGER.error("Update mapping Error: {}", e.getMessage());
    }
    
  }
}
