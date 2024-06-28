package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.kafka.connect.sink.ClickHouseBase;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.util.Utils;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ClickHouseWriterTest extends ClickHouseBase {

    @Test
    public void updateMapping() {
        Map<String, String> props = createProps();;
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("missing_table_mapping_test");

        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16 ) Engine = MergeTree ORDER BY off16");

        ClickHouseWriter chw = new ClickHouseWriter();
        chw.setSinkConfig(createConfig());
        chw.setClient(chc);

        chw.updateMapping("default");
        Map<String, Table> tables = chw.getMapping();
        assertNull(tables.get(Utils.escapeTableName(chc.getDatabase(), topic)));

        Table table = chw.getTable(chc.getDatabase(), topic);
        assertNotNull(table);
        assertEquals(Utils.escapeTableName(chc.getDatabase(), topic), table.getFullName());

        tables = chw.getMapping();
        assertNotNull(tables.get(Utils.escapeTableName(chc.getDatabase(), topic)));

        ClickHouseTestHelpers.dropTable(chc, topic);
    }
}
