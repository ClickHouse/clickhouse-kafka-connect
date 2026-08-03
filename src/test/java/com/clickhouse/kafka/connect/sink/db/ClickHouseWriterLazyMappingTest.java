package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClickHouseWriterLazyMappingTest {
    @Mock private ClickHouseHelperClient helperClient;
    @Mock private ClickHouseSinkConfig sinkConfig;

    private ClickHouseWriter writer;

    @BeforeEach
    void setUp() {
        writer =
                new ClickHouseWriter(
                        new SinkTaskStatistics(0), helperClient, sinkConfig);
    }

    @Test
    void describesDestinationOnlyOnFirstUse() {
        when(sinkConfig.getTopicToTableMap()).thenReturn(Map.of());
        Table described = new Table("default", "events");
        when(helperClient.describeTable("default", "events")).thenReturn(described);

        assertSame(described, writer.getTable("default", "events"));
        assertSame(described, writer.getTable("default", "events"));

        verify(helperClient, times(1)).describeTable("default", "events");
    }

    @Test
    void appliesTopicMappingAndRuntimeDatabaseBeforeDescribing() {
        when(sinkConfig.getTopicToTableMap()).thenReturn(Map.of("events", "events_v2"));
        Table described = new Table("tenant", "events_v2");
        when(helperClient.describeTable("tenant", "events_v2")).thenReturn(described);

        assertSame(described, writer.getTable("tenant", "events"));

        verify(helperClient).describeTable("tenant", "events_v2");
    }

    @Test
    void refreshesOnlyTablesAlreadyInTheCache() {
        Table original = new Table("default", "events");
        Table refreshed = new Table("default", "events");
        writer.getMapping().put(original.getFullName(), original);
        when(helperClient.describeTable("default", "events")).thenReturn(refreshed);

        assertTrue(writer.updateMapping());

        assertSame(refreshed, writer.getMapping().get(original.getFullName()));
        verify(helperClient).describeTable("default", "events");
        verify(helperClient, never()).extractTablesMapping("default", writer.getMapping());
    }
}
