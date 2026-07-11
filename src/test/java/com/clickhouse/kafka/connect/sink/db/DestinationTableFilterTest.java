package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DestinationTableFilterTest {
    @Test
    public void filtersExplicitTopics() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConfig.DATABASE, "analytics");
        props.put(ClickHouseSinkConfig.TOPICS, "orders, customers");

        DestinationTableFilter filter = new DestinationTableFilter(new ClickHouseSinkConfig(props));

        assertTrue(filter.test(new Table("analytics", "orders")));
        assertTrue(filter.test(new Table("analytics", "customers")));
        assertFalse(filter.test(new Table("analytics", "unrelated")));
    }

    @Test
    public void filtersRegexTopicsAndMappings() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConfig.DATABASE, "analytics");
        props.put(ClickHouseSinkConfig.TOPICS_REGEX, "events_.*");
        props.put(ClickHouseSinkConfig.TABLE_MAPPING, "events_orders=orders");

        DestinationTableFilter filter = new DestinationTableFilter(new ClickHouseSinkConfig(props));

        assertTrue(filter.test(new Table("analytics", "orders")));
        assertTrue(filter.test(new Table("analytics", "events_customers")));
        assertFalse(filter.test(new Table("analytics", "events_orders")));
        assertFalse(filter.test(new Table("analytics", "customers")));
    }

    @Test
    public void handlesDatabaseTopicSplitting() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConfig.DATABASE, "default");
        props.put(ClickHouseSinkConfig.TOPICS, "analytics.orders");
        props.put(ClickHouseSinkConfig.TOPICS_REGEX, Pattern.quote("analytics.customers"));
        props.put(ClickHouseSinkConfig.ENABLE_DB_TOPIC_SPLIT, "true");
        props.put(ClickHouseSinkConfig.DB_TOPIC_SPLIT_CHAR, ".");

        DestinationTableFilter filter = new DestinationTableFilter(new ClickHouseSinkConfig(props));

        assertTrue(filter.test(new Table("analytics", "orders")));
        assertTrue(filter.test(new Table("analytics", "customers")));
        assertFalse(filter.test(new Table("default", "orders")));
    }

    @Test
    public void includesRuntimeDestinations() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConfig.DATABASE, "analytics");
        props.put(ClickHouseSinkConfig.TOPICS_REGEX, "events_.*");
        DestinationTableFilter filter = new DestinationTableFilter(new ClickHouseSinkConfig(props));

        assertFalse(filter.test(new Table("analytics", "runtime_table")));
        filter.registerDestination("analytics", "runtime_table");
        assertTrue(filter.test(new Table("analytics", "runtime_table")));
    }
}
