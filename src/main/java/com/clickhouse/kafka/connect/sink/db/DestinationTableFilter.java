package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.util.Utils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;

final class DestinationTableFilter implements Predicate<Table> {
    private final ClickHouseSinkConfig config;
    private final Set<String> destinationTables = ConcurrentHashMap.newKeySet();

    DestinationTableFilter(ClickHouseSinkConfig config) {
        this.config = config;
        registerConfiguredDestinations();
    }

    String registerDestination(String database, String topic) {
        String tableName = Utils.getTableName(database, topic, config.getTopicToTableMap());
        destinationTables.add(tableName);
        return tableName;
    }

    boolean hasDestinationIn(String database) {
        String escapedDatabasePrefix = Utils.escapeName(database) + ".";
        return destinationTables.stream().anyMatch(table -> table.startsWith(escapedDatabasePrefix));
    }

    @Override
    public boolean test(Table table) {
        String fullName = Utils.escapeTableName(table.getDatabase(), table.getCleanName());
        if (destinationTables.contains(fullName)) {
            return true;
        }
        if (config.getTopics().isEmpty() && config.getTopicsRegex() == null) {
            return true;
        }
        if (config.getTopicsRegex() == null) {
            return false;
        }

        String tableName = table.getCleanName();
        for (Map.Entry<String, String> entry : config.getTopicToTableMap().entrySet()) {
            if (Utils.escapeName(entry.getValue()).equals(Utils.escapeName(tableName))
                    && config.getTopicsRegex().matcher(toConfiguredTopic(table.getDatabase(), entry.getKey())).matches()) {
                return true;
            }
        }

        return !config.getTopicToTableMap().containsKey(tableName)
                && config.getTopicsRegex().matcher(toConfiguredTopic(table.getDatabase(), tableName)).matches();
    }

    private void registerConfiguredDestinations() {
        for (String configuredTopic : config.getTopics()) {
            String database = config.getDatabase();
            String topic = configuredTopic;
            if (config.isEnableDbTopicSplit()) {
                String[] parts = configuredTopic.split(Pattern.quote(config.getDbTopicSplitChar()));
                if (parts.length == 2) {
                    database = parts[0];
                    topic = parts[1];
                }
            }
            registerDestination(database, topic);
        }
    }

    private String toConfiguredTopic(String database, String topic) {
        if (config.isEnableDbTopicSplit()) {
            return database + config.getDbTopicSplitChar() + topic;
        }
        return topic;
    }
}
