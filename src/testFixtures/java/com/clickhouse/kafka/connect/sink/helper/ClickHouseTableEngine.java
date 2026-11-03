package com.clickhouse.kafka.connect.sink.helper;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Enum representing ClickHouse table engines.
 */
public enum ClickHouseTableEngine {
    // Add more engines as needed
    MERGE_TREE("MergeTree"),
    NULL_ENGINE("Null");

    private final String sqlName;

    ClickHouseTableEngine(String sqlName) {
        this.sqlName = sqlName;
    }

    public String getSqlName() {
        return sqlName;
    }

    /**
     * Returns the ClickHouseTableEngine corresponding to the given engine name.
     *
     * @param sqlName the name of the engine
     * @return the corresponding ClickHouseTableEngine
     * @throws IllegalArgumentException if the engine name is unknown
     */
    public static ClickHouseTableEngine fromName(String sqlName) {
        for (ClickHouseTableEngine engine : ClickHouseTableEngine.values()) {
            if (engine.getSqlName().equalsIgnoreCase(sqlName)) {
                return engine;
            }
        }
        throw new IllegalArgumentException(
                String.format("Unknown ClickHouse table engine: %s. Known engines: %s",
                        sqlName,
                        Arrays.stream(ClickHouseTableEngine.values())
                                .map(ClickHouseTableEngine::getSqlName)
                                .collect(Collectors.joining(", ")))
        );
    }
}
