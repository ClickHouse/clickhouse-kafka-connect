package com.clickhouse.kafka.connect.sink.helper;

import javax.annotation.Nullable;

/**
 * Represents the ClickHouse cluster topology used for a test run.
 *
 * <p>When {@code CLICKHOUSE_CLUSTER_MODE=true}, tests run against both
 * {@link #THREE_SHARDS_ONE_REPLICA_EACH} and {@link #ONE_SHARD_THREE_REPLICAS}.
 * Otherwise, tests run as {@link #STANDALONE} (single-node, no distributed DDL).
 */
public enum ClusterConfig {

    /**
     * No cluster — single-node ClickHouse (local container or cloud).
     * DDL uses plain MergeTree; SELECTs use local table.
     */
    STANDALONE(null, false),

    /**
     * Cluster {@code three_shards_one_replica_each}: 3 shards (ch0, ch1, ch2),
     * each with 1 replica. Data written to one node stays on that shard.
     * DDL uses MergeTree (local per-shard table); SELECTs must use
     * {@code clusterAllReplicas} with a sharding key to aggregate across shards.
     */
    THREE_SHARDS_ONE_REPLICA_EACH("three_shards_one_replica_each", true),

    /**
     * Cluster {@code one_shard_three_replicas}: 1 shard replicated to all 3 nodes.
     * Data written to any node is replicated to the others.
     * DDL uses ReplicatedMergeTree; SELECTs can use the local table.
     */
    ONE_SHARD_THREE_REPLICAS("one_shard_three_replicas", false);

    /** The ClickHouse cluster name, or {@code null} for STANDALONE. */
    public final @Nullable String clusterName;

    /**
     * {@code true} = local MergeTree per shard (data is sharded, not replicated).
     * {@code false} = ReplicatedMergeTree (data is replicated across all replicas).
     */
    public final boolean sharded;

    ClusterConfig(@Nullable String clusterName, boolean sharded) {
        this.clusterName = clusterName;
        this.sharded = sharded;
    }

    /** Returns {@code true} when this config requires distributed DDL (ON CLUSTER). */
    public boolean isDistributed() {
        return clusterName != null;
    }

    /**
     * Resolves the ClickHouse engine string for CREATE TABLE.
     *
     * <ul>
     *   <li>STANDALONE / THREE_SHARDS: returns {@code baseEngine} unchanged.</li>
     *   <li>ONE_SHARD_THREE_REPLICAS: wraps base engine as
     *       {@code ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{server_index}')}.</li>
     * </ul>
     *
     * @param baseEngine the base engine name, e.g. {@code "MergeTree"}
     * @return the engine string to use in the CREATE TABLE statement
     */
    public String resolveEngine(String baseEngine) {
        if (!isDistributed() || sharded) {
            return baseEngine;
        }
        // Strip any existing parameters from the engine name before wrapping
        String bare = baseEngine.replaceAll("\\s*\\(.*\\)\\s*$", "").trim();
        // Only MergeTree-family engines support the Replicated* variant
        if (!bare.endsWith("MergeTree")) {
            return baseEngine;
        }
        // {database} and {table} are ClickHouse auto-substitution variables (not user macros).
        // {replica} is defined in config.xml via <replica from_env="REPLICA_NUM"/>.
        return "Replicated" + bare +
                "('/clickhouse/tables/{database}/{table}', '{replica}')";
    }

    /**
     * Returns {@code true} when SELECT queries must use
     * {@code clusterAllReplicas} with a sharding key to read across all shards.
     * Only {@code true} for {@link #THREE_SHARDS_ONE_REPLICA_EACH}.
     */
    public boolean requiresClusterRead() {
        return sharded;
    }

    @Override
    public String toString() {
        return clusterName != null ? clusterName : "standalone";
    }
}
