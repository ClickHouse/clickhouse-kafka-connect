package com.clickhouse.kafka.connect.sink.helper;

/**
 * Represents the ClickHouse deployment mode used for a test run.
 */
public enum ClickHouseDeploymentType {

    /**
     * No cluster — single-node ClickHouse (local container).
     * DDL uses plain MergeTree; SELECTs use local table.
     */
    STANDALONE(null),

    /**
     * Clustering is abstracted away in ClickHouse cloud.
     * DDL uses plain MergeTree; SELECTs use local table.
     */
    CLOUD(null),

    /**
     * Cluster {@code three_shards_one_replica_each}: 3 shards (ch0, ch1, ch2),
     * each with 1 replica.
     * DDL uses ReplicatedMergeTree; SELECTs must use {@code cluster} to query across shards.
     */
    THREE_SHARDS_ONE_REPLICA_EACH(ClickHouseCluster.THREE_SHARDS_ONE_REPLICA_EACH),

    /**
     * Cluster {@code one_shard_three_replicas}: 1 shard replicated to all 3 nodes.
     * DDL uses ReplicatedMergeTree; SELECTs can use the local table or {@code cluster} interchangeably.
     */
    ONE_SHARD_THREE_REPLICAS(ClickHouseCluster.ONE_SHARD_THREE_REPLICAS);

    public final String clusterName; // may be null

    ClickHouseDeploymentType(String clusterName) {
        this.clusterName = clusterName;
    }

    /** Returns {@code true} when this config requires distributed DDL (ON CLUSTER & FROM cluster(...)). */
    public boolean isLocalCluster() {
        return clusterName != null;
    }

    public String getMergeTreeEngine() {
        if (!isLocalCluster()) {
            return "MergeTree";
        }
        // {database} and {table} are ClickHouse auto-substitution variables.
        // {replica} is defined in config.xml via <replica from_env="REPLICA_NUM"/>.
        if (this == THREE_SHARDS_ONE_REPLICA_EACH) {
            // Include {shard} so each shard gets its own ZK replication group.
            return "ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')";
        }
        // ONE_SHARD_THREE_REPLICAS: all nodes replicate the same data; omit {shard}
        // because the {shard} macro differs per node (1,2,3) which would break replication.
        return "ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}')";
    }
}
