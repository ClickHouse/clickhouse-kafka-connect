package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.util.Map;

/**
 * This class represents a CH cluster that runs locally as defined by src/testFixtures/docker/cluster/docker-compose.yml.
 */
public enum ClickHouseCluster {
    THREE_SHARDS_ONE_REPLICA_EACH("three_shards_one_replica_each"),

    /**
     * Cluster {@code one_shard_three_replicas}: 1 shard replicated to all 3 nodes.
     * DDL uses ReplicatedMergeTree; SELECTs can use the local table or {@code cluster} interchangeably.
     */
    ONE_SHARD_THREE_REPLICAS("one_shard_three_replicas");

    // fixed port mapped by docker-compose.yml: "10726:8123" for clickhouse-nginx
    // requests to the cluster are round-robin'ed
    private static final String CLUSTER_HOST = "localhost";
    private static final int CLUSTER_PORT = 10726;
    private static ComposeContainer container;

    private final String name; // may be null

    ClickHouseCluster(String name) {
        this.name = name;
    }

    public static Integer getPort() {
        return CLUSTER_PORT;
    }

    public String getName() {
        return name;
    }

    public static Map<String, String> getClusterProps(String database) {
        return Map.of(
                ClickHouseSinkConnector.HOSTNAME, CLUSTER_HOST,
                ClickHouseSinkConnector.PORT, getPort().toString(),
                ClickHouseSinkConnector.DATABASE, database,
                ClickHouseSinkConnector.USERNAME, ClickHouseTestHelpers.USERNAME_DEFAULT,
                ClickHouseSinkConnector.PASSWORD, "",
                ClickHouseSinkConnector.SSL_ENABLED, "false",
                ClickHouseSinkConfig.CLICKHOUSE_SETTINGS, "insert_quorum=3,insert_quorum_parallel=0,distributed_ddl_task_timeout=-1"
        );
    }

    /** not thread safe */
    public void start() {
        container = new ComposeContainer(new File("src/testFixtures/docker/clickhouse/cluster/docker-compose.yml")).waitingFor("nginx", Wait.defaultWaitStrategy());
        container
                .withEnv("DOCKER_ROOT", new File("src/testFixtures/docker").getAbsolutePath())
                .withEnv("CH_VERSION", ClickHouseTestHelpers.getClickhouseVersion())
                .start();
    }

    public void stop() {
        if (container != null) {
            container.stop();
        }
    }

    public String getMergeTreeEngine() {
        // {database} and {table} are ClickHouse auto-substitution variables.
        // {replica} is defined in config.xml via <replica from_env="REPLICA_NUM"/>.
        if (this == ONE_SHARD_THREE_REPLICAS) {
            // all nodes replicate the same data, so omit {shard}
            return "ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}')";
        }
        return "MergeTree";
    }

    public static ClickHouseCluster getClusterFromEnvVar() {
        String clusterName = System.getenv(ClickHouseTestHelpers.CLICKHOUSE_CLUSTER_NAME);
        if (THREE_SHARDS_ONE_REPLICA_EACH.name.equalsIgnoreCase(clusterName)) {
            return THREE_SHARDS_ONE_REPLICA_EACH;
        } else if (ONE_SHARD_THREE_REPLICAS.name.equalsIgnoreCase(clusterName)) {
            return ONE_SHARD_THREE_REPLICAS;
        }
        throw new IllegalArgumentException(String.format("Unknown cluster name from env var %s: %s", ClickHouseTestHelpers.CLICKHOUSE_CLUSTER_NAME, clusterName));
    }
}
