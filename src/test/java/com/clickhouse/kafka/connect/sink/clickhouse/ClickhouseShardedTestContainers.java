package com.clickhouse.kafka.connect.sink.clickhouse;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ClickhouseShardedTestContainers {

    private List<ShardConfig> shardConfigs;
    private String zookeeperImageName = "zookeeper:3.5.10";
    private String clickhouseImageName = "hub.tess.io/nudata/clickhouse-cluster-test";//"clickhouse/clickhouse-server:22.5";
    private List<GenericContainer> dbs;
    private GenericContainer zookeeper;
    private String ZookeeperAddress=null;
    private Network network;
    private int numShards = 0;

    public ClickhouseShardedTestContainers() {
        shardConfigs = new ArrayList<>();
        dbs = new ArrayList<>();
        network = Network.newNetwork();
    }
    public void setClickhouseImageName(String clickhouseImageName) {
        this.clickhouseImageName = clickhouseImageName;
    }
    public void setZookeeperImageName(String zookeeperImageName) {
        this.zookeeperImageName = zookeeperImageName;
    }
    public void addShard(int numReplicas) {
        numShards++;
        if (numReplicas<1){
            throw new RuntimeException(String.format("shard %s needs at least 1 replica not %s ",numShards,numReplicas));
        }
        for (int i = 0; i < numReplicas; i++) {
            ShardConfig config = new ShardConfig();
            config.generateClickhouseConfigFilePath(String.valueOf(numShards),String.valueOf(i));
            shardConfigs.add(config);
        }

    }

    public List<ShardConfig> getShardConfigs() {
        return shardConfigs;
    }

    public List<GenericContainer> getClickhouseInstances() {
        return dbs;
    }

    public void start(){
        if (numShards<1){
            throw new RuntimeException("no shard added to this instance");
        }
        startZookeeper();
        for (ShardConfig config:shardConfigs){
            dbs.add(createContainer(config));
        }
        for (GenericContainer db : dbs) {
            db.start();
        }
    }

    private void startZookeeper() {
        zookeeper = new GenericContainer<>(zookeeperImageName)
                .withNetwork(network)
                .withExposedPorts(2181)
                .withNetworkAliases("zookeeper")
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
                .waitingFor(Wait.forListeningPort());
        zookeeper.start();
        ZookeeperAddress = zookeeper.getContainerInfo().getNetworkSettings().getNetworks().entrySet().iterator().next().getValue().getIpAddress();
    }


    private GenericContainer createContainer(ShardConfig shardConfig) {
        if (ZookeeperAddress==null){
            throw new RuntimeException("Zookeeper Address is not given, start Zookeeper first.");
        }
        shardConfig.generateZookeeperConfigFilePath(ZookeeperAddress,String.valueOf(2181));
        GenericContainer<?> clickhouse = new GenericContainer<>(clickhouseImageName)
                .withCopyFileToContainer(MountableFile.forHostPath(shardConfig.getZookeeperConfigFilePath()),
                        "/etc/clickhouse-server/config.d/zookeeper.xml")
                .withCopyFileToContainer(MountableFile.forHostPath(shardConfig.getClickhouseConfigFilePath()),
                        "/etc/clickhouse-server/config.d/macro.xml")
                .withNetwork(network)
                .withExposedPorts(8123)
                .withEnv("MY_VAR","xyz")
                .dependsOn(zookeeper)
                .waitingFor(Wait.forListeningPort());
        return clickhouse;
    }

    public void tearDown() throws IOException {
        for (var db : dbs) {
            db.stop();
        }
        zookeeper.stop();
        for (var sc:shardConfigs) {
            sc.deleteTempFile();
        }
    }
}
