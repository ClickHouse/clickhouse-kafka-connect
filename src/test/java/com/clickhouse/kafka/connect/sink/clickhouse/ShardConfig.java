package com.clickhouse.kafka.connect.sink.clickhouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.attribute.PosixFilePermissions;

import java.io.*;

import java.nio.file.Path;
import java.nio.file.Files;

public class ShardConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShardConfig.class);
    private Path tempClickhousePath = null;
    private Path tempZookeeperPath = null;

    public ShardConfig() {}

    public void generateZookeeperConfigFilePath(String host, String port){
        String file = "<?xml version=\"1.0\"?>\n" +
                "<yandex>\n" +
                "    <zookeeper>\n" +
                "        <node>\n" +
                String.format("            <host>%s</host>\n",host) +
                String.format("            <port>%s</port>\n",port) +
                "        </node>\n" +
                "    </zookeeper>\n" +
                "</yandex>";
        try {
            Path tempFile = Files.createTempFile("zookeeper-", "-config",
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx")));
            Files.write(tempFile,file.getBytes());
            this.tempZookeeperPath = tempFile;
        } catch (Exception error) {
            LOGGER.error("fail to generate temporary zookeeper config file, "+error);
        }
    }

    public void generateClickhouseConfigFilePath(String shardID, String replicaID) {
        String file = "<?xml version=\"1.0\"?>\n" +
                "<yandex>\n" +
                "    <macros>\n" +
                String.format("        <shard>%s</shard>\n", shardID) +
                String.format("        <replica>%s</replica>\n", replicaID) +
                "    </macros>\n" +
                "</yandex>";
        try {
            if (tempClickhousePath != null) {
                Files.deleteIfExists(tempClickhousePath);
            }
            Path tempFile = Files.createTempFile("ch-", "-config",
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx")));
            Files.write(tempFile,file.getBytes());
            this.tempClickhousePath = tempFile;
        } catch (Exception error) {
            LOGGER.error("fail to generate temporary clickhouse config file, " + error);
        }
    }

    public Path getClickhouseConfigFilePath(){return tempClickhousePath;}
    public Path getZookeeperConfigFilePath(){return tempZookeeperPath;}

    public void deleteTempFile() throws IOException {
        if (tempZookeeperPath!=null){
            Files.deleteIfExists(tempZookeeperPath);
        }
        if (tempClickhousePath!=null) {
            Files.deleteIfExists(tempClickhousePath);
        }
    }

}
