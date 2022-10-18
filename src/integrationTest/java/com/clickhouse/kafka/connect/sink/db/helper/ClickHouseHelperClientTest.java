package com.clickhouse.kafka.connect.sink.db.helper;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.sink.db.mapping.Type;
import jdk.jfr.Description;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.ClickHouseContainer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ClickHouseHelperClientTest {
    private static ClickHouseHelperClient chc = null;
    private static ClickHouseContainer db = null;

    @BeforeAll
    private static void setup() {
        db = new ClickHouseContainer("clickhouse/clickhouse-server:22.5");
        db.start();

        chc = new ClickHouseHelperClient.ClickHouseClientBuilder(db.getHost(), db.getFirstMappedPort())
                .setDatabase("default")
                .setUsername(db.getUsername())
                .setPassword(db.getPassword())
                .sslEnable(false)
                .setTimeout(30)
                .setRetry(3)
                .build();

    }

    @Test
    @Order(1)
    @Description("CreateTableTest")
    public void CreateTableTest() {
        String name = "create_table_test";
        String createTable = String.format("CREATE TABLE %s ( `off` Int8 , `str` String , `double` DOUBLE, `arr` Array(Int8),  `bool` BOOLEAN)  Engine = MergeTree ORDER BY off", name);
        chc.query(createTable);
        List<String> tables = chc.showTables();
        assertEquals(1, tables.size());
        assertEquals(name, tables.get(0));
    }

    @Test
    @Order(2)
    @Description("DescTableTest")
    public void DescTableTest() {
        String name = "desc_table_test";
        String createTable = String.format("CREATE TABLE %s ( `off` Int8 , `str` String , `double` DOUBLE, `arr` Array(Int8),  `bool` BOOLEAN)  Engine = MergeTree ORDER BY off", name);
        chc.query(createTable);
        Table table = chc.describeTable(name);
        assertEquals(name, table.getName());
        assertEquals(Type.INT8, table.getColumnByName("off").getType());
        assertEquals(Type.STRING, table.getColumnByName("str").getType());
        assertEquals(Type.FLOAT64, table.getColumnByName("double").getType());
        assertEquals(Type.BOOLEAN, table.getColumnByName("bool").getType());
    }



    @AfterAll
    protected static void tearDown() {
        db.stop();
    }

}
