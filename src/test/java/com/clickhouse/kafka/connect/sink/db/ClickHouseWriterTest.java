package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.kafka.connect.sink.ClickHouseBase;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.sink.db.mapping.Type;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.junit.extension.FromVersionConditionExtension;
import com.clickhouse.kafka.connect.sink.junit.extension.SinceClickHouseVersion;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers.newDescriptor;

@ExtendWith(FromVersionConditionExtension.class)
public class ClickHouseWriterTest extends ClickHouseBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseWriterTest.class);
    ClickHouseHelperClient chc = null;

    @BeforeEach
    public void setUp() {
        LOGGER.info("Setting up...");
        Map<String, String> props = createProps();
        chc = createClient(props);
    }

    @Test
    public void writeUTF8StringPrimitive() throws IOException {
        ClickHouseWriter writer = new ClickHouseWriter();
        Column column = Column.extractColumn(newDescriptor("utf8String", "String"));
        ClickHousePipedOutputStream out = new ClickHousePipedOutputStream(null) {
            List<Byte> bytes = new ArrayList<>();

            @Override
            public ClickHouseOutputStream transferBytes(byte[] bytes, int i, int i1) throws IOException {
                for (int j = i; j < i1; j++) {
                    this.bytes.add(bytes[j]);
                }
                return this;
            }

            @Override
            public ClickHouseOutputStream writeByte(byte b) throws IOException {
                this.bytes.add(b);
                return this;
            }

            @Override
            public ClickHouseOutputStream writeBytes(byte[] bytes, int i, int i1) throws IOException {
                for (int j = i; j < i1; j++) {
                    this.bytes.add(bytes[j]);
                }
                return this;
            }

            @Override
            public ClickHouseOutputStream writeCustom(ClickHouseDataUpdater clickHouseDataUpdater) throws IOException {
                return this;
            }

            @Override
            public ClickHouseInputStream getInputStream(Runnable runnable) {
                return null;
            }

            @Override
            public String toString() {
                byte[] bytes = new byte[this.bytes.size()];
                for (int i = 0; i < this.bytes.size(); i++) {
                    bytes[i] = this.bytes.get(i);
                }
                return new String(bytes, StandardCharsets.UTF_8);
            }
        };
        byte[] originalBytes = "שלום".getBytes(StandardCharsets.UTF_8);
        writer.doWritePrimitive(Type.STRING, Schema.Type.STRING, out,"שלום", column);
        byte[] newBytes = out.toString().getBytes(StandardCharsets.UTF_8);
        Assertions.assertTrue(Arrays.equals(originalBytes, Arrays.copyOfRange(newBytes, 1, newBytes.length)));//We add a length before the string
    }
}
