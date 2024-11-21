package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.kafka.connect.sink.ClickHouseBase;
import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.sink.db.mapping.Type;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.junit.extension.FromVersionConditionExtension;
import com.clickhouse.kafka.connect.util.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers.newDescriptor;
import static org.junit.jupiter.api.Assertions.*;

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
        assertTrue(Arrays.equals(originalBytes, Arrays.copyOfRange(newBytes, 1, newBytes.length)));//We add a length before the string
    }

    @Test
    public void updateMapping() {
        Map<String, String> props = createProps();;
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("missing_table_mapping_test");

        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s ( `off16` Int16 ) Engine = MergeTree ORDER BY off16");

        ClickHouseWriter chw = new ClickHouseWriter();
        chw.setSinkConfig(createConfig());
        chw.setClient(chc);

        chw.updateMapping("default");
        Map<String, Table> tables = chw.getMapping();
        assertNull(tables.get(Utils.escapeTableName(chc.getDatabase(), topic)));

        Table table = chw.getTable(chc.getDatabase(), topic);
        assertNotNull(table);
        assertEquals(Utils.escapeTableName(chc.getDatabase(), topic), table.getFullName());

        tables = chw.getMapping();
        assertNotNull(tables.get(Utils.escapeTableName(chc.getDatabase(), topic)));

        ClickHouseTestHelpers.dropTable(chc, topic);
    }

    @Test
    public void doWriteColValue_Tuples() {
        Map<String, String> props = createProps();;
        ClickHouseHelperClient chc = createClient(props);
        String topic = createTopicName("do_write_col_value_tuples_test");

        ClickHouseTestHelpers.dropTable(chc, topic);
        ClickHouseTestHelpers.createTable(chc, topic, "CREATE TABLE %s (`_id` String, `result` Tuple(`id` String, `isanswered` Int32, `relevancescore` Float64, `subject` String, `istextanswered` Int32 )) Engine = MergeTree ORDER BY _id");

        ClickHouseWriter chw = new ClickHouseWriter();
        chw.setSinkConfig(createConfig());
        chw.setClient(chc);

        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("isanswered", Schema.INT32_SCHEMA)
                .field("relevancescore", Schema.FLOAT64_SCHEMA)
                .field("subject", Schema.STRING_SCHEMA)
                .field("istextanswered", Schema.INT32_SCHEMA)
                .build();

        Struct dataObject = new Struct(schema)
                .put("id", "24554770")
                .put("isanswered", 1)
                .put("relevancescore", 84.7)
                .put("subject", "SUBJECT")
                .put("istextanswered", 1);

        Data data = new Data(schema, dataObject);

        List<Column> tupleFields = Arrays.asList(
                Column.builder().name("result.id").type(Type.STRING).hasDefault(true).build(),
                Column.builder().name("result.isanswered").type(Type.INT32).hasDefault(true).build(),
                Column.builder().name("result.relevancescore").type(Type.FLOAT64).hasDefault(true).build(),
                Column.builder().name("result.subject").type(Type.STRING).hasDefault(true).build(),
                Column.builder().name("result.istextanswered").type(Type.INT32).hasDefault(true).build()
        );

        Column column = Column.builder()
                .name("result")
                .type(Type.TUPLE)
                .tupleFields(tupleFields)
                .build();

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

        try {
            chw.doWriteColValue(column, out, data, true);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        ClickHouseTestHelpers.dropTable(chc, topic);
    }
}