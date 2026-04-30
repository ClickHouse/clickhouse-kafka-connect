package kafka_connector;

import com.clickhouse.kafka.connect.transforms.KeyToValue;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@State(Scope.Benchmark)
public class KeyToValueBenchmark {

    private static final String TOPIC_NAME = "test_topic";
    private static final int PARTITION = 1;

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"10000", "100000", "1000000"})
        int records;

        Collection<SinkRecord> sinkRecords;

        KeyToValue<SinkRecord> keyToValueTransform;

        @Setup(Level.Trial)
        public void setup() {
            keyToValueTransform = new KeyToValue<>();
            Map<String, String> configs = new HashMap<>();
            configs.put("field", "_key");
            keyToValueTransform.configure(configs);

            Schema keySchema = Schema.INT32_SCHEMA;

            Schema valueSchemaV1 = SchemaBuilder.struct()
                    .name("com.example.Record")
                    .version(1)
                    .field("off16", Schema.INT16_SCHEMA)
                    .field("string", Schema.STRING_SCHEMA)
                    .build();

            Schema valueSchemaV2 = SchemaBuilder.struct()
                    .name("com.example.Record")
                    .version(2)
                    .field("off16", Schema.INT16_SCHEMA)
                    .field("string", Schema.STRING_SCHEMA)
                    .field("extra_field", Schema.OPTIONAL_STRING_SCHEMA)
                    .build();

            List<SinkRecord> array = new ArrayList<>();
            for (int n = 0; n < records; n++) {
                // Alternate between schema versions to simulate schema evolution
                // This tests cache hits and misses
                Schema currentSchema = (n % 2 == 0) ? valueSchemaV1 : valueSchemaV2;

                Struct value_struct;
                if (currentSchema == valueSchemaV1) {
                    value_struct = new Struct(valueSchemaV1)
                            .put("off16", (short) n)
                            .put("string", "test string");
                } else {
                    value_struct = new Struct(valueSchemaV2)
                            .put("off16", (short) n)
                            .put("string", "test string")
                            .put("extra_field", "extra value");
                }

                SinkRecord sr = new SinkRecord(
                        TOPIC_NAME,
                        PARTITION,
                        keySchema,
                        n, // integer key
                        currentSchema,
                        value_struct,
                        n,
                        System.currentTimeMillis(),
                        TimestampType.CREATE_TIME
                );

                array.add(sr);
            }
            this.sinkRecords = array;
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            if (keyToValueTransform != null) {
                keyToValueTransform.close();
            }
        }
    }

    @Benchmark
    public void transformWithSchema(BenchmarkState state, Blackhole blackhole) {
        for (SinkRecord sinkRecord : state.sinkRecords) {
            blackhole.consume(state.keyToValueTransform.apply(sinkRecord));
        }
    }
}