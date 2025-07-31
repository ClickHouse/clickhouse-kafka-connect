package kafka_connector;

import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.data.convert.RecordConvertor;
import com.clickhouse.kafka.connect.sink.data.convert.SchemaRecordConvertor;
import com.clickhouse.kafka.connect.sink.data.convert.SchemalessRecordConvertor;
import com.clickhouse.kafka.connect.sink.data.convert.StringRecordConvertor;
import kafka_connector.data.SchemaTestData;
import kafka_connector.data.SchemalessTestData;
import org.apache.kafka.connect.sink.SinkRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Collection;

@State(Scope.Benchmark)
public class SimpleBenchmark {

    private static final String TOPIC_NAME = "test_topic";

    private static final int PARTITION = 1;

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"10000", "100000", "1000000"})
        int records;

        Collection<SinkRecord> sinkRecords;

        Collection<SinkRecord> schemalessRecords;

        SchemaRecordConvertor schemaRecordConvertor;

        SchemalessRecordConvertor  schemalessRecordConvertor;

        StringRecordConvertor stringRecordConvertor;
    }

    @Setup(Level.Trial)
    public void setup(BenchmarkState state) {
        state.schemaRecordConvertor = new SchemaRecordConvertor();
        state.schemalessRecordConvertor = new SchemalessRecordConvertor();
        state.stringRecordConvertor = new StringRecordConvertor();
        state.sinkRecords = SchemaTestData.createDateType(TOPIC_NAME, PARTITION, state.records);
        state.schemalessRecords = SchemalessTestData.createPrimitiveTypes(TOPIC_NAME, PARTITION, state.records);
    }



    @Benchmark
    public void schemaConvertor(BenchmarkState state, Blackhole blackhole) {
        for (SinkRecord sinkRecord : state.sinkRecords) {
            blackhole.consume(state.schemaRecordConvertor.convert(sinkRecord, false, "_", "config_db"));
        }
    }

    @Benchmark
    public void schemalessConvertor(BenchmarkState state, Blackhole blackhole) {
        for (SinkRecord sinkRecord : state.schemalessRecords) {
            blackhole.consume(state.schemalessRecordConvertor.convert(sinkRecord, false, "_", "config_db"));
        }
    }

    @Benchmark
    public void stringConvertor(BenchmarkState state, Blackhole blackhole) {
        for (SinkRecord sinkRecord : state.sinkRecords) {
            blackhole.consume(state.stringRecordConvertor.convert(sinkRecord, false, "_", "config_db"));
        }
    }
}
