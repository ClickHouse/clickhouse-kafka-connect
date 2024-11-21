package transforms;

import com.clickhouse.kafka.connect.transforms.KeyToValue;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;

public class KeyToValueTest {
    @Test
    public void applySchemalessTest() {
        try(KeyToValue<SinkRecord> keyToValue = new KeyToValue<>()) {
            keyToValue.configure(new HashMap<>());
            SinkRecord record = new SinkRecord(UUID.randomUUID().toString(), 0, null, "Sample Key", null, new HashMap<>(), 0);
            SinkRecord newRecord = keyToValue.apply(record);
            Assertions.assertTrue(newRecord.value() instanceof HashMap && ((HashMap) newRecord.value()).containsKey("_key"));
        }
    }

    @Test
    public void applyWithSchemaTest() {
        try(KeyToValue<SinkRecord> keyToValue = new KeyToValue<>()) {
            keyToValue.configure(new HashMap<>());
            SinkRecord record = generateSampleRecord(UUID.randomUUID().toString(), 0, 0);
            SinkRecord newRecord = keyToValue.apply(record);
            Assertions.assertTrue(newRecord.value() instanceof Struct &&
                    ((Struct) newRecord.value()).get("off16") instanceof Short &&
                    ((Struct) newRecord.value()).get("_key") != null);
        }
    }



    private SinkRecord generateSampleRecord(String topic, int partition, long offset) {
        Schema schema = SchemaBuilder.struct()
                .field("off16", Schema.INT16_SCHEMA)
                .field("timestamp_int64",  Timestamp.SCHEMA)
                .field("date_date", Time.SCHEMA)
                .build();
        Struct value_struct = new Struct(schema)
                .put("off16", (short) new Random().nextInt(Short.MAX_VALUE + 1))
                .put("timestamp_int64", new Date(System.currentTimeMillis()))
                .put("date_date", new Date(System.currentTimeMillis()));
        return new SinkRecord(topic, partition, null, "{\"sample\": \"keys\"}", schema, value_struct, offset);
    }
}
