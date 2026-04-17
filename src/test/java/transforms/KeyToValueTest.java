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



    @Test
    public void applyWithDifferentSchemasTest() {
        try(KeyToValue<SinkRecord> keyToValue = new KeyToValue<>()) {
            keyToValue.configure(new HashMap<>());

            SinkRecord record1 = generateSampleRecord("topic1", 0, 0);
            SinkRecord newRecord1 = keyToValue.apply(record1);

            Schema schema2 = SchemaBuilder.struct()
                    .field("string_field", Schema.STRING_SCHEMA)
                    .build();
            Struct valueStruct2 = new Struct(schema2)
                    .put("string_field", "value");
            SinkRecord record2 = new SinkRecord("topic2", 0, null, "key2", schema2, valueStruct2, 1);
            SinkRecord newRecord2 = keyToValue.apply(record2);

            Assertions.assertTrue(newRecord1.value() instanceof Struct);
            Assertions.assertNotNull(((Struct) newRecord1.value()).get("_key"));
            Assertions.assertNotNull(((Struct) newRecord1.value()).schema().field("off16"));

            Assertions.assertTrue(newRecord2.value() instanceof Struct);
            Assertions.assertNotNull(((Struct) newRecord2.value()).get("_key"));
            Assertions.assertNotNull(((Struct) newRecord2.value()).schema().field("string_field"));
            Assertions.assertNull(((Struct) newRecord2.value()).schema().field("off16"));

            Assertions.assertNotEquals(((Struct) newRecord1.value()).schema(), ((Struct) newRecord2.value()).schema());
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
