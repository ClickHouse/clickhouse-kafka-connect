package com.clickhouse.kafka.connect.benchmark;

import com.clickhouse.kafka.connect.sink.ClickHouseBase;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkTask;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import com.clickhouse.kafka.connect.sink.helper.SchemaTestData;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Benchmark v2 (task 27) — validates the FULL ClickBench {@code hits} column mapping end-to-end,
 * once, in CI, rather than discovering a broken column at 200K rows/s during a load test.
 *
 * <p>Path exercised: records conforming to {@code benchmarks/e2e/schema/hits.avsc}
 * -&gt; {@link io.confluent.connect.avro.AvroConverter} (mock schema registry, no live registry)
 * -&gt; {@link ClickHouseSinkTask#put} against a Testcontainers ClickHouse whose {@code hits} table
 * uses the CH types the DDL aliases resolve to (source of truth:
 * {@code spark-clickhouse-connector/benchmarks/sql/clickbench/02_create_hits.sql})
 * -&gt; SELECT back and assert every one of the 105 columns round-trips.
 *
 * <p>Type-mapping decisions proven here (see benchmarks/e2e/schema/README.md):
 * <ul>
 *   <li>CH {@code SMALLINT} == Int16 requires a Connect INT16 value (Java {@code Short}); the sink's
 *       {@code doWritePrimitive} casts strictly to {@code Short}. Avro has no 16-bit type, so the
 *       {@code .avsc} annotates {@code int} with {@code "connect.type":"int16"} — this test proves
 *       AvroConverter honors that and yields a {@code Short}.</li>
 *   <li>CH {@code TIMESTAMP} == DateTime is fed epoch-seconds as a bare Avro {@code long}
 *       (NOT {@code timestamp-micros} / Connect Timestamp, which the sink maps to DateTime64(3)).</li>
 *   <li>CH {@code Date} is fed an Avro {@code int} logical {@code date} (days since epoch).</li>
 * </ul>
 */
public class HitsAvroMappingTest extends ClickHouseBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(HitsAvroMappingTest.class);

    /** Resolve hits.avsc relative to the project root regardless of the test working directory. */
    private static Path avscPath() {
        Path direct = Path.of("benchmarks", "e2e", "schema", "hits.avsc");
        if (Files.exists(direct)) {
            return direct;
        }
        return Path.of("..", "benchmarks", "e2e", "schema", "hits.avsc");
    }

    /**
     * The 105 columns in DDL order, paired with the ClickHouse type each DDL alias resolves to.
     * Verified against 02_create_hits.sql via `clickhouse local` DESCRIBE:
     * SMALLINT-&gt;Int16, INTEGER-&gt;Int32, BIGINT-&gt;Int64, TEXT/VARCHAR/CHAR-&gt;String,
     * TIMESTAMP-&gt;DateTime, Date-&gt;Date.
     */
    private static final LinkedHashMap<String, String> COLUMNS = new LinkedHashMap<>();
    static {
        COLUMNS.put("WatchID", "Int64");
        COLUMNS.put("JavaEnable", "Int16");
        COLUMNS.put("Title", "String");
        COLUMNS.put("GoodEvent", "Int16");
        COLUMNS.put("EventTime", "DateTime");
        COLUMNS.put("EventDate", "Date");
        COLUMNS.put("CounterID", "Int32");
        COLUMNS.put("ClientIP", "Int32");
        COLUMNS.put("RegionID", "Int32");
        COLUMNS.put("UserID", "Int64");
        COLUMNS.put("CounterClass", "Int16");
        COLUMNS.put("OS", "Int16");
        COLUMNS.put("UserAgent", "Int16");
        COLUMNS.put("URL", "String");
        COLUMNS.put("Referer", "String");
        COLUMNS.put("IsRefresh", "Int16");
        COLUMNS.put("RefererCategoryID", "Int16");
        COLUMNS.put("RefererRegionID", "Int32");
        COLUMNS.put("URLCategoryID", "Int16");
        COLUMNS.put("URLRegionID", "Int32");
        COLUMNS.put("ResolutionWidth", "Int16");
        COLUMNS.put("ResolutionHeight", "Int16");
        COLUMNS.put("ResolutionDepth", "Int16");
        COLUMNS.put("FlashMajor", "Int16");
        COLUMNS.put("FlashMinor", "Int16");
        COLUMNS.put("FlashMinor2", "String");
        COLUMNS.put("NetMajor", "Int16");
        COLUMNS.put("NetMinor", "Int16");
        COLUMNS.put("UserAgentMajor", "Int16");
        COLUMNS.put("UserAgentMinor", "String");
        COLUMNS.put("CookieEnable", "Int16");
        COLUMNS.put("JavascriptEnable", "Int16");
        COLUMNS.put("IsMobile", "Int16");
        COLUMNS.put("MobilePhone", "Int16");
        COLUMNS.put("MobilePhoneModel", "String");
        COLUMNS.put("Params", "String");
        COLUMNS.put("IPNetworkID", "Int32");
        COLUMNS.put("TraficSourceID", "Int16");
        COLUMNS.put("SearchEngineID", "Int16");
        COLUMNS.put("SearchPhrase", "String");
        COLUMNS.put("AdvEngineID", "Int16");
        COLUMNS.put("IsArtifical", "Int16");
        COLUMNS.put("WindowClientWidth", "Int16");
        COLUMNS.put("WindowClientHeight", "Int16");
        COLUMNS.put("ClientTimeZone", "Int16");
        COLUMNS.put("ClientEventTime", "DateTime");
        COLUMNS.put("SilverlightVersion1", "Int16");
        COLUMNS.put("SilverlightVersion2", "Int16");
        COLUMNS.put("SilverlightVersion3", "Int32");
        COLUMNS.put("SilverlightVersion4", "Int16");
        COLUMNS.put("PageCharset", "String");
        COLUMNS.put("CodeVersion", "Int32");
        COLUMNS.put("IsLink", "Int16");
        COLUMNS.put("IsDownload", "Int16");
        COLUMNS.put("IsNotBounce", "Int16");
        COLUMNS.put("FUniqID", "Int64");
        COLUMNS.put("OriginalURL", "String");
        COLUMNS.put("HID", "Int32");
        COLUMNS.put("IsOldCounter", "Int16");
        COLUMNS.put("IsEvent", "Int16");
        COLUMNS.put("IsParameter", "Int16");
        COLUMNS.put("DontCountHits", "Int16");
        COLUMNS.put("WithHash", "Int16");
        COLUMNS.put("HitColor", "String");
        COLUMNS.put("LocalEventTime", "DateTime");
        COLUMNS.put("Age", "Int16");
        COLUMNS.put("Sex", "Int16");
        COLUMNS.put("Income", "Int16");
        COLUMNS.put("Interests", "Int16");
        COLUMNS.put("Robotness", "Int16");
        COLUMNS.put("RemoteIP", "Int32");
        COLUMNS.put("WindowName", "Int32");
        COLUMNS.put("OpenerName", "Int32");
        COLUMNS.put("HistoryLength", "Int16");
        COLUMNS.put("BrowserLanguage", "String");
        COLUMNS.put("BrowserCountry", "String");
        COLUMNS.put("SocialNetwork", "String");
        COLUMNS.put("SocialAction", "String");
        COLUMNS.put("HTTPError", "Int16");
        COLUMNS.put("SendTiming", "Int32");
        COLUMNS.put("DNSTiming", "Int32");
        COLUMNS.put("ConnectTiming", "Int32");
        COLUMNS.put("ResponseStartTiming", "Int32");
        COLUMNS.put("ResponseEndTiming", "Int32");
        COLUMNS.put("FetchTiming", "Int32");
        COLUMNS.put("SocialSourceNetworkID", "Int16");
        COLUMNS.put("SocialSourcePage", "String");
        COLUMNS.put("ParamPrice", "Int64");
        COLUMNS.put("ParamOrderID", "String");
        COLUMNS.put("ParamCurrency", "String");
        COLUMNS.put("ParamCurrencyID", "Int16");
        COLUMNS.put("OpenstatServiceName", "String");
        COLUMNS.put("OpenstatCampaignID", "String");
        COLUMNS.put("OpenstatAdID", "String");
        COLUMNS.put("OpenstatSourceID", "String");
        COLUMNS.put("UTMSource", "String");
        COLUMNS.put("UTMMedium", "String");
        COLUMNS.put("UTMCampaign", "String");
        COLUMNS.put("UTMContent", "String");
        COLUMNS.put("UTMTerm", "String");
        COLUMNS.put("FromTag", "String");
        COLUMNS.put("HasGCLID", "Int16");
        COLUMNS.put("RefererHash", "Int64");
        COLUMNS.put("URLHash", "Int64");
        COLUMNS.put("CLID", "Int32");
    }

    @Test
    public void allColumnsRoundTrip() throws Exception {
        assertEquals(105, COLUMNS.size(), "hits schema must be exactly 105 columns");

        Map<String, String> props = getBaseProps();
        ClickHouseHelperClient chc = ClickHouseTestHelpers.createClient(props);
        String topic = createTopicName("hits_avro_mapping");

        // 1. Create the target table with the CH types the DDL aliases resolve to.
        ClickHouseTestHelpers.dropTable(chc, topic);
        CreateTableStatement stmt = new CreateTableStatement()
                .tableName(topic)
                .engine("MergeTree")
                .orderByColumn("(CounterID, EventDate, EventTime)");
        COLUMNS.forEach(stmt::column);
        stmt.execute(chc);

        // 2. Load the Avro schema (the artifact under test) and build edge-value records.
        Schema avro = new Schema.Parser().parse(Files.readString(avscPath()));
        assertEquals(105, avro.getFields().size(), "hits.avsc must declare exactly 105 fields");
        List<Object> genericRecords = buildEdgeRecords(avro);
        int expectedRows = genericRecords.size();

        // 3. Avro -> Confluent AvroConverter -> SinkRecords (mock schema registry).
        List<SinkRecord> sinkRecords =
                SchemaTestData.convertAvroToSinkRecord(topic, new AvroSchema(avro), genericRecords);

        // 4. Feed through the actual sink.
        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(props);
        task.put(sinkRecords);
        task.stop();

        // 5. Read back and assert every column round-trips for every row.
        assertEquals(expectedRows, ClickHouseTestHelpers.countRows(chc, topic));
        List<JSONObject> rows = ClickHouseTestHelpers.getAllRowsAsJson(chc, topic);
        assertEquals(expectedRows, rows.size());
        // SELECT * has no guaranteed order; key rows by WatchID (unique per row by construction).
        Map<Long, JSONObject> rowsByWatchId = new LinkedHashMap<>();
        for (JSONObject row : rows) {
            rowsByWatchId.put(row.getLong("WatchID"), row);
        }

        for (Map<String, Object> exp : EXPECTED) {
            long watchId = ((Number) exp.get("WatchID")).longValue();
            JSONObject row = rowsByWatchId.get(watchId);
            assertTrue(row != null, "no row read back for WatchID=" + watchId);
            for (Map.Entry<String, String> col : COLUMNS.entrySet()) {
                String name = col.getKey();
                String chType = col.getValue();
                Object want = exp.get(name);
                switch (chType) {
                    case "Int64":
                        assertEquals(((Number) want).longValue(), row.getLong(name),
                                "column " + name + " (Int64)");
                        break;
                    case "Int32":
                    case "Int16":
                        assertEquals(((Number) want).longValue(), row.getLong(name),
                                "column " + name + " (" + chType + ")");
                        break;
                    case "String":
                        assertEquals(String.valueOf(want), row.get(name).toString(),
                                "column " + name + " (String)");
                        break;
                    case "DateTime":
                        // stored as epoch seconds; CH renders "yyyy-MM-dd HH:mm:ss".
                        assertEquals(want, epochSecondsOf(row.get(name).toString()),
                                "column " + name + " (DateTime) rendered=" + row.get(name));
                        break;
                    case "Date":
                        assertEquals(want, epochDaysOf(row.get(name).toString()),
                                "column " + name + " (Date) rendered=" + row.get(name));
                        break;
                    default:
                        throw new IllegalStateException("unhandled CH type " + chType);
                }
            }
        }
        LOGGER.info("Validated all {} columns across {} rows", COLUMNS.size(), rows.size());
    }

    private static long epochSecondsOf(String rendered) {
        // CH DateTime renders in UTC as "yyyy-MM-dd HH:mm:ss".
        return java.time.LocalDateTime.parse(rendered.replace(' ', 'T'))
                .toEpochSecond(java.time.ZoneOffset.UTC);
    }

    private static long epochDaysOf(String rendered) {
        return java.time.LocalDate.parse(rendered).toEpochDay();
    }

    // Expected values per row, keyed by column name, in creation order.
    private static final List<Map<String, Object>> EXPECTED = new ArrayList<>();

    /**
     * Build 6 edge-value rows: zero, negatives (signed widths), width maxima, empty/unicode strings,
     * and DateTime/Date epoch boundaries. Every value is recorded into {@link #EXPECTED} for readback
     * assertion.
     */
    private List<Object> buildEdgeRecords(Schema avro) {
        EXPECTED.clear();
        // Per-row "profiles" that pick edge values appropriate to each width.
        long[] i64 = { 0L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 42L, 1234567890123L };
        int[]  i32 = { 0, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 7, -7 };
        short[] i16 = { 0, -1, Short.MAX_VALUE, Short.MIN_VALUE, 3, -3 };
        String[] str = { "", "a", "unicode-é中文-🚀", "line\ttab",
                "The quick brown fox", "x".repeat(300) };
        // DateTime is UInt32 epoch-seconds on the server: [0, 4294967295]. No negatives.
        long[] dt = { 0L, 1L, 4294967295L, 1000000000L, 1609459200L, 253402300799L % 4294967296L };
        // Date is UInt16 days on the server: [0, 65535].
        int[] date = { 0, 1, 65535, 30000, 20089 /* ~2025-01-01 */, 12345 };

        int nRows = 6;
        List<Object> out = new ArrayList<>(nRows);
        for (int r = 0; r < nRows; r++) {
            GenericRecord rec = new GenericData.Record(avro);
            Map<String, Object> exp = new LinkedHashMap<>();
            // Force distinct WatchID per row for deterministic ordering on readback.
            for (Map.Entry<String, String> col : COLUMNS.entrySet()) {
                String name = col.getKey();
                String chType = col.getValue();
                Object avroVal;
                Object expVal;
                switch (chType) {
                    case "Int64":
                        long lv = "WatchID".equals(name) ? (long) r : i64[r];
                        avroVal = lv; expVal = lv;
                        break;
                    case "Int32":
                        int iv = i32[r];
                        avroVal = iv; expVal = (long) iv;
                        break;
                    case "Int16":
                        short sv = i16[r];
                        // Avro int on the wire; connect.type=int16 makes the Connect value a Short.
                        avroVal = (int) sv; expVal = (long) sv;
                        break;
                    case "String":
                        String s = str[r];
                        avroVal = s; expVal = s;
                        break;
                    case "DateTime":
                        long secs = dt[r];
                        avroVal = secs;      // bare long epoch-seconds
                        expVal = secs;
                        break;
                    case "Date":
                        int days = date[r];
                        avroVal = days;      // logical date == days since epoch (int)
                        expVal = (long) days;
                        break;
                    default:
                        throw new IllegalStateException("unhandled CH type " + chType);
                }
                rec.put(name, avroVal);
                exp.put(name, expVal);
            }
            out.add(rec);
            EXPECTED.add(exp);
        }
        return out;
    }
}
