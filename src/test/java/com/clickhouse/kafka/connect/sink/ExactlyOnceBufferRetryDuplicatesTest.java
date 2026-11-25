package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseCluster;
import com.clickhouse.kafka.connect.sink.helper.ClickHouseTestHelpers;
import com.clickhouse.kafka.connect.sink.helper.CreateTableStatement;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Reproduces https://github.com/ClickHouse/clickhouse-kafka-connect/issues/895: with exactly-once buffering
 * ({@code exactlyOnce=true}, {@code bufferCount > 0}) a flush that fails with a retriable error leaves the records of
 * that put() in the per-partition buffer. Kafka Connect redelivers the same batch, so the buffer holds those records
 * twice, the later chunks no longer match the original offset ranges, and some records reach ClickHouse twice.
 *
 * <p>The connector talks to ClickHouse through Toxiproxy so the connection can be cut during the flush, like
 * ClickHouse restarting or a network outage. Assertions use a direct client. Same pattern as #801, fixed for
 * at-least-once buffering in #835.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExactlyOnceBufferRetryDuplicatesTest extends ClickHouseBase {

    private static final int BUFFER_COUNT = 500;
    private static final int PROXY_PORT = 8666;

    private static final Supplier<CreateTableStatement> PRIMITIVE_TYPES_TABLE = () -> CreateTableStatement.create()
            .column("off16", "Int16")
            .column("str", "String")
            .column("p_int8", "Int8")
            .column("p_int16", "Int16")
            .column("p_int32", "Int32")
            .column("p_int64", "Int64")
            .column("p_float32", "Float32")
            .column("p_float64", "Float64")
            .column("p_bool", "Bool")
            .orderByColumn("off16");

    private ToxiproxyContainer toxiproxy;
    private Proxy proxy;

    @BeforeAll
    public void setup() throws IOException {
        super.setup();
        toxiproxy = new ToxiproxyContainer(ClickHouseTestHelpers.TOXIPROXY_DOCKER_IMAGE_NAME)
                .withNetwork(isCluster || isCloud ? Network.newNetwork() : db.getNetwork())
                .withNetworkAliases(ClickHouseTestHelpers.TOXIPROXY_NETWORK_ALIAS);
        if (isCluster) {
            toxiproxy = toxiproxy.withExtraHost("host.docker.internal", "host-gateway");
        }
        toxiproxy.start();

        String upstream;
        if (isCloud) {
            ClickHouseSinkConfig csc = new ClickHouseSinkConfig(getBaseProps());
            upstream = String.format("%s:%d", csc.getHostname(), csc.getPort());
        } else if (isCluster) {
            upstream = String.format("host.docker.internal:%d", cluster.getPort());
        } else {
            upstream = String.format("%s:%d", ClickHouseTestHelpers.CLICKHOUSE_DB_NETWORK_ALIAS, ClickHouseProtocol.HTTP.getDefaultPort());
        }
        proxy = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort())
                .createProxy("clickhouse-proxy", "0.0.0.0:" + PROXY_PORT, upstream);
    }

    @AfterAll
    public void tearDown() {
        super.tearDown();
        if (toxiproxy != null) {
            toxiproxy.stop();
        }
    }

    @Test
    public void retriableFlushFailureDoesNotDuplicateRecordsWithExactlyOnceBuffering() throws IOException {
        ClickHouseHelperClient chc = createTestClient();
        String topic = createTopicName("eo_buffer_retry_duplicates_test");
        ClickHouseTestHelpers.dropTable(chc, topic);
        PRIMITIVE_TYPES_TABLE.get().tableName(topic).execute(chc);

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.start(exactlyOnceBufferPropsThroughProxy());
        try {
            // Offsets [0..299] stay in the buffer: below bufferCount.
            task.put(createRecords(topic, 0, 300));
            assertEquals(0, ClickHouseTestHelpers.countRows(chc, topic), "300 records are below bufferCount and must stay buffered");

            // Offsets [300..599] fill the buffer to 600, so chunk [0..499] is flushed while ClickHouse is unreachable.
            List<SinkRecord> batch2 = createRecords(topic, 300, 300);
            proxy.disable();
            try {
                assertThrows(RetriableException.class, () -> task.put(batch2),
                        "A lost connection during the flush must surface as a RetriableException so Connect redelivers the batch");
            } finally {
                proxy.enable();
            }

            // ClickHouse is reachable again and Kafka Connect redelivers exactly the same batch.
            task.put(batch2);

            // Offsets [600..999]: enough to flush everything up to offset 999 in two chunks of 500.
            task.put(createRecords(topic, 600, 400));
        } finally {
            task.stop();
        }

        long rows = ClickHouseTestHelpers.countRows(chc, topic);
        long distinctOffsets = countDistinctOffsets(chc, topic);
        assertEquals(distinctOffsets, rows, String.format(
                "Every Kafka record must be written at most once: %d rows but only %d distinct offsets", rows, distinctOffsets));
        assertEquals(1000, rows, "Offsets [0..999] fill exactly two chunks of " + BUFFER_COUNT + " and must all be written");
    }

    /** Exactly-once buffering, connecting through the proxy. A lost connection is retriable (retryOnSocketException). */
    private Map<String, String> exactlyOnceBufferPropsThroughProxy() {
        Map<String, String> props = getConnectorProps();
        if (isCloud) {
            // TLS through the proxy needs the real hostname as SNI (see ClickHouseSinkTaskSchemalessProxyTest).
            props.put(ClickHouseSinkConfig.SSL_SOCKET_SNI, props.get(ClickHouseSinkConfig.HOSTNAME));
        }
        props.put(ClickHouseSinkConfig.HOSTNAME, toxiproxy.getHost());
        props.put(ClickHouseSinkConfig.PORT, String.valueOf(toxiproxy.getMappedPort(PROXY_PORT)));
        // KeeperStateProvider.getStateRecord still uses the V1 API (see ClickHouseSinkTaskBufferTest).
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, "V1");
        props.put(ClickHouseSinkConfig.BUFFER_COUNT, String.valueOf(BUFFER_COUNT));
        props.put(ClickHouseSinkConfig.BUFFER_FLUSH_TIME, "0");
        props.put(ClickHouseSinkConfig.EXACTLY_ONCE, "true");
        props.put(ClickHouseSinkConfig.IGNORE_PARTITIONS_WHEN_BATCHING, "false");
        props.put(ClickHouseSinkConfig.RETRY_ON_SOCKET_EXCEPTION, "true");
        if (isCluster) {
            props.put(ClickHouseSinkConfig.KEEPER_ON_CLUSTER, ClickHouseCluster.getClusterFromEnvVarOrThrow().getName());
        }
        return props;
    }

    /** {@code count} records on partition 0 with consecutive Kafka offsets starting at {@code startOffset}. */
    private static List<SinkRecord> createRecords(String topic, long startOffset, int count) {
        List<SinkRecord> records = new ArrayList<>(count);
        for (long off = startOffset; off < startOffset + count; off++) {
            Map<String, Object> value = new HashMap<>();
            value.put("off16", (short) off);
            value.put("str", "num" + off);
            value.put("p_int8", (byte) off);
            value.put("p_int16", (short) off);
            value.put("p_int32", (int) off);
            value.put("p_int64", off);
            value.put("p_float32", (float) (off * 1.1));
            value.put("p_float64", off * 1.111111);
            value.put("p_bool", true);
            records.add(new SinkRecord(topic, 0, null, null, null, value, off, System.currentTimeMillis(), TimestampType.CREATE_TIME));
        }
        return records;
    }

    /** p_int64 carries the Kafka offset, so its distinct count is the number of distinct records written. */
    private static long countDistinctOffsets(ClickHouseHelperClient chc, String topic) {
        String query = "SELECT uniqExact(p_int64) FROM " + ClickHouseTestHelpers.buildFromClause(chc, topic)
                + " SETTINGS select_sequential_consistency = 1";
        try (Records records = chc.queryV2(query)) {
            return Long.parseLong(records.iterator().next().getString(1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
