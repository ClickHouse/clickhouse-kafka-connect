package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class ClickHouseReplicaPinningMockServerTest {

    private HttpServer server;
    private int port;
    private final List<RecordedRequest> recordedRequests = new CopyOnWriteArrayList<>();

    public static class RecordedRequest {
        private final String path;
        private final String method;
        private final Map<String, List<String>> headers;
        private final String body;

        public RecordedRequest(String path, String method, Map<String, List<String>> headers, String body) {
            this.path = path;
            this.method = method;
            this.headers = headers;
            this.body = body;
        }

        public String getPath() {
            return path;
        }

        public String getMethod() {
            return method;
        }

        public Map<String, List<String>> getHeaders() {
            return headers;
        }

        public String getBody() {
            return body;
        }

        public String getHeader(String name) {
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(name)) {
                    List<String> values = entry.getValue();
                    return (values != null && !values.isEmpty()) ? values.get(0) : null;
                }
            }
            return null;
        }
    }

    @BeforeEach
    public void startServer() throws IOException {
        recordedRequests.clear();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        port = server.getAddress().getPort();
        server.createContext("/", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                InputStream is = exchange.getRequestBody();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] buf = new byte[1024];
                int n;
                while ((n = is.read(buf)) != -1) {
                    baos.write(buf, 0, n);
                }
                String body = baos.toString("UTF-8");
                recordedRequests.add(new RecordedRequest(
                        exchange.getRequestURI().toString(),
                        exchange.getRequestMethod(),
                        new HashMap<>(exchange.getRequestHeaders()),
                        body
                ));

                byte[] responseBytes = "Ok.\n".getBytes("UTF-8");
                if (body.contains("DESCRIBE TABLE") || exchange.getRequestURI().toString().contains("DESCRIBE")) {
                    responseBytes = "{\"name\":\"id\",\"type\":\"Int32\",\"default_type\":\"\",\"default_expression\":\"\",\"comment\":\"\",\"is_subcolumn\":false}\n".getBytes("UTF-8");
                } else if (body.contains("SELECT version()") || exchange.getRequestURI().toString().contains("version")) {
                    responseBytes = "24.3.1.1\n".getBytes("UTF-8");
                }

                exchange.sendResponseHeaders(200, responseBytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(responseBytes);
                }
            }
        });
        server.start();
    }

    @AfterEach
    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testReplicaTaggingFlowClientV1() {
        ClickHouseHelperClient chc = new ClickHouseHelperClient.ClickHouseClientBuilder("127.0.0.1", port, ClickHouseProxyType.IGNORE, null, -1)
                .setDatabase("default")
                .useClientV2(false)
                .setJdbcConnectionProperties("compress=0&decompress=0")
                .enableReplicaPinning(true)
                .build();

        // 1. Initial describe call before pinning: tag header should NOT be present
        recordedRequests.clear();
        Table table1 = chc.describeTable("default", "test_table");
        Assertions.assertNotNull(table1);
        Assertions.assertFalse(recordedRequests.isEmpty());
        RecordedRequest initialReq = recordedRequests.get(recordedRequests.size() - 1);
        Assertions.assertNull(initialReq.getHeader(ClickHouseHelperClient.REPLICA_TAG_HEADER));

        // 2. Failure occurs -> pinReplica() is called
        chc.pinReplica();

        // 3. Retry describe call while pinned: tag header SHOULD be present
        recordedRequests.clear();
        Table table2 = chc.describeTable("default", "test_table");
        Assertions.assertNotNull(table2);
        Assertions.assertFalse(recordedRequests.isEmpty());
        RecordedRequest pinnedReq1 = recordedRequests.get(recordedRequests.size() - 1);
        String replicaTag = pinnedReq1.getHeader(ClickHouseHelperClient.REPLICA_TAG_HEADER);
        Assertions.assertNotNull(replicaTag);

        // Subsequent call while still pinned should reuse the same tag
        recordedRequests.clear();
        Table table3 = chc.describeTable("default", "test_table");
        Assertions.assertNotNull(table3);
        Assertions.assertFalse(recordedRequests.isEmpty());
        RecordedRequest pinnedReq2 = recordedRequests.get(recordedRequests.size() - 1);
        Assertions.assertEquals(replicaTag, pinnedReq2.getHeader(ClickHouseHelperClient.REPLICA_TAG_HEADER));

        // 4. Retry finishes -> unpinReplica() is called
        chc.unpinReplica();

        // 5. Subsequent call after unpinning: tag header should NOT be present
        recordedRequests.clear();
        Table table4 = chc.describeTable("default", "test_table");
        Assertions.assertNotNull(table4);
        Assertions.assertFalse(recordedRequests.isEmpty());
        RecordedRequest unpinnedReq = recordedRequests.get(recordedRequests.size() - 1);
        Assertions.assertNull(unpinnedReq.getHeader(ClickHouseHelperClient.REPLICA_TAG_HEADER));
    }

    @Test
    public void testReplicaTaggingFlowClientV2() {
        ClickHouseHelperClient chc = new ClickHouseHelperClient.ClickHouseClientBuilder("127.0.0.1", port, ClickHouseProxyType.IGNORE, null, -1)
                .setDatabase("default")
                .useClientV2(true)
                .setJdbcConnectionProperties("compress=false&decompress=false")
                .enableReplicaPinning(true)
                .build();

        // Initial call without pinning
        recordedRequests.clear();
        Table table1 = chc.describeTable("default", "test_table");
        Assertions.assertNotNull(table1);
        Assertions.assertFalse(recordedRequests.isEmpty());
        RecordedRequest initialReq = recordedRequests.get(recordedRequests.size() - 1);
        Assertions.assertNull(initialReq.getHeader(ClickHouseHelperClient.REPLICA_TAG_HEADER));

        // Pin replica on failure
        chc.pinReplica();

        // Pinned call
        recordedRequests.clear();
        Table table2 = chc.describeTable("default", "test_table");
        Assertions.assertNotNull(table2);
        Assertions.assertFalse(recordedRequests.isEmpty());
        RecordedRequest pinnedReq = recordedRequests.get(recordedRequests.size() - 1);
        String tag = pinnedReq.getHeader(ClickHouseHelperClient.REPLICA_TAG_HEADER);
        Assertions.assertNotNull(tag, "Headers received: " + pinnedReq.getHeaders());

        // Unpin replica after retry
        chc.unpinReplica();

        // Unpinned call
        recordedRequests.clear();
        Table table3 = chc.describeTable("default", "test_table");
        Assertions.assertNotNull(table3);
        Assertions.assertFalse(recordedRequests.isEmpty());
        RecordedRequest unpinnedReq = recordedRequests.get(recordedRequests.size() - 1);
        Assertions.assertNull(unpinnedReq.getHeader(ClickHouseHelperClient.REPLICA_TAG_HEADER));
    }

    @Test
    public void testReplicaTaggingDisabledFeatureFlag() {
        ClickHouseHelperClient chc = new ClickHouseHelperClient.ClickHouseClientBuilder("127.0.0.1", port, ClickHouseProxyType.IGNORE, null, -1)
                .setDatabase("default")
                .useClientV2(false)
                .setJdbcConnectionProperties("compress=0&decompress=0")
                .enableReplicaPinning(false)
                .build();

        chc.pinReplica();

        recordedRequests.clear();
        Table table = chc.describeTable("default", "test_table");
        Assertions.assertNotNull(table);
        Assertions.assertFalse(recordedRequests.isEmpty());
        RecordedRequest req = recordedRequests.get(recordedRequests.size() - 1);
        Assertions.assertNull(req.getHeader(ClickHouseHelperClient.REPLICA_TAG_HEADER));
    }
}
