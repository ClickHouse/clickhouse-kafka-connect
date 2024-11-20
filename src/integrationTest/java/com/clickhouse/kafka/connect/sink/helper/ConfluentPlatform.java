package com.clickhouse.kafka.connect.sink.helper;


import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;


public class ConfluentPlatform {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfluentPlatform.class);

    private static final String CONFLUENT_VERSION = "7.5.0";
    private static final DockerImageName KAFKA_REST_IMAGE = DockerImageName.parse(
            "confluentinc/cp-kafka-rest:" + CONFLUENT_VERSION
    );

    private static final DockerImageName ZOOKEEPER_IMAGE = DockerImageName.parse(
            "confluentinc/cp-zookeeper:" + CONFLUENT_VERSION
    );

    private static final DockerImageName CP_SERVER_IMAGE = DockerImageName.parse(
            "confluentinc/cp-server:" + CONFLUENT_VERSION
    );

    private static final DockerImageName CP_SCHEMA_REGISTRY = DockerImageName.parse(
            "confluentinc/cp-schema-registry:" + CONFLUENT_VERSION
    );
    // 0.4.0-6.0.1
    private static final DockerImageName CP_DATA_GEN = DockerImageName.parse(
            "cnfldemos/cp-server-connect-datagen:0.6.2-7.5.0"
    );

    private static final DockerImageName CP_CONTROL_CENTER = DockerImageName.parse(
            "confluentinc/cp-enterprise-control-center:" + CONFLUENT_VERSION
    );

    private static final DockerImageName CP_KSQLDB_SERVER = DockerImageName.parse(
            "confluentinc/cp-ksqldb-server:" + CONFLUENT_VERSION
    );

    private static int CONTROL_CENTER_INTERNAL_PORT = 9021;
    private static int REST_PROXY_INTERNAL_PORT = 8082;
    private static int CONNECT_INTERNAL_PORT = 8083;
    private static int KSQL_INTERNAL_PORT = 8088;
    private String clusterId = null;
    private String controlCenterEndpoint = null;
    private String restProxyEndpoint = null;
    private String connectRestEndPoint = null;
    private String ksqlRestEndPoint = null;

    GenericContainer<?> zookeeper;
    GenericContainer<?> cp_server;
    GenericContainer<?> cp_schema_registry;
    GenericContainer<?> cp_data_gen;
    GenericContainer<?> cp_control_center;
    GenericContainer<?> cp_ksqldb_server;
    GenericContainer<?> rest_proxy;

    public ConfluentPlatform(Network network, List<String> connectorPathList) {
        zookeeper = new GenericContainer<>(ZOOKEEPER_IMAGE)
                .withNetwork(network)
                .withExposedPorts(2181)
                .withNetworkAliases("zookeeper")
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181");

        cp_server = new GenericContainer<>(CP_SERVER_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("broker")
                .withExposedPorts(9092, 9101)
                .dependsOn(zookeeper)
                .withEnv("KAFKA_BROKER_ID", "1")
                .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT")

                .withEnv("KAFKA_ADVERTISED_LISTENERS","PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092")
                .withEnv("KAFKA_METRIC_REPORTERS","io.confluent.metrics.reporter.ConfluentMetricsReporter")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR","1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS","0")
                .withEnv("KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR","1")
                .withEnv("KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR","1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR","1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR","1")
                .withEnv("KAFKA_JMX_PORT","9101")
                .withEnv("KAFKA_JMX_HOSTNAME","localhost")
                .withEnv("KAFKA_CONFLUENT_SCHEMA_REGISTRY_URL","http://schema-registry:8081")
                .withEnv("CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS","broker:29092")
                .withEnv("CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS","1")
                .withEnv("CONFLUENT_METRICS_ENABLE","true")
                .withEnv("CONFLUENT_SUPPORT_CUSTOMER_ID","anonymous'");

        cp_schema_registry = new GenericContainer<>(CP_SCHEMA_REGISTRY)
                .withNetwork(network)
                .withNetworkAliases("schema-registry")
                .withExposedPorts(8081)
                .dependsOn(cp_server)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "broker:29092")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081");

        cp_data_gen = new GenericContainer<>(CP_DATA_GEN)
                .withNetwork(network)
                .withNetworkAliases("connect")
                .withExposedPorts(8083)
                .dependsOn(cp_server, cp_schema_registry)
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", "broker:29092")
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
                .withEnv("CONNECT_GROUP_ID", "compose-connect-group")
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "docker-connect-configs")
                .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_OFFSET_FLUSH_INTERVAL_MS", "10000")
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "docker-connect-offsets")
                .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "docker-connect-status")
                .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
                .withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
                .withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
                .withEnv("CLASSPATH", "/usr/share/java/monitoring-interceptors/monitoring-interceptors-7.2.1.jar")
                .withEnv("CONNECT_PRODUCER_INTERCEPTOR_CLASSES", "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor")
                .withEnv("CONNECT_CONSUMER_INTERCEPTOR_CLASSES", "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor")
                .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/confluent-hub-components")
                .withEnv("CONNECT_LOG4J_LOGGERS", "org.apache.zookeeper=ERROR,org.I0Itec.zkclient=ERROR,org.reflections=ERROR,com.clickhouse=DEBUG")
                .waitingFor(Wait.forHttp("/connectors").forStatusCode(200));

        if (connectorPathList != null) {
            for(String connectorPath : connectorPathList) {
                cp_data_gen
                        .withCopyToContainer(MountableFile.forHostPath(connectorPath),"/usr/share/confluent-hub-components/");
            }
        }

        cp_control_center = new GenericContainer<>(CP_CONTROL_CENTER)
                .withNetwork(network)
                .withNetworkAliases("control-center")
                .withExposedPorts(9021)
                .dependsOn(cp_server, cp_schema_registry)

                .withEnv("CONTROL_CENTER_BOOTSTRAP_SERVERS", "broker:29092")
                .withEnv("CONTROL_CENTER_CONNECT_CONNECT-DEFAULT_CLUSTER", "connect:8083")
                .withEnv("CONTROL_CENTER_KSQL_KSQLDB1_URL", "http://ksqldb-server:8088")
                .withEnv("CONTROL_CENTER_KSQL_KSQLDB1_ADVERTISED_URL", "http://localhost:8088")
                .withEnv("CONTROL_CENTER_SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
                .withEnv("CONTROL_CENTER_REPLICATION_FACTOR", "1")
                .withEnv("CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS", "1")
                .withEnv("CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS", "1")
                .withEnv("CONFLUENT_METRICS_TOPIC_REPLICATION", "1")
                .withEnv("PORT", "9021");

        cp_ksqldb_server = new GenericContainer<>(CP_KSQLDB_SERVER)
                .withNetwork(network)
                .withNetworkAliases("ksqldb-server")
                .withExposedPorts(8088)
                .dependsOn(cp_server, cp_data_gen)

                .withEnv("KSQL_CONFIG_DIR", "/etc/ksql")
                .withEnv("KSQL_BOOTSTRAP_SERVERS", "broker:29092")
                .withEnv("KSQL_HOST_NAME", "ksqldb-server")
                .withEnv("KSQL_LISTENERS", "http://0.0.0.0:8088")
                .withEnv("KSQL_CACHE_MAX_BYTES_BUFFERING", "0")
                .withEnv("KSQL_KSQL_SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
                .withEnv("KSQL_PRODUCER_INTERCEPTOR_CLASSES", "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor")
                .withEnv("KSQL_CONSUMER_INTERCEPTOR_CLASSES", "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor")
                .withEnv("KSQL_KSQL_CONNECT_URL", "http://connect:8083")
                .withEnv("KSQL_KSQL_LOGGING_PROCESSING_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE", "true")
                .withEnv("KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE", "true");

        rest_proxy = new GenericContainer<>(KAFKA_REST_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("rest-proxy")
                .withExposedPorts(8082)
                .dependsOn(cp_server, cp_schema_registry, cp_data_gen)

                .withEnv("KAFKA_REST_HOST_NAME", "rest-proxy")
                .withEnv("KAFKA_REST_BOOTSTRAP_SERVERS", "broker:29092")
                .withEnv("KAFKA_REST_LISTENERS", "http://0.0.0.0:8082")
                .withEnv("KAFKA_REST_SCHEMA_REGISTRY_URL", "http://schema-registry:8081");

        zookeeper.start();
        cp_server.start();
        cp_schema_registry.start();

        cp_data_gen.start();
        cp_control_center.start();

        cp_ksqldb_server.start();
        rest_proxy.start();


        System.out.println("Done....");
        this.controlCenterEndpoint = extractControlCenterURL(cp_control_center);
        this.restProxyEndpoint = extractRestProxyURL(rest_proxy);
        this.connectRestEndPoint = extractConnectURL(cp_data_gen);
        this.ksqlRestEndPoint = extractKsqlURL(cp_ksqldb_server);

        System.out.println(getKsqlRestEndPoint());
        this.clusterId = extractClusterId();
    }

    public void close() {
        LOGGER.info("Stopping Confluent Platform...");
        rest_proxy.close();
        cp_ksqldb_server.close();
        cp_control_center.close();
        cp_data_gen.close();
        cp_schema_registry.close();
        cp_server.close();
        zookeeper.close();
    }


    public String getControlCenterEndpoint() {
        return controlCenterEndpoint;
    }

    public String getRestProxyEndpoint() {
        return restProxyEndpoint;
    }

    public String getConnectRestEndPoint() {
        return connectRestEndPoint;
    }
    public String getKsqlRestEndPoint() { return ksqlRestEndPoint; }
    private String extractControlCenterURL(GenericContainer<?> cp_control_center) {
        return String.format("http://%s:%d", cp_control_center.getHost(), cp_control_center.getMappedPort(CONTROL_CENTER_INTERNAL_PORT));
    }
    private String extractRestProxyURL(GenericContainer<?> rest_proxy) {
        return String.format("http://%s:%d", rest_proxy.getHost(), rest_proxy.getMappedPort(REST_PROXY_INTERNAL_PORT));
    }

    private String extractConnectURL(GenericContainer<?> connect) {
        return String.format("http://%s:%d", connect.getHost(), connect.getMappedPort(CONNECT_INTERNAL_PORT));
    }

    private String extractKsqlURL(GenericContainer<?> ksql) {
        return String.format("http://%s:%d", ksql.getHost(), ksql.getMappedPort(KSQL_INTERNAL_PORT));
    }
    private String extractClusterId() {
        String restProxyURL = getRestProxyEndpoint();

        OkHttpClient client = new OkHttpClient();
        String clusterIDurl = String.format("%s/v3/clusters/", restProxyURL);
        Request request = new Request.Builder()
                .url(clusterIDurl)
                .build();
        try (Response response = client.newCall(request).execute()) {
            JSONObject obj = new JSONObject(response.body().string());
            return obj.getJSONArray("data").getJSONObject(0).getString("cluster_id").toString();
        } catch (IOException ioe) {
            return null;
        }

    }

    //    A link for `rest proxy` http api
//    https://docs.confluent.io/platform/current/kafka-rest/api.html
    public String createTopic(String topicName, int partitions) throws IOException {
        String restProxyEndpoint = getRestProxyEndpoint();
        OkHttpClient client = new OkHttpClient();
        String kafkaTopicEndpoint = String.format("%s/v3/clusters/%s/topics", restProxyEndpoint, clusterId);
        MediaType JSON = MediaType.get("application/json; charset=utf-8");
        String json = String.format("{\"topic_name\":\"%s\",\"partitions_count\":%d,\"configs\":[]}", topicName, partitions);
        RequestBody body = RequestBody.create(json, JSON);
        Request request = new Request.Builder()
                .url(kafkaTopicEndpoint)
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            LOGGER.debug("Create topic response code: {}", response.code());
            assert response.body() != null;
            String responseBody = response.body().string();
            LOGGER.debug("Create topic response body: {}", responseBody);
            return responseBody;
        }
    }

    public String deleteTopic(String topicName) throws IOException {
        String restProxyEndpoint = getRestProxyEndpoint();
        OkHttpClient client = new OkHttpClient();
        String kafkaTopicEndpoint = String.format("%s/v3/clusters/%s/topics/%s", restProxyEndpoint, clusterId, topicName);
        Request request = new Request.Builder()
                .url(kafkaTopicEndpoint)
                .delete()
                .build();
        try (Response response = client.newCall(request).execute()) {
            LOGGER.debug("Delete topic response code: {}", response.code());
            assert response.body() != null;
            String responseBody = response.body().string();
            LOGGER.debug("Delete topic response body: {}", responseBody);
            return responseBody;
        }
    }

    public boolean doesTopicExist(String topicName) {
        String restProxyEndpoint = getRestProxyEndpoint();
        OkHttpClient client = new OkHttpClient();
        String kafkaTopicEndpoint = String.format("%s/topics/%s", restProxyEndpoint, topicName);
        Request request = new Request.Builder()
                .url(kafkaTopicEndpoint)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (response.code() == 200)
                return true;
            if (response.code() == 404)
                return false;
        } catch (IOException ioe) {
            return false;
        }
        return false;
    }

    public void createConnect(String payload) throws IOException {
        String connectRestEndpoint = getConnectRestEndPoint();
        OkHttpClient client = new OkHttpClient();
        String connectorsEndpoint = String.format("%s/connectors", connectRestEndpoint);
        MediaType JSON = MediaType.get("application/json");
        RequestBody body = RequestBody.create(payload, JSON);
        Request request = new Request.Builder()
                .url(connectorsEndpoint)
                .addHeader("Content-Type","application/json")
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            LOGGER.info("Create connectors response code: {}", response.code());
            assert response.body() != null;
            String responseBody = response.body().string();
            LOGGER.debug("Create connectors response body: {}", responseBody);
        }
    }



    public void deleteConnectors(String connectorName) throws IOException {
        LOGGER.info("Deleting connector: " + connectorName);
        String connectRestEndpoint = getConnectRestEndPoint();
        OkHttpClient client = new OkHttpClient();
        String connectorsEndpoint = String.format("%s/connectors/%s", connectRestEndpoint, connectorName);
        Request request = new Request.Builder()
                .url(connectorsEndpoint)
                .delete()
                .build();
        try (Response response = client.newCall(request).execute()) {
            LOGGER.info("Delete connectors response code: {}", response.code());
            assert response.body() != null;
            String responseBody = response.body().string();
            LOGGER.debug("Delete connectors response body: {}", responseBody);
        }
    }

    public void printConnectors() {
        LOGGER.info(getConnectors());
    }

    public String getConnectors() {
        String connectRestEndpoint = getConnectRestEndPoint();
        OkHttpClient client = new OkHttpClient();
        String connectorsEndpoint = String.format("%s/connectors?expand=status&expand=info", connectRestEndpoint);
        Request request = new Request.Builder()
                .url(connectorsEndpoint)
                .get()
                .build();
        try (Response response = client.newCall(request).execute()) {
            LOGGER.debug("Get connectors response code: {}", response.code());
            assert response.body() != null;
            String responseBody = response.body().string();
            LOGGER.debug("Get connectors response body: {}", responseBody);
            return responseBody;
        } catch (IOException ioe) {
            return "";
        }
    }

    public void restartConnector(String connectorName) throws InterruptedException, IOException {
        LOGGER.info("Restarting connector: " + connectorName);
        String connectRestEndpoint = getConnectRestEndPoint();
        OkHttpClient client = new OkHttpClient();
        String connectorsEndpoint = String.format("%s/connectors/%s/restart?includeTasks=true", connectRestEndpoint, connectorName);
        Request request = new Request.Builder()
                .url(connectorsEndpoint)
                .post(RequestBody.create("", null))
                .build();
        try (Response response = client.newCall(request).execute()) {
            assert response.body() != null;
            if (response.code() == 200) {
                LOGGER.info("Waiting for connector to finish restarting...");
                int loopCount = 0;
                do {
                    Thread.sleep(2 * 1000);
                    LOGGER.info("Restarting...");
                    loopCount++;
                } while (!isConnectorRunning(connectorName) && loopCount < 30);
            }
        }
    }

    public boolean runKsql(String payload) {
        String ksqlRestEndPoint = getKsqlRestEndPoint();
        OkHttpClient client = new OkHttpClient();
        String ksqlEndpoint = String.format("%s/ksql", ksqlRestEndPoint);
        MediaType JSON = MediaType.get("application/json");
        RequestBody body = RequestBody.create(payload, JSON);
        Request request = new Request.Builder()
                .url(ksqlEndpoint)
                .addHeader("Content-Type","application/json")
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (response.code() == 200)
                return true;
        } catch (IOException ioe) {
            return false;
        }
        return false;

    }

    public long getOffset(String topicName, int partitionId) throws IOException {
        String restProxyEndpoint = getRestProxyEndpoint();
        String kafkaTopicOffsetEndpoint = String.format("%s/topics/%s/partitions/%d/offsets", restProxyEndpoint, topicName, partitionId);
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(kafkaTopicOffsetEndpoint)
                .get()
                .build();
        try (Response response = client.newCall(request).execute()) {
            LOGGER.info("Get offset response code: {}", response.code());
            if (response.code() == 200) {
                JSONObject obj = new JSONObject(response.body().string());
//                long beginningOffset = obj.getLong("beginning_offset");
                return obj.getLong("end_offset");
            }
        }

        return 0;
    }


    public int generateData(String templateFileName, String topicName, int numberOfPartitions, int numberOfRecords) throws IOException, InterruptedException {
        LOGGER.info("Generating data...");
        String connectorName = "DatagenConnector_" + topicName + UUID.randomUUID();
        int totalWorkers = numberOfPartitions * 10;

        String payloadDataGen = String.join("", Files.readAllLines(Paths.get(templateFileName)));
        createConnect(String.format(payloadDataGen, connectorName, connectorName, totalWorkers, topicName, numberOfRecords));
        String currentState = "";
        int loopCount = 0;
        do {
            Thread.sleep(2 * 1000);
            if (loopCount % 4 == 0) {
                LOGGER.info("Generating...");
            }
            currentState = getConnectors();
            LOGGER.debug(currentState);
            loopCount++;
        } while (Pattern.compile("Stopping connector: generated the configured").matcher(currentState).results().count() != totalWorkers && loopCount < 120);//We have to track multiple

        if (loopCount >= 120) {
            LOGGER.error("Data generation failed");
            throw new RuntimeException("Data generation failed");
        }

        deleteConnectors(connectorName);
        Thread.sleep(3 * 1000);//Wait for the connector to be deleted
        long offsetTotal = 0;
        for (int i=0; i < numberOfPartitions; i++) {
            offsetTotal += getOffset(topicName, i);
        }
        int runningTotal = numberOfPartitions * numberOfRecords * 5;
        LOGGER.info("Generated records for [{}]: Total (By Offset): [{}], Diff from Theoretical Total: [{}]", topicName, offsetTotal, runningTotal - offsetTotal);
        return (int) offsetTotal;
    }


    public boolean isConnectorRunning(String connectorName) throws IOException {
        String connectors = getConnectors();
        HashMap<String, Map> map = (new ObjectMapper()).readValue(connectors, HashMap.class);
        Map connector = map.get(connectorName);
        Map status = (Map) connector.get("status");
        Map connectorStatus = (Map) status.get("connector");
        String connectorState = String.valueOf(connectorStatus.get("state"));

        return "RUNNING".equalsIgnoreCase(connectorState);
    }

}
