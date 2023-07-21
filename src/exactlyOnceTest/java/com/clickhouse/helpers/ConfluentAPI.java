package com.clickhouse.helpers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Base64;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ConfluentAPI {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfluentAPI.class);
    private final Properties properties;

    public ConfluentAPI(Properties properties) {
        this.properties = properties;
    }

    public boolean createTopic(String topicName, int partitions, int replicationFactor, boolean compact, boolean validateOnly) throws IOException, InterruptedException, URISyntaxException {
        LOGGER.info("Creating topic {} with {} partitions and replication factor {}", topicName, partitions, replicationFactor);

        String restURL = "https://"+properties.getProperty("rest.servers")+"/kafka/v3/clusters/"+properties.getProperty("cluster.id")+"/topics";
        String basicAuthCreds = Base64.getEncoder().encodeToString(
                (properties.getProperty("rest.authentication.user") + ":" + properties.getProperty("rest.authentication.pass")).getBytes());
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(restURL))
                .header("Content-Type", "application/json;charset=UTF-8")
                .header("Authorization", "Basic " + basicAuthCreds)
                .POST(HttpRequest.BodyPublishers.ofString("{\n" +
                        "    \"topic_name\": \"" + topicName + "\",\n" +
                        "    \"partitions_count\": " + partitions + ",\n" +
                        "    \"replication_factor\": " + replicationFactor + ",\n" +
                        "    \"validate_only\": " + validateOnly + ",\n" +
                        "    \"configs\": [{\n" +
                        "       \"name\": \"cleanup.policy\",\n" +
                        "       \"value\": \"" + (compact ? "compact" : "delete") +"\"\n" +
                        "    }]" +
                        "}"))
                .build();
        HttpResponse<String> response = HttpClient.newBuilder().proxy(ProxySelector.getDefault()).build().send(request, HttpResponse.BodyHandlers.ofString());

        LOGGER.info(String.valueOf(response.statusCode()));
        LOGGER.info(response.body());

        return response.statusCode() == 200 || response.statusCode() == 201;
    }

    public boolean deleteTopic(String topicName) throws IOException, InterruptedException, URISyntaxException {
        LOGGER.info("Deleting topic {}", topicName);

        String restURL = "https://"+properties.getProperty("rest.servers")+"/kafka/v3/clusters/"+properties.getProperty("cluster.id")+"/topics/" + topicName;
        String basicAuthCreds = Base64.getEncoder().encodeToString(
                (properties.getProperty("rest.authentication.user") + ":" + properties.getProperty("rest.authentication.pass")).getBytes());
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(restURL))
                .header("Authorization", "Basic " + basicAuthCreds)
                .DELETE()
                .build();
        HttpResponse<String> response = HttpClient.newBuilder().proxy(ProxySelector.getDefault()).build().send(request, HttpResponse.BodyHandlers.ofString());
        LOGGER.info(String.valueOf(response.statusCode()));

        return response.statusCode() == 204;
    }
}
