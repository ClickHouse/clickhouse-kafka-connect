# ClickHouse Kafka Connect Sink
clickhouse-kafka-connect is the official Kafka Connect sink connector for [ClickHouse](https://clickhouse.com/) and is distributed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

## Version Compatibility
| ClickHouse Kafka Connect Version | ClickHouse Version | Kafka Connect | Confluent Platform |
|----------------------------------|--------------------|---------------|--------------------|
| 0.0.19                           | 22.5               | 2.7           | 6.0                |

## Features
* High throughput data ingestion
* Exactly-once delivery semantics
* Built, maintained, and supported by ClickHouse
* 3rd party state store support
* Schema and Schema-less support
* Support for most* ClickHouse data types (see [here](https://clickhouse.com/docs/en/sql-reference/data-types/))

## Configuration Properties
| Property Name                                   | Default Value                                            | Description                                                                                                | Optional? |
|-------------------------------------------------|----------------------------------------------------------|------------------------------------------------------------------------------------------------------------|-----------|
| `hostname`                                      | `localhost`                                              | This is the ClickHouse hostname to connect against                                                         |           |
| `port`                                          | `8443`                                                   | This is the ClickHouse port - default is the SSL value                                                     |           |
| `ssl`                                           | `true`                                                   | Enable ssl connection to ClickHouse                                                                        |           |
| `username`                                      | `default`                                                | ClickHouse database username                                                                               |           |
| `password`                                      | `""`                                                     | ClickHouse database password                                                                               |           |
| `database`                                      | `default`                                                | ClickHouse database name                                                                                   |           |
| `connector.class`                               | `"com.clickhouse.kafka.connect.ClickHouseSinkConnector"` | Connector Class (set and keep as the default)                                                              |           |
| `tasks.max`                                     | `"1"`                                                    | # of Connector Tasks                                                                                       |           |
| `errors.retry.timeout`                          | `"60"`                                                   | ClickHouse JDBC Retry Timeout                                                                              |           |
| `exactlyOnce`                                   | `"false"`                                                | Exactly Once Enabled                                                                                       |           |
| `topics`                                        | `""`                                                     | The Kafka topics to poll - topic names must match table names*                                             |           |
| `value.converter`                               | `"org.apache.kafka.connect.json.JsonConverter"`          | Connector Value Converter                                                                                  |           |
| `value.converter.schemas.enable`                | `"false"`                                                | Connector Value Converter Schema Support                                                                   |           |
| `errors.tolerance`                              | `"none"`                                                 | Connector Error Tolerance                                                                                  | Optional  |
| `errors.deadletterqueue.topic.name`             | `""`                                                     | If set, a DLQ will be used for failed batches                                                              | Optional  |
| `errors.deadletterqueue.context.headers.enable` | `""`                                                     | This adds additional headers for the DLQ                                                                   | Optional  |
| `clickhouseSettings`                            | `""`                                                     | Allows configuration of ClickHouse settings, using a comma seperated list (e.g. "insert_quorum=2, etc...") | Optional  |


## Prerequisites
* Java 11
* Kafka Connect 2.7.0
* ClickHouse 22.5.0.4422 or later
* Confluent Platform 6.0.0 or later (optional)

## Quick Start
### Configuration Recipes
These are some common configuration recipes to get you started quickly - we'll go into more detail on the actual setup/install later in the documentation.
#### Basic Configuration
This is the most basic configuration to get you started - it assumes you're running Kafka Connect in distributed mode and have a ClickHouse server running on localhost:8443 with SSL enabled.
```json
{
  "name": "clickhouse-connect",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "tasks.max": "1",
    "database": "default",
    "errors.retry.timeout": "60",
    "exactlyOnce": "false",
    "hostname": "localhost",
    "password": "[INSERT_PASSWORD_HERE]",
    "port": "8443",
    "ssl": "true",
    "topics": "SAMPLE_TOPIC",
    "username": "default",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "clickhouseSettings": ""
  }
}
```

#### Basic Configuration with Multiple Topics
```json
{
  "name": "clickhouse-connect",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    ...
    "topics": "SAMPLE_TOPIC, ANOTHER_TOPIC, YET_ANOTHER_TOPIC",
    ...
  }
}
```

#### Basic Configuration with DLQ
```json
{
  "name": "clickhouse-connect",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    ...
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "SAMPLE_DLQ_TOPIC",
    "errors.deadletterqueue.context.headers.enable": "true",
  }
}
```

#### Basic Configuration with DLQ and Multiple Topics
```json
{
  "name": "clickhouse-connect",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    ...
    "topics": "SAMPLE_TOPIC, ANOTHER_TOPIC, YET_ANOTHER_TOPIC",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "SAMPLE_DLQ_TOPIC",
    "errors.deadletterqueue.context.headers.enable": "true",
  }
}
```

#### Avro Schema Support
```json
{
  "name": "clickhouse-connect",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    ...
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://localhost:8081",
    "value.converter.schemas.enable": "true",
  }
}
```

#### JSON Schema Support
```json
{
  "name": "clickhouse-connect",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    ...
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
  }
}
```


#### Exactly-Once Configuration
```json
{
  "name": "clickhouse-connect",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "tasks.max": "1",
    "database": "default",
    "errors.retry.timeout": "60",
    "exactlyOnce": "true",
    "hostname": "localhost",
    "password": "[INSERT_PASSWORD_HERE]",
    "port": "8443",
    "ssl": "true",
    "topics": "SAMPLE_TOPIC",
    "username": "default",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "clickhouseSettings": ""
  }
}
```

#### Exactly-Once Configuration with DLQ
```json
{
  "name": "clickhouse-connect",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    ...
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "SAMPLE_DLQ_TOPIC",
    "errors.deadletterqueue.context.headers.enable": "true",
  }
}
```

### Installing the Connector
Running an instance of Kafka Connect is a prerequisite for installing the connector - see the [Kafka Connect documentation](https://docs.confluent.io/platform/current/connect/index.html) - but a simple way for development work is using docker compose.

#### Docker Compose
This version of the docker compose takes the latest version of kafka connect and pulls security details from a .env file in the same directory - it's important to note that the .env file is not included in the repository, and must be created manually.

Also important to note: "~/DockerShare:/usr/share/dockershare/:ro" is the path to the directory containing the clickhouse-kafka-connect folder (unzipped from a [release](https://github.com/ClickHouse/clickhouse-kafka-connect/releases)).
This is mounted to the docker container, and is used to load the connector - your configuration should reflect this.

```yaml
---
version: '2'
name: 'confluent-connect'
services:
  connect:
    image: confluentinc/cp-kafka-connect:latest
    hostname: kafka-connect
    container_name: kafka-connect
    volumes:
      - "~/DockerShare:/usr/share/dockershare/:ro"
    ports:
      - "8083:8083"
    environment:
      CONNECT_BOOTSTRAP_SERVERS: ${BOOTSTRAP_SERVERS}
      CONNECT_SECURITY_PROTOCOL: ${SECURITY_PROTOCOL}
      CONNECT_CONSUMER_SECURITY_PROTOCOL: ${SECURITY_PROTOCOL}
      CONNECT_PRODUCER_SECURITY_PROTOCOL: ${SECURITY_PROTOCOL}
      CONNECT_SASL_MECHANISM: ${SASL_MECHANISM}
      CONNECT_CONSUMER_SASL_MECHANISM: ${SASL_MECHANISM}
      CONNECT_PRODUCER_SASL_MECHANISM: ${SASL_MECHANISM}
      CONNECT_SASL_JAAS_CONFIG: ${SASL_JAAS_CONFIG}
      CONNECT_CONSUMER_SASL_JAAS_CONFIG: ${SASL_JAAS_CONFIG}
      CONNECT_PRODUCER_SASL_JAAS_CONFIG: ${SASL_JAAS_CONFIG}
      CONNECT_CONSUMER_OVERRIDE_MAX_POLL_RECORDS: 1500
      CONNECT_CONSUMER_MAX_POLL_RECORDS: 5000
      CONNECT_REST_ADVERTISED_HOST_NAME: connect
      CONNECT_GROUP_ID: local-connect-group
      CONNECT_CONFIG_STORAGE_TOPIC: docker-connect-configs
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_OFFSET_FLUSH_INTERVAL_MS: 10000
      CONNECT_OFFSET_STORAGE_TOPIC: docker-connect-offsets
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_STATUS_STORAGE_TOPIC: docker-connect-status
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      # CLASSPATH required due to CC-2422
      CLASSPATH: /usr/share/java/monitoring-interceptors/monitoring-interceptors-7.3.0.jar
      # CONNECT_PRODUCER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor"
      # CONNECT_CONSUMER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor"
      CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components,/usr/share/dockershare"
      CONNECT_LOG4J_LOGGERS: org.apache.zookeeper=ERROR,org.I0Itec.zkclient=ERROR,org.reflections=ERROR,com.clickhouse=DEBUG
```

The .env file should contain the following variables:
```properties
BOOTSTRAP_SERVERS=[INSERT_BOOTSTRAP_SERVERS_HERE]
SECURITY_PROTOCOL=[INSERT_SECURITY_PROTOCOL_HERE]
SASL_MECHANISM=[INSERT_SASL_MECHANISM_HERE]
SASL_JAAS_CONFIG=[INSERT_SASL_JAAS_CONFIG_HERE]
```


## Documentation

See the [ClickHouse website](https://clickhouse.com/docs/en/integrations/kafka/clickhouse-kafka-connect-sink) for the full documentation entry.

## Design
For a full overview of the design and how exactly-once delivery semantics are achieved, see the [design document](./docs/DESIGN.md).

## Help
For additional help, please [file an issue in the repository](https://github.com/ClickHouse/clickhouse-kafka-connect/issues) or raise a question in [ClickHouse public Slack](https://clickhouse.com/slack).



