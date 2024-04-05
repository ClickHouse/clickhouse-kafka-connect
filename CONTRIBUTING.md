# Getting Started
So you want to get started developing with our Kafka Connect Sink Connector, eh? Well welcome aboard!

## Pre-Requisites
* [OpenJDK 17](https://aws.amazon.com/corretto/) or some other equivalent for compiling the connector code
* [Docker Desktop](https://docs.docker.com/engine/install/) for running tests
* [Github Desktop](https://desktop.github.com/) or some other equivalent git to download the repo
* A local copy of the repo, pulled from git

## Building and Running Unit Tests
You should be able to compile + run the unit tests locally by going to the root project folder and running `./gradlew clean test`. Note this doesn't produce a release artifact, you'll have to execute a later step for that.

## Generating the Build Artifact
To create the actual jar we need, run `./gradlew createConfluentArchive` from the project root. That should output a zip file into `/build/confluent/` that you can use to run the connector locally (or upload to a cloud service).

## Using Docker for Local Development
Docker is a great tool for local development! To make things easier, this is a docker compose file (named docker-compose.yml in later sections) that can be tweaked as needed. We've split off the environment variables into a separate .env file, and we supply the connector details making a simple REST call.


```yaml
---
version: '2'
name: 'confluent-connect'
services:
  connect:
    image: confluentinc/cp-kafka-connect:latest
    hostname: connect
    container_name: kafka-connect
    volumes:
      - "~/DockerShare:/usr/share/dockershare/:ro"
    ports:
      - "8083:8083"
      - "7778:7778"
      - "8020:8020"
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
      CONNECT_SCHEMA_REGISTRY_URL: ${SCHEMA_REGISTRY_URL}
      CONNECT_CONSUMER_SCHEMA_REGISTRY_URL: ${SCHEMA_REGISTRY_URL}
      CONNECT_PRODUCER_SCHEMA_REGISTRY_URL: ${SCHEMA_REGISTRY_URL}
      CONNECT_BASIC_AUTH_CREDENTIALS_SOURCE: ${SCHEMA_BASIC_AUTH_CREDENTIALS_SOURCE}
      CONNECT_CONSUMER_BASIC_AUTH_CREDENTIALS_SOURCE: ${SCHEMA_BASIC_AUTH_CREDENTIALS_SOURCE}
      CONNECT_PRODUCER_BASIC_AUTH_CREDENTIALS_SOURCE: ${SCHEMA_BASIC_AUTH_CREDENTIALS_SOURCE}
      CONNECT_BASIC_AUTH_USER_INFO: ${SCHEMA_BASIC_AUTH_USER_INFO}
      CONNECT_CONSUMER_BASIC_AUTH_USER_INFO: ${SCHEMA_BASIC_AUTH_USER_INFO}
      CONNECT_PRODUCER_BASIC_AUTH_USER_INFO: ${SCHEMA_BASIC_AUTH_USER_INFO}
      CONNECT_CONSUMER_MAX_POLL_RECORDS: 1
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
      # CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: 
      # CLASSPATH required due to CC-2422
      CLASSPATH: /usr/share/java/monitoring-interceptors/monitoring-interceptors-7.3.0.jar
      # CONNECT_PRODUCER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor"
      # CONNECT_CONSUMER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor"
      CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components,/usr/share/dockershare"
      CONNECT_LOG4J_LOGGERS: org.apache.zookeeper=ERROR,org.I0Itec.zkclient=ERROR,org.reflections=ERROR,com.clickhouse=DEBUG
      # KAFKA_JMX_HOSTNAME: localhost
      # KAFKA_JMX_PORT: 7778
      # KAFKA_JMX_OPTS: -javaagent:/usr/share/java/cp-base-new/jmx_prometheus_javaagent-0.18.0.jar=8020:/usr/share/dockershare/jmx-export.yml -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false

```

A sample .env file (should be located in the same directory as the docker-compose.yml file):
```
BOOTSTRAP_SERVERS=[HOST_NAME:PORT_NUMBER]
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISM=PLAIN
SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username='[KAFKA_USERNAME]' password='[KAFKA_PASSWORD]';"
SCHEMA_REGISTRY_URL=[https://HOST_NAME OR http://HOST_NAME]
SCHEMA_BASIC_AUTH_CREDENTIALS_SOURCE=USER_INFO
SCHEMA_BASIC_AUTH_USER_INFO="[SCHEMA_USERNAME]:[SCHEMA_PASSWORD]"
```

A sample REST call you could use to create the connector (POST to `localhost:8083/connectors`). NOTE: This includes postman environment variables, just replace any of the {{...}} variables with your values.
```
{
  "name": "clickhouse-connect",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "tasks.max": "1",
    "database": "{{ClickHouse_database}}",
    "hostname": "{{ClickHouse_hostname}}",
    "password": "{{ClickHouse_password}}",
    "port": "{{ClickHouse_port}}",
    "errors.retry.timeout": "60",
    "exactlyOnce": "false",
    "ssl": "true",
    "topics": "sample-topic",
    "errors.tolerance": "none",
    "username": "{{ClickHouse_username}}",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "errors.log.enable": "true",
    "clickhouseSettings": "",
    "topic2TableMap": "",
    "consumer.override.max.poll.records": "50",
    "transforms": "Metadata",
    "transforms.Metadata.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.Metadata.offset.field": "offset",
    "transforms.Metadata.partition.field": "part",
    "transforms.Metadata.timestamp.field": "kafkaField",
    "transforms.Metadata.topic.field": "topic"
  }
}
```


## Proposing code changes
This is a relatively straightforward process:
* Ensure there's unit test coverage for the changes (and that prior tests work still, of course).
* Update VERSION to the next logical version number
* Add changes to CHANGELOG in a human-readable way
* Submit a PR

## Releasing a new version
There's an action for that!