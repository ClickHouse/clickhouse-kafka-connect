# Docker
Environment Variables (.env) file:
```
BOOTSTRAP_SERVERS=
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISM=PLAIN
SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username='' password='';"
SCHEMA_REGISTRY_URL=https://
SCHEMA_BASIC_AUTH_CREDENTIALS_SOURCE=USER_INFO
SCHEMA_BASIC_AUTH_USER_INFO=""
```
## Quarter-Stack (Cloud Kafka/ClickHouse + Local Connector):
Running the basic docker compose: `docker compose -f ~/kafka-connect.yml up`
```yml
---
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
      CONNECT_REST_ADVERTISED_HOST_NAME: kafka-connect
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
      CLASSPATH: /usr/share/java/monitoring-interceptors/monitoring-interceptors-7.6.0.jar
      CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components,/usr/share/dockershare"
      CONNECT_LOG4J_LOGGERS: org.apache.zookeeper=ERROR,org.I0Itec.zkclient=ERROR,org.reflections=ERROR,com.clickhouse=INFO,com.clickhouse.kafka.connect=TRACE
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_JMX_PORT: 7778
      KAFKA_JMX_OPTS: -javaagent:/usr/share/java/cp-base-new/jmx_prometheus_javaagent-0.18.0.jar=8020:/usr/share/dockershare/jmx-export.yml -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false
```
## Half-Stack (Cloud ClickHouse + Local Kafka)
Running the basic docker compose: `docker compose -f ~/kafka-half.yml up`
```yaml
---
name: 'confluent-fullstack'
services:
  broker:
    image: confluentinc/cp-kafka:latest
    hostname: broker
    container_name: broker
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@broker:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://broker:29092,CONTROLLER://broker:29093,PLAINTEXT_HOST://0.0.0.0:9092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
      # Replace CLUSTER_ID with a unique base64 UUID using "bin/kafka-storage.sh random-uuid" 
      # See https://docs.confluent.io/kafka/operations-tools/kafka-tools.html#kafka-storage-sh
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'

  schema-registry:
    image: confluentinc/cp-schema-registry:latest
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      - broker
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'broker:29092'
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

  connect:
    image: confluentinc/cp-kafka-connect:latest
    hostname: connect
    container_name: connect
    volumes:
      - "~/DockerShare:/usr/share/dockershare/:ro"
    depends_on:
      - broker
      - schema-registry
    ports:
      - "8083:8083"
    environment:
      CONNECT_BOOTSTRAP_SERVERS: 'broker:29092'
      CONNECT_REST_ADVERTISED_HOST_NAME: connect
      CONNECT_GROUP_ID: compose-connect-group
      CONNECT_CONFIG_STORAGE_TOPIC: docker-connect-configs
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_OFFSET_FLUSH_INTERVAL_MS: 10000
      CONNECT_OFFSET_STORAGE_TOPIC: docker-connect-offsets
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_STATUS_STORAGE_TOPIC: docker-connect-status
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      # CLASSPATH required due to CC-2422
      CLASSPATH: /usr/share/java/monitoring-interceptors/monitoring-interceptors-7.6.0.jar
      CONNECT_PRODUCER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor"
      CONNECT_CONSUMER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor"
      CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components,/usr/share/dockershare"
      CONNECT_LOG4J_LOGGERS: org.apache.zookeeper=ERROR,org.I0Itec.zkclient=ERROR,org.reflections=ERROR,com.clickhouse=DEBUG

  control-center:
    image: confluentinc/cp-enterprise-control-center:latest
    hostname: control-center
    container_name: control-center
    depends_on:
      - broker
      - schema-registry
      - connect
      - ksqldb-server
    ports:
      - "9021:9021"
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: 'broker:29092'
      CONTROL_CENTER_CONNECT_CONNECT-DEFAULT_CLUSTER: 'connect:8083'
      CONTROL_CENTER_CONNECT_HEALTHCHECK_ENDPOINT: '/connectors'
      CONTROL_CENTER_KSQL_KSQLDB1_URL: "http://ksqldb-server:8088"
      CONTROL_CENTER_KSQL_KSQLDB1_ADVERTISED_URL: "http://localhost:8088"
      CONTROL_CENTER_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      CONTROL_CENTER_REPLICATION_FACTOR: 1
      CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS: 1
      CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS: 1
      CONFLUENT_METRICS_TOPIC_REPLICATION: 1
      PORT: 9021

  ksqldb-server:
    image: confluentinc/cp-ksqldb-server:latest
    hostname: ksqldb-server
    container_name: ksqldb-server
    depends_on:
      - broker
      - connect
    ports:
      - "8088:8088"
    environment:
      KSQL_CONFIG_DIR: "/etc/ksql"
      KSQL_BOOTSTRAP_SERVERS: "broker:29092"
      KSQL_HOST_NAME: ksqldb-server
      KSQL_LISTENERS: "http://0.0.0.0:8088"
      KSQL_CACHE_MAX_BYTES_BUFFERING: 0
      KSQL_KSQL_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_PRODUCER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor"
      KSQL_CONSUMER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor"
      KSQL_KSQL_CONNECT_URL: "http://connect:8083"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_REPLICATION_FACTOR: 1
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: 'true'
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: 'true'

  rest-proxy:
    image: confluentinc/cp-kafka-rest:latest
    depends_on:
      - broker
      - schema-registry
    ports:
      - 8082:8082
    hostname: rest-proxy
    container_name: rest-proxy
    environment:
      KAFKA_REST_HOST_NAME: rest-proxy
      KAFKA_REST_BOOTSTRAP_SERVERS: 'broker:29092'
      KAFKA_REST_LISTENERS: "http://0.0.0.0:8082"
      KAFKA_REST_SCHEMA_REGISTRY_URL: 'http://schema-registry:8081'

```
## Full-Stack (Local Kafka + ClickHouse)
Running the basic docker compose: `docker compose -f ~/kafka-full.yml up`
```yaml
---
name: 'confluent-fullstack'
services:
  clickhouse:
    image: clickhouse/clickhouse-server
    container_name: clickhouse
    hostname: clickhouse
    volumes:
      - "~/DockerShare:/usr/share/dockershare/:ro"
    ports:
      - "18123:8123"
      - "19000:9000"

  broker:
    image: confluentinc/cp-kafka:latest
    hostname: broker
    container_name: broker
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@broker:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://broker:29092,CONTROLLER://broker:29093,PLAINTEXT_HOST://0.0.0.0:9092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
      # Replace CLUSTER_ID with a unique base64 UUID using "bin/kafka-storage.sh random-uuid" 
      # See https://docs.confluent.io/kafka/operations-tools/kafka-tools.html#kafka-storage-sh
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'

  schema-registry:
    image: confluentinc/cp-schema-registry:latest
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      - broker
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'broker:29092'
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

  connect:
    image: confluentinc/cp-kafka-connect:latest
    hostname: connect
    container_name: connect
    volumes:
      - "~/DockerShare:/usr/share/dockershare/:ro"
    depends_on:
      - broker
      - schema-registry
    ports:
      - "8083:8083"
    environment:
      CONNECT_BOOTSTRAP_SERVERS: 'broker:29092'
      CONNECT_REST_ADVERTISED_HOST_NAME: connect
      CONNECT_GROUP_ID: compose-connect-group
      CONNECT_CONFIG_STORAGE_TOPIC: docker-connect-configs
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_OFFSET_FLUSH_INTERVAL_MS: 10000
      CONNECT_OFFSET_STORAGE_TOPIC: docker-connect-offsets
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_STATUS_STORAGE_TOPIC: docker-connect-status
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      # CLASSPATH required due to CC-2422
      CLASSPATH: /usr/share/java/monitoring-interceptors/monitoring-interceptors-7.6.0.jar
      CONNECT_PRODUCER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor"
      CONNECT_CONSUMER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor"
      CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components,/usr/share/dockershare"
      CONNECT_LOG4J_LOGGERS: org.apache.zookeeper=ERROR,org.I0Itec.zkclient=ERROR,org.reflections=ERROR,com.clickhouse=DEBUG

  control-center:
    image: confluentinc/cp-enterprise-control-center:latest
    hostname: control-center
    container_name: control-center
    depends_on:
      - broker
      - schema-registry
      - connect
      - ksqldb-server
    ports:
      - "9021:9021"
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: 'broker:29092'
      CONTROL_CENTER_CONNECT_CONNECT-DEFAULT_CLUSTER: 'connect:8083'
      CONTROL_CENTER_CONNECT_HEALTHCHECK_ENDPOINT: '/connectors'
      CONTROL_CENTER_KSQL_KSQLDB1_URL: "http://ksqldb-server:8088"
      CONTROL_CENTER_KSQL_KSQLDB1_ADVERTISED_URL: "http://localhost:8088"
      CONTROL_CENTER_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      CONTROL_CENTER_REPLICATION_FACTOR: 1
      CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS: 1
      CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS: 1
      CONFLUENT_METRICS_TOPIC_REPLICATION: 1
      PORT: 9021

  ksqldb-server:
    image: confluentinc/cp-ksqldb-server:latest
    hostname: ksqldb-server
    container_name: ksqldb-server
    depends_on:
      - broker
      - connect
    ports:
      - "8088:8088"
    environment:
      KSQL_CONFIG_DIR: "/etc/ksql"
      KSQL_BOOTSTRAP_SERVERS: "broker:29092"
      KSQL_HOST_NAME: ksqldb-server
      KSQL_LISTENERS: "http://0.0.0.0:8088"
      KSQL_CACHE_MAX_BYTES_BUFFERING: 0
      KSQL_KSQL_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_PRODUCER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor"
      KSQL_CONSUMER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor"
      KSQL_KSQL_CONNECT_URL: "http://connect:8083"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_REPLICATION_FACTOR: 1
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: 'true'
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: 'true'

  rest-proxy:
    image: confluentinc/cp-kafka-rest:latest
    depends_on:
      - broker
      - schema-registry
    ports:
      - 8082:8082
    hostname: rest-proxy
    container_name: rest-proxy
    environment:
      KAFKA_REST_HOST_NAME: rest-proxy
      KAFKA_REST_BOOTSTRAP_SERVERS: 'broker:29092'
      KAFKA_REST_LISTENERS: "http://0.0.0.0:8082"
      KAFKA_REST_SCHEMA_REGISTRY_URL: 'http://schema-registry:8081'

```
# REST API Templates
List Connectors: `GET localhost:8083/connectors?expand=status&expand=info`

Delete Connectors: `DELETE localhost:8083/connectors/clickhouse-connect`

Create "Basic" Connector: `POST localhost:8083/connectors`

```json
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
		"topics": "data_testing",
		"errors.tolerance": "none",
		"errors.deadletterqueue.topic.name": "data_testing_dlq",
		"username": "default",
		"value.converter": "org.apache.kafka.connect.json.JsonConverter",
		"value.converter.schemas.enable": "false",
		"errors.log.enable": "true",
		"clickhouseSettings": "",
		"topic2TableMap": "",
		"jdbcConnectionProperties": "?ssl=true&sslmode=strict",
		"consumer.override.max.poll.records": "50"
	}
}
```
Create "Avro" Connector: `POST localhost:8083/connectors`
```json
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
		"topics": "protobuf_testing",
		"errors.tolerance": "all",
		"errors.deadletterqueue.topic.name": "data_testing_dlq",
		"username": "default",
		"value.converter": "io.confluent.connect.avro.AvroConverter",
		"value.converter.schema.registry.url": "{{Confluent_schema_registry_url}}",
		"value.converter.schemas.enable": "false",
		"value.converter.basic.auth.credentials.source":"USER_INFO",
		"value.converter.basic.auth.user.info":"{{Confluent_schema_user_info}}",
		"errors.log.enable": "true",
		"clickhouseSettings": "",
		"topic2TableMap": "",
		"consumer.override.max.poll.records": "50"
	}
}
```
Create "Protobuf" Connector: `POST localhost:8083/connectors`
```json
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
		"topics": "protobuf_testing",
		"errors.tolerance": "all",
		"errors.deadletterqueue.topic.name": "data_testing_dlq",
		"username": "default",
		"value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
		"value.converter.schema.registry.url": "{{Confluent_schema_registry_url}}",
		"value.converter.schemas.enable": "false",
		"value.converter.basic.auth.credentials.source":"USER_INFO",
		"value.converter.basic.auth.user.info":"{{Confluent_schema_user_info}}",
		"errors.log.enable": "true",
		"clickhouseSettings": "",
		"topic2TableMap": "",
		"consumer.override.max.poll.records": "50"
	}
}
```
# Data + Data Generation
## Loading Data
Inserting specific test data is easy with a few different options. 
### Intellij:
The Kafka plugin is my preferred method: https://www.jetbrains.com/help/idea/big-data-tools-kafka.html
### Confluent UI
Confluent cloud has a UI for creating messages: https://docs.confluent.io/cloud/current/client-apps/topics/messages.html
### kcat
There's also a command-line program called kcat: https://github.com/edenhill/kcat
## Data Generator Source
Repo: https://github.com/ClickHouse/kafka-connect-datagen
Initialize Data Source:
```json
{
	"name": "clickhouse-datagen",
	"config": {
		"connector.class": "com.clickhouse.kafka.connect.datagen.DatagenConnector",
		"tasks.max": "1",
		"topic": "data_testing",
		"max.interval": "1000",
		"iterations": "100",
		"value.converter": "org.apache.kafka.connect.storage.StringConverter"
	}
}
```
Sample data:
```json
{
  "self_reported_start_time": "2024-07-23T05:48:25.333448",
  "event_id": "344ea13b-830f-4462-9ed5-198d62132005",
  "create_time": "2024-07-23T05:48:25.331711",
  "flow_category": "DmLm",
  "route_hash": "J14oGtl6wjecSfgWer4OFjD4CzTa4moUpEEL",
  "self_reported_end_time": "2024-07-23T05:48:25.333457",
  "run": "flows/5fkySsHM4PI8Am6LFfOsHwAcjpw7SO6fYMJxHAo",
  "trigger": "720615b8-4512-41ac-85a4-2c63a47e3137",
  "type": "flow_run",
  "event_time": "2024-07-23T05:48:25.331637",
  "flow": "Events#create_report.bad_report.talking#zuNelckaxxKJPFDZObjJFn"
}
```
# Logs
Repo: https://github.com/ClickHouse/kafka-connect-log-analyzer
## Download the log files from the Kafka Connect instance you want to analyze.
	For your local docker image, you can run: `docker logs -f kafka-connect > ~/Desktop/Logs/connector.log`
## Generate a Query Log using the following:
```sql
SELECT 
  event_time,
  log_comment, 
  query_id, 
  query, 
  type, 
  Settings['insert_deduplication_token'], 
  ProfileEvents['DuplicatedInsertedBlocks'], 
  exception_code, 
  exception
FROM clusterAllReplicas(default, merge('system', '^query_log*')) 
WHERE query ILIKE '%INSERT INTO%'
  AND event_time >= '2024-01-01 00:00:00'--Start Time
  AND event_time <= '2024-12-31 23:59:59'--End Time
```
## Run the tool with either of the following commands:
```shell
java -jar log-analyzer.jar ~/Desktop/Logs
java -jar log-analyzer.jar ~/Desktop/Logs -topic=<topicName> -partition=<partitionNumber> -offset=<offset>
```
Replace `<topicName>`, `<partitionNumber>`, and `<offset>` with the topic, partition, and offset you want to investigate. This will give you a summary of the logs around the specified offset.
	The tool will output a summary of the logs and any areas of interest.
### Sample output snippet:
```shell
[main] WARN com.clickhouse.LogAnalyzer - Exception Batch: Batch{topic='datetime_schema', partition=0, minOffset=26, maxOffset=26, attempts=1, eventualSuccess=false}
[main] WARN com.clickhouse.LogAnalyzer - Attempt QueryId: [1d0c11e6-e9bc-43b3-89bd-6bb6e4b4d4f0]
[main] WARN com.clickhouse.LogAnalyzer - QueryLogEntry: null
```
# JMX
Add this to the docker compose and head to http://localhost:3000/:
```yaml
 prometheus:
   hostname: prometheus
   container_name: prometheus
   image: prom/prometheus
   volumes:
     - "~/DockerShare:/usr/share/dockershare/:ro"
   command:
     - '--config.file=/usr/share/dockershare/prometheus.yml'
   ports:
     - 9090:9090

 grafana:
   image: grafana/grafana
   hostname: grafana
   container_name: grafana
   depends_on:
     - prometheus
   volumes:
     - "~/DockerShare:/usr/share/dockershare/:ro"
   ports:
     - 3000:3000
```
