ARG CONFLUENT_VERSION
FROM confluentinc/cp-server-connect-base:${CONFLUENT_VERSION}

ARG CONNECTOR_VERSION
RUN confluent-hub install --no-prompt clickhouse/clickhouse-kafka-connect:${CONNECTOR_VERSION}