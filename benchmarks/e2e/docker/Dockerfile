# Benchmark v2 — Kafka Connect image with the ClickHouse sink plugin baked in.
#
# Base: a pinned Strimzi Kafka Connect image. Strimzi is the deployment substrate
# (plan phase 1 / section 5); the Connect worker discovers plugins by scanning
# directories under KAFKA_CONNECT_PLUGIN_PATH (default /opt/kafka/plugins). We
# unzip the connector's Confluent-archive plugin (a self-contained plugin dir)
# into that path.
#
# The connector plugin zip is produced by the repo's gradle `createConfluentArchive`
# task (build/confluent/clickhouse-kafka-connect-<version>.zip). It contains a
# single top-level directory <name>-<version>/ with lib/, manifest.json, assets/,
# doc/ — exactly the layout Connect's plugin.path loader expects.
#
# Build args (all provenance-carrying; see build-arm.sh which sets them):
#   STRIMZI_IMAGE      full pinned base image ref (also recorded as provenance)
#   PLUGIN_ZIP         build-context-relative path to the plugin zip to bake in
#   ARM               head | pinned
#   GIT_SHA           git sha the plugin was built from
#   CONNECTOR_VERSION connector version (VERSION file / tag) of the plugin
#   KAFKA_VERSION     kafka version of the base image (for provenance.json)
#   STRIMZI_VERSION   strimzi version of the base image (for provenance.json)
#   BUILD_TIMESTAMP   ISO-8601 build time

ARG STRIMZI_IMAGE=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0
FROM ${STRIMZI_IMAGE}

# Re-declare in the build stage so it is in scope for provenance.json / LABEL
# below (an ARG before FROM only scopes the FROM line itself).
ARG STRIMZI_IMAGE
ARG PLUGIN_ZIP
ARG ARM=head
ARG GIT_SHA=unknown
ARG CONNECTOR_VERSION=unknown
ARG PLUGIN_SHA256=unknown
ARG KAFKA_VERSION=unknown
ARG STRIMZI_VERSION=unknown
ARG BUILD_TIMESTAMP=unknown

# Strimzi's default plugin path. Kept as an ARG so it tracks the base if it moves.
ARG PLUGIN_PATH=/opt/kafka/plugins

USER root

# Bake the plugin in: unzip the Confluent archive into a dedicated plugin dir.
COPY ${PLUGIN_ZIP} /tmp/clickhouse-connector-plugin.zip
RUN mkdir -p ${PLUGIN_PATH}/clickhouse-kafka-connect \
    && unzip -q /tmp/clickhouse-connector-plugin.zip -d ${PLUGIN_PATH}/clickhouse-kafka-connect \
    && rm -f /tmp/clickhouse-connector-plugin.zip

# Provenance the orchestrator reads into the runtime map (plan section 3, phase 2).
RUN mkdir -p /opt/benchmark && cat > /opt/benchmark/provenance.json <<EOF
{
  "arm": "${ARM}",
  "git_sha": "${GIT_SHA}",
  "connector_version": "${CONNECTOR_VERSION}",
  "plugin_sha256": "${PLUGIN_SHA256}",
  "kafka_version": "${KAFKA_VERSION}",
  "strimzi_version": "${STRIMZI_VERSION}",
  "strimzi_image": "${STRIMZI_IMAGE}",
  "build_timestamp": "${BUILD_TIMESTAMP}"
}
EOF

USER 1001

# OCI labels carrying the same provenance (queryable via `docker inspect` /
# registry metadata without pulling the layers).
LABEL org.opencontainers.image.title="clickhouse-kafka-connect-benchmark" \
      org.opencontainers.image.description="Kafka Connect + ClickHouse sink (Benchmark v2 arm image)" \
      org.opencontainers.image.source="https://github.com/ClickHouse/clickhouse-kafka-connect" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.version="${CONNECTOR_VERSION}" \
      org.opencontainers.image.created="${BUILD_TIMESTAMP}" \
      com.clickhouse.benchmark.arm="${ARM}" \
      com.clickhouse.benchmark.git_sha="${GIT_SHA}" \
      com.clickhouse.benchmark.connector_version="${CONNECTOR_VERSION}" \
      com.clickhouse.benchmark.plugin_sha256="${PLUGIN_SHA256}" \
      com.clickhouse.benchmark.kafka_version="${KAFKA_VERSION}" \
      com.clickhouse.benchmark.strimzi_version="${STRIMZI_VERSION}" \
      com.clickhouse.benchmark.strimzi_image="${STRIMZI_IMAGE}"
