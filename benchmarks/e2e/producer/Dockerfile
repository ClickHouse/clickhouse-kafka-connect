# Benchmark v2 producer image — parquet -> Avro -> Kafka topic.
#
# BUILD CONTEXT: benchmarks/e2e/ (NOT this directory). The Avro schema lives in
# ../schema/hits.avsc, outside this dir, so the context must be one level up:
#
#     docker build -f benchmarks/e2e/producer/Dockerfile \
#                  -t kc-benchmark-producer:latest \
#                  benchmarks/e2e
#
# Small, self-contained. confluent-kafka bundles librdkafka; python:slim keeps
# the image lean enough for an m6i.large node.
FROM python:3.11-slim

WORKDIR /app

# librdkafka wheels are bundled with confluent-kafka, so no apt build tooling
# is needed. Keep the layer minimal.
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The producer and the Avro schema (baked in so the Job needs no shared volume).
COPY producer/producer.py /app/producer.py
COPY schema/hits.avsc /app/hits.avsc

ENV SCHEMA_PATH=/app/hits.avsc
# Config is env-driven (BOOTSTRAP / REGISTRY_URL / TOPIC / PARTITIONS /
# PARQUET_SOURCE). No CMD args needed; the Job sets env.
ENTRYPOINT ["python", "-u", "/app/producer.py"]
