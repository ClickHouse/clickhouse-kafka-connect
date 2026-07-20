# Benchmark 

## About
This is a standalone gradle project of JMH based benchmark for kafka-connector. It is built for performance testing of 
different components of ClickHouse Kafka connector. The test scope can be small (ex. converting specific type) or big
(ex. end-to-end test of record conversion). 

## How to Run

As simple as:
```shell
./gradlew run --args="-b kafka_connector.SimpleBenchmark -i 2 -t 10"
```

This command will run 1 warmup iteration to 15 seconds of `kafka_connector.SimpleBenchmark`. Then it will run 
two (2) iterations for ten (10) seconds each. 

Two files will be created as output. One with `.json` extension is benchmark report that can be visualized with https://jmh.morethan.io/. 
Another file with `.out` extension is standard output of the JMH. 

### Insert benchmark (V1 vs V2)

Use this benchmark to compare insert performance between client `V1` and `V2` for:
- `rowbinary` inserts (schema records)
- `json` inserts (schemaless map records)
- `string` inserts (JSONEachRow payload)

It runs with data sizes: `10000`, `50000`, `100000` rows.

```shell
./gradlew run --args="-b kafka_connector.InsertClientBenchmark -i 3 -t 15"
```

By default, the benchmark starts a temporary ClickHouse container via Testcontainers.
You can point it to an external ClickHouse instance with environment variables:

```shell
CLICKHOUSE_HOST=localhost \
CLICKHOUSE_PORT=8123 \
CLICKHOUSE_USER=default \
CLICKHOUSE_PASSWORD= \
CLICKHOUSE_SSL=false \
./gradlew run --args="-b kafka_connector.InsertClientBenchmark -i 3 -t 15"
```

Optional: override default Testcontainers image with `CLICKHOUSE_IMAGE`.

### Options 

`-b <benchmark class>` - run specific benchmark. 
`-i <number of iterations>` - how meany measurement iterations should be run. 3 - 5 should be enough. Default 3. 
`-t <seconds>`  - how long each measurement iteration should be run. Default 15.


