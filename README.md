# ClickHouse Kafka Connect Sink
**The connector is available in alpha stage for early adopters. Please do not use it in production.**

clickhouse-kafka-connect is the official Kafka Connect sink connector for [ClickHouse](https://clickhouse.com/).

## Main Features
- Shipped with out of the box exactly-once semantics. This is powered by a new ClickHouse core feature named KeeperMap (used a state store by the connector) and allows to keep the architecture minimalistic.
- Support for 3rd party state stores: Currently defaults to In-memory but can use KeeperMap (Redis to be added soon).
- Core integration: Built, maintained and supported by ClickHouse
- Tested continuously against [ClickHouse Cloud](https://clickhouse.com/cloud)
- Data inserts with a declared schema
- Support for most major datatypes of ClickHouse (more to be added soon)

## Setup
- Download the latest release or run `./gradlew` createConfluentArchive to create your own archive
- Copy & Unzip the Archive Of ClickHouse Sink to Connect Plugin directory (Can be found at `/usr/share/java,/usr/share/confluent-hub-components`) (`clickhouse-kafka-connect-0.0.1.zip`)
- Restart connect worker to load ClickHouse Sink
- Open Control Center
- Add ClickHouseSink & Configure

## Current Limitations 
- Schemaless inserts are not supported yet
- Batch size is inherited from the Kafka Consumer properties
- Destination table name needs to match the source topic name
- Destination ClickHouse table needs to be already created in ClickHouse
