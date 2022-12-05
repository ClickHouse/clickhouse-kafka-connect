# ClickHouse Kafka Connect Sink
**The connector is available in alpha stage for early adopters. Please do not use it in production.**

clickhouse-kafka-connect is the official Kafka Connect sink connector for [ClickHouse](https://clickhouse.com/).

## Glossary
**Apache Kafka** is a distributed event store and stream-processing platform. [Apache Kafka documentation](https://kafka.apache.org/documentation/)

**Kafka Connect** is a streaming integration framework to import/export data from/to other systems.
[Kafka Connect. The official Kafka documentation.](https://kafka.apache.org/documentation/#connect)
[Confluent platform. Kafka Connect.](https://docs.confluent.io/platform/current/connect/index.html)

**ClickHouse Kafka Connect Sink** is the Kafka connector delivering data from a Kafka topic to a ClickHouse table.
[ClickHouse Kafka Connect Sink GitHub repository](https://github.com/ClickHouse/clickhouse-kafka-connect)

## Requirements for the environment
The [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) framework v3.0 or later should be installed in the environment.

## Version compatibility matrix
| ClickHouse Kafka Connect version | ClickHouse version | Kafka Connect |
| ------------- | ------------- | ------------- |
| 0.0.1-0.0.3   | 22.5 or later | 3.0 or later  |

## Main Features
- Shipped with out-of-box exactly-once semantics. It's powered by a new ClickHouse core feature named KeeperMap (used as a state store by the connector) and allows for minimalistic architecture.
- Support for 3rd party state stores: Currently defaults to In-memory but can use KeeperMap (Redis to be added soon).
- Core integration: Built, maintained, and supported by ClickHouse
- Tested continuously against [ClickHouse Cloud](https://clickhouse.com/cloud)
- Data inserts with a declared schema and schemaless
- Support for most major data types of ClickHouse (more to be added soon).

## Installation instructions
The connector is distributed as a single uber JAR file containing all the class files necessary to run the plugin.

To install the plugin, follow these steps:
1. Download a zip archive containing the Connector JAR file from the [Releases](https://github.com/ClickHouse/clickhouse-kafka-connect/releases) page of ClickHouse Kafka Connect Sink repository.
1. Extract the ZIP file content and copy it to the desired location.
1. Add a path with the plugin director to [plugin.path](https://kafka.apache.org/documentation/#connectconfigs_plugin.path) configuration in your Connect properties file to allow Confluent Platform to find the plugin.
1. Restart the Confluent Platform.
1. If you use Confluent Platform, log into Confluent Control Center UI to verify the ClickHouse Sink is available in the list of available connectors.

## Configuration options
To connect the ClickHouse Sink to the ClickHouse server, you need to provide
- connection details: hostname and port(optional)
- user credentials: password and username(optional)

The full table of configuration options:
| Name | Required | Type | Description | Default value |
| ---- | -------- | ---- | ----------- | ------- |
| hostname | required | string | The hostname or IP address of the server| N/A |
| port | optional | integer | Port the server listens to | 8443 |
| username | optional | string | The name of the user on whose behalf to connect to the server | default |
| password | required | string | Password for the specified user | N/A |
| database | optional | string | The name of the database to write to | default |
| ssl | optional | boolean | Enable TLS for network connections | true |
| exactlyOnce | optional | boolean | Enable exactly-once processing guarantees.<br/>When **true**, stores processing state in KeeperMap.<br/>When **false**, stores processing state in-memory.  | false |
| timeoutSeconds | optional | integer | Connection timeout in seconds. | 30 |
| retryCount | optional | integer | Maximum number of retries for a query. No delay between retries. | 3 |


## Target Tables
ClickHouse Connect Sink reads messages from Kafka topics and writes them to appropriate tables. ClickHouse Connect Sink writes data into existing tables. Please, make sure a target table with an appropriate schema was created in ClickHouse before starting to insert data into it.

Each topic requires a dedicated target table in ClickHouse. The target table name must match the source topic name. 

## Pre-processing
If you need to transform outbound messages before they are sent to ClickHouse Kafka Connect
Sink, use [Kafka Connect Transformations](https://docs.confluent.io/platform/current/connect/transforms/overview.html).


## Supported Data types
With a schema declared:
| Kafka Connect Type | ClickHouse Type          | Supported | Primitive |
| ------------------ | ------------------------ | --------- | --------- |
| STRING             | String                   | ✅        | Yes       |
| INT8               | Int8                     | ✅        | Yes       |
| INT16              | Int16                    | ✅        | Yes       |
| INT32              | Int32                    | ✅        | Yes       |
| INT64              | Int64                    | ✅        | Yes       |
| FLOAT32            | Float32                  | ✅        | Yes       |
| FLOAT64            | Float64                  | ✅        | Yes       |
| BOOLEAN            | Boolean                  | ✅        | Yes       |
| ARRAY              | Array(Primitive)         | ✅        | No        |
| MAP                | Map(Primitive, Primitive)| ✅        | No        |
| STRUCT             | N/A                      | ❌        | No        |
| BYTES              | N/A                      | ❌        | No        |

Without a schema declared:
A Map without a schema declared, will be converted to a JSON object and inserted in ClickHouse as [JSON Data Type](https://clickhouse.com/docs/en/sql-reference/data-types/json).

## Logging
Logging is automatically provided by Kafka Connect Platform.
The logging destination and format might be configured via Kafka connect [configuration file](https://docs.confluent.io/platform/current/connect/logging.html#log4j-properties-file).

*Hint*: If using the Confluent Platform, the logs can be seen by running a CLI command:
> confluent local services connect log

For additional details check out the official [tutorial](https://docs.confluent.io/platform/current/connect/logging.html).

## Monitoring

ClickHouse Kafka Connect reports runtime metrics via [Java Management Extensions (JMX)](https://www.oracle.com/technical-resources/articles/javase/jmx.html). JMX is enabled in Kafka Connector by default.

ClickHouse Connect MBeanName:
> com.clickhouse:type=ClickHouseKafkaConnector,name=SinkTask{id}

ClickHouse Kafka Connect reports the following metrics:
| Name                 | Type | Description              |
| -------------------- | ---- | ------------------------ |
| receivedRecords      | long | The total number of records received. |
| recordProcessingTime | long | Total time in nanoseconds spent grouping and converting records to a unified structure. |
| taskProcessingTime   | long | Total time in nanoseconds spent processing and inserting data into ClickHouse. |
## Limitations
- Deletes aren’t supported.
- Batch size is inherited from the Kafka Consumer properties.
- When using KeeperMap for exactly-once and the offset is changed or rewinded, please delete the content from KeeperMap for that specific topic.
