## 1.0.4 2023-11-08
* Bugfix

## 1.0.3 2023-11-07
* Added support for proxy configurations
* Additional test cases
* Additional error codes added to retry list (242, 319, 999)

## 1.0.2 2023-11-02
* Updated dependencies
* Add BYTES type support
* Add Decimal type support
* Additional exception handling
* Retriable Exceptions are now always retried (even when errors.tolerance=all)
* Support for org.apache.kafka.connect.storage.StringConverter

## 1.0.1 2023-09-11
* Added support for `tableRefreshInterval` to re-fetch table changes from ClickHouse

## 1.0.0 2023-08-23
* Additional tests for ExactlyOnce
* Allows customized ClickHouse settings using `clickhouse.settings' property
* Tweaked deduplication behavior to account for dynamic fields
* Added support for `errors.tolerance' and the DLQ

## 0.0.18 2023-07-17
* Support inline schema with org.apache.kafka.connect.data.Timestamp type 
* Support inline schema with org.apache.kafka.connect.data.Time type

## 0.0.17 2023-07-12
* Updating Logo

## 0.0.16 2023-06-13
* Updated state handling so that warnings are posted, rather than errors + exceptions.
* Adding 285 TOO_FEW_LIVE_REPLICAS to the retry list
* Tweaking the code to switch to JSON handling when default values are present. This should be more reliable (if the
data structure is simple enough) while supporting defaults. This is a temporary solution until we can implement a
longer-term fix in core code.

## 0.0.15 2023-05-30
* Added 202 (TOO_MANY_SIMULTANEOUS_QUERIES) Code to retry list [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/109) 
* Added 252 (TOO_MANY_PARTS) to the retry list


## 0.0.14, 2023-05-24
* Fix for LowCardinality(t) and LowCardinality(Nullable(t)) [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/105)

## 0.0.13, 2023-05-08
* Fix null exception when getting empty record

## 0.0.12, 2023-05-05
* Add UNEXPECTED_END_OF_FILE as RetriableException #98

## 0.0.11, 2023-04-20
* Implemented retry mechanism to fix [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/74)
** Some ClickHouse errors (159 - TIMEOUT_EXCEEDED; 164 - READONLY; 203 - NO_FREE_CONNECTION; 209 - SOCKET_TIMEOUT; 210 - NETWORK_ERROR; 425 - SYSTEM_ERROR) as well as SocketTimeoutException and UnknownHostException will result in the connector retrying the operation (based on configuration).
This should help mitigate temporary (but unavoidable) hiccups in network operations, though this list will likely be tweaked over time as well.

## 0.0.10, 2023-04-10
* Nullable date columns fix [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/60)

## 0.0.9, 2023-03-08
### Bug Fixes
* Implicit date conversion [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/57). [#63](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/63)
* Handle null columns [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/62). [#65](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/65)

## 0.0.8, 2023-02-10
### Bug Fixes
* Support nullable values in SchemalessRecordConverter. [#55](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/55)
* Add Emojis tests & fix utf-8 bug. [#52](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/52)
* Add manifest.json. [#44](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/44)

## 0.0.7, 2023-02-01
### Bug Fixes
* Fix index error in describeTable. [#49](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/49)

## 0.0.6, 2023-02-01
### Bug Fixes
* Don't validate JSON records against the table schema. [#47](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/47)

## 0.0.5, 2023-01-31
### New features
* Add support for Amazon MSK in [#46](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/46)
