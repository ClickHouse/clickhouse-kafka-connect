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
