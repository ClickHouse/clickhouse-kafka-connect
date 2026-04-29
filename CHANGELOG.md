# 1.3.8 (unreleased)

## New Features
* Added `auto.evolve` configuration option for automatic table schema evolution. When enabled, the connector detects new fields in incoming records and issues `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` against ClickHouse. Disabled by default. (https://github.com/ClickHouse/clickhouse-kafka-connect/issues/277)

## Bug Fixes
* Fixed RowBinary serialization for Map columns with Nullable value types. The nullable marker byte was missing when writing map values, causing `CANNOT_READ_ALL_DATA` errors for `Map(K, Nullable(V))` columns.

## Dependencies
* Updated clickhouse-java version from `0.9.4` to `0.9.5`

# 1.3.7, 2026-03-25

## Security
* Upgraded `com.fasterxml.jackson.core` dependencies to version with fix for https://github.com/advisories/GHSA-72hv-8253-57qq (https://github.com/ClickHouse/clickhouse-kafka-connect/pull/690).

# Improvements
* `Gson` replaced with `Jackson` for performance and better maintainability (https://github.com/ClickHouse/clickhouse-kafka-connect/pull/676).

# 1.3.6, 2026-03-18

## New Features
* Added internal record buffering support via `bufferCount` and `bufferFlushTime` configuration options. 
When enabled, records from multiple `poll()` calls are accumulated and flushed as a single large batch, 
reducing the number of insert operations to ClickHouse. Buffering is disabled by default (bufferCount=0) for full backward compatibility. (https://github.com/ClickHouse/clickhouse-kafka-connect/pull/658)

## Improvements
* Report inserted offsets in `preCommit()` method. Previously connector was returning same map that is passed to 
the method. This may lead to missed offsets in a situation of partition rebalance. Feature is turned off 
by default and `reportInsertedOffsets` property should be set to `true` to enable. (https://github.com/ClickHouse/clickhouse-kafka-connect/pull/669)

## Bug Fixes 
* Fixed invalid concurrency handling in `ClickHouseWriter.updateMapping`. Previously flag was set not in atomic way. (https://github.com/ClickHouse/clickhouse-kafka-connect/pull/678) 
* Fixes handling server error "Code 33" after schema is updated. Previously logic did not wait for target table to be updated. 
After the fix logic will use describe of a single table when detects that `updateMapping` is running. (https://github.com/ClickHouse/clickhouse-kafka-connect/pull/680)

# 1.3.5, 2025-10-11

## Improvements
* Added topic metrics to JMX and extended task metrics. Topic metrics have partition granularity - each partition has its own metrics (https://github.com/ClickHouse/clickhouse-kafka-connect/issues/612)
* Improved failure handling when messages should be sent to DLQ. Previous implementation was sending whole batch to DLQ 
when one message failed. Now it sends only failed group of messages to DLQ. If failed because of schema validation then 
logs error message contains field name and schema type. (https://github.com/ClickHouse/clickhouse-kafka-connect/issues/590)
* Added support for writing boolean values to number columns. This is allowed because boolean fits into any number type. (https://github.com/ClickHouse/clickhouse-kafka-connect/issues/633)
## Bug Fixes
* Fixed error message about unhandled complex type in array (https://github.com/ClickHouse/clickhouse-kafka-connect/pull/608)
* Fixed logging in schema validation logic. Message "Got non-root column, but its parent was not found to be updated" 
was logged as error but should be a warning. (https://github.com/ClickHouse/clickhouse-kafka-connect/pull/645)
* Fixed writing Avro timestamp value. (https://github.com/ClickHouse/clickhouse-kafka-connect/issues/599)
* Fixed negative timestamp in logs. (https://github.com/ClickHouse/clickhouse-kafka-connect/issues/614)
## Dependencies
* Updated clickhouse-java version to `0.9.4` (https://github.com/ClickHouse/clickhouse-kafka-connect/pull/629)

# 1.3.4, 2025-10-08
## Bug Fixes
* Added column named that causes error (https://github.com/ClickHouse/clickhouse-kafka-connect/pull/607)
* Updated clickhouse-java version `0.9.2` (https://github.com/ClickHouse/clickhouse-kafka-connect/pull/596)
* Fixed Client Name reported for `query_log` (https://github.com/ClickHouse/clickhouse-kafka-connect/issues/542)
* Fixed writing Avro union of `string` and `bytes` to `String` column (https://github.com/ClickHouse/clickhouse-kafka-connect/issues/572)

# 1.3.3, 2025-08-30
## Bug Fixes
* Fixed writing JSON values to ClickHouse. Previously all JSON Object were written as objects where field value were wrapped with `{ "object": <field value> }`. Now objects stored with original structure. (https://github.com/ClickHouse/clickhouse-kafka-connect/issues/574)

## New Features
* Added support of SimpleAggregateFunction column type. (https://github.com/ClickHouse/clickhouse-kafka-connect/pull/571)

# 1.3.2, 2025-07-04
## Improvements
* Added support of writing JSON as string to a JSON column.
## Dependencies
* Upgraded dependency `org.slf4j:slf4j-reload4j` from `2.0.13` to `2.0.17`
* Upgraded dependency `org.apache.httpcomponents.client5:httpclient5` from `5.4.4` to `5.5`
* Upgraded dependency `com.fasterxml.jackson.core:jackson-annotations` from `2.19.0` to `2.19.1`
* Upgraded dependency `org.projectlombok:lombok` from `1.18.34` to `1.18.38`

# 1.3.1, 2025-05-19
## Improvements
* Ensure resources are auto-closed in exactlyOnce
* Adjust JSON parsing for backslash characters

# 1.3.0, 2025-04-14
## Improvements
* Added additional debug logging around KeeperMap and exactlyOnce

# 1.2.9, 2025-03-25
## Improvements
* Add `ignorePartitionsWhenBatching` flag to adjust batching when not using exactlyOnce
* Add version information to MBean

# 1.2.8, 2025-02-13
## Improvements
* Reduce archive size by removing unnecessary dependencies from the jar
## Bug Fixes
* Fix NPE on V1 client when Clickhouse has table with Array(Nested...) field

# 1.2.7, 2025-02-04
## Improvements
* Bump clickhouse-java to 0.8.0
## Bug Fixes
* Fix for handling of nullable fields in tuples ClickHouse data type

# 1.2.6, 2024-11-28
## Improvements
* Detect if table schema has changed and refresh the schema
* Allow bypassing field cleanup

# 1.2.5, 2024-11-21
## Improvements
* Remove redis state provide since we are using KeeperMap for state storage
* Remove unused avro property from `build.gradle.kts`
* Trim schemaless data to only pass the fields that are in the table
* Allow bypassing the schema validation

# 1.2.4, 2024-11-04
## Improvements
* Adjusting underlying client version to `0.7.0`
* Bugfix for UINT handling

# 1.2.3, 2024-09-25
## Improvements
* Tweaking schema validation to allow for UINT

# 1.2.2, 2024-09-25
## New Features
* Adding a new property `tolerateStateMismatch` to allow for the connector to continue processing even if the state stored in ClickHouse does not match the current offset in Kafka

# 1.2.1, 2024-09-18
## Improvements
* Adding some additional logging details to help debug issues

# 1.2.0, 2024-08-26
## New Features
* Adding a KeyToValue transformation to allow for key to be stored in a separate column in ClickHouse

# 1.1.4, 2024-07-30
## Bug Fixes
* Bugfix to address field value to column name mapping for Tuples

# 1.1.3, 2024-07-22
## Improvements
* Update to java-client 0.6.3
* Bugfix to address commas in the column name of enums

# 1.1.2, 2024-07-18
## New Features
* Adding a "dateTimeFormat" configuration option to allow for custom date/time formatting with String schema values
* Adding ephemeral column support and adding an error message

# 1.1.1, 2024-06-27
## Bug Fixes
* Bugfix to address string encoding issue
* Bugfix to address issue with nested types and flatten_nested setting conflict
* Bugfix to avoid storing keeper state in same column name if virtual topic is enabled
* Updated java-client to 0.6.1
* Bugfix to let create missing KeeperMap entries if there are some records present already
* Added a flag to allow bypassing RowBinary and RowBinaryWithDefaults format for schema insertions
* Bugfix to remove erroneous error messages about complex type handling

# 1.1.0, 2024-05-22
## New Features
* Updated java-client to 0.6.0-patch4
* Added config 'keeperOnCluster' to help self-hosted use exactly-once
* Added support for Tuple type
* Added support for Variant type
* Added support for Nested type
* Refactored Column class so that we use Builder pattern using Lombok
* Refactored recursive Map type parsing to iterative approach using describe_include_subcolumns=1
* Adjusted logging to reduce volume while keeping important information
* Adjusted tests to be more reliable and self-cleaning

# 1.0.17, 2024-04-10
## New Features
* Added support for ClickHouse Enum type #370
* Added extra break down of time measurement for insert operations 

# 1.0.16, 2024-04-02
## Improvements
* Removed 1002 from the retriable exceptions list as it's a catch-all for all other exceptions
* Change Java Client to 0.6.0-patch3 and switch tests to use same Base class

# 1.0.15, 2024-03-20
## Improvements
* Added code 107 to the exception list and added more DLQ logging
* Added ExtractTopic transform to our source so that it would work on MSK

# 1.0.14, 2024-03-03
## Bug Fixes
* Fixed print vs log bug
* Fixed bug in overlap logic, added more tests to support this
* Fixed null-pointer bug in stop() method
* Updated dependencies

# 1.0.13, 2024-02-26
## Bug Fixes
* Fix missing jdbcConnectionProperties setup in ClickHouseWriter.getMutationRequest

# 1.0.12, 2024-02-21
## New Features
* Added support for multiple databases in single stream using a virtual topic  #41
* Add support for configuring JDBC properties in connection URL (i.e. `?auto_discovery=true`)
* Added minimum version check for ClickHouse (currently `23.3`)
* Added support for fixed_string type

# 1.0.11, 2024-01-30
## New Features
* Added support for RowBinaryWithDefaults
* Updated dependencies
* Adjusting default values for some settings (like insert_quorum)
* Added string support for DateTime64

# 1.0.10, 2023-12-11
## Bug Fixes
* Fixed writing into nullable Decimal column by @mlivirov in #276
* Adjusting the deduplication token hash by @Paultagoras in #280
* Fixed exception for when an array type column gets null value by @mlivirov in #279
* Support for data ingestion into Array(DateTime64(3)) type columns by @alexshanabrook in #260

# 1.0.9, 2023-12-06
## Improvements
* Added more logging to help debug future issues
* Restored send_progress_in_http_headers flag

# 1.0.8, 2023-12-04
## Improvements
* Remove send_progress_in_http_headers flag as it conflicts
* Updated tests
* Updated dependencies

# 1.0.7, 2023-12-02
## Bug Fixes
* Fix Handle ClickHouse JDBC Client Timeout Exception by @ygrzjh in #252
* Make sure an HTTP request is successfully processed by ClickHouse 

# 1.0.6, 2023-11-22
## New Features
* Added additional logging to help debug future issues

# 1.0.5, 2023-11-13
## New Features
* Added 'zkPath' and 'zkDatabase' properties to customize exactly-once state storage

# 1.0.4, 2023-11-09
* Bugfix

# 1.0.3, 2023-11-07
## New Features
* Added support for proxy configurations
* Additional test cases
* Additional error codes added to retry list (242, 319, 999)

# 1.0.2, 2023-11-02
## New Features
* Updated dependencies
* Add BYTES type support
* Add Decimal type support
* Additional exception handling
* Retriable Exceptions are now always retried (even when errors.tolerance=all)
* Support for org.apache.kafka.connect.storage.StringConverter

# 1.0.1, 2023-09-13
## New Features
* Added support for `tableRefreshInterval` to re-fetch table changes from ClickHouse

# 1.0.0, 2023-09-07
## New Features
* Additional tests for ExactlyOnce
* Allows customized ClickHouse settings using `clickhouseSettings` property
* Tweaked deduplication behavior to account for dynamic fields
* Added support for `errors.tolerance` and the DLQ

# 0.0.18, 2023-07-18
## New Features
* Support inline schema with org.apache.kafka.connect.data.Timestamp type 
* Support inline schema with org.apache.kafka.connect.data.Time type

# 0.0.17, 2023-07-13
* Updating Logo

# 0.0.16, 2023-06-21
## Improvements
* Updated state handling so that warnings are posted, rather than errors + exceptions.
* Adding 285 TOO_FEW_LIVE_REPLICAS to the retry list
* Tweaking the code to switch to JSON handling when default values are present. This should be more reliable (if the
data structure is simple enough) while supporting defaults. This is a temporary solution until we can implement a
longer-term fix in core code.

# 0.0.15, 2023-06-02
## Bug Fixes
* Added 202 (TOO_MANY_SIMULTANEOUS_QUERIES) Code to retry list [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/109) 
* Added 252 (TOO_MANY_PARTS) to the retry list

# 0.0.14, 2023-05-26
## Bug Fixes
* Fix for LowCardinality(t) and LowCardinality(Nullable(t)) [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/105)

# 0.0.13, 2023-05-08
## Bug Fixes
* Fix null exception when getting empty record

# 0.0.12, 2023-05-05
## Bug Fixes
* Add UNEXPECTED_END_OF_FILE as RetriableException #98

# 0.0.11, 2023-04-20
## Improvements
* Implemented retry mechanism to fix [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/74)
** Some ClickHouse errors (159 - TIMEOUT_EXCEEDED; 164 - READONLY; 203 - NO_FREE_CONNECTION; 209 - SOCKET_TIMEOUT; 210 - NETWORK_ERROR; 425 - SYSTEM_ERROR) as well as SocketTimeoutException and UnknownHostException will result in the connector retrying the operation (based on configuration).
This should help mitigate temporary (but unavoidable) hiccups in network operations, though this list will likely be tweaked over time as well.

# 0.0.10, 2023-04-10
## Bug Fixes
* Nullable date columns fix [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/60)

# 0.0.9, 2023-03-12
## Bug Fixes
* Implicit date conversion [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/57). [#63](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/63)
* Handle null columns [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/62). [#65](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/65)

# 0.0.8, 2023-02-10
## Bug Fixes
* Support nullable values in SchemalessRecordConverter. [#55](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/55)
* Add Emojis tests & fix utf-8 bug. [#52](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/52)
* Add manifest.json. [#44](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/44)

# 0.0.7, 2023-02-01
## Bug Fixes
* Fix index error in describeTable. [#49](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/49)

# 0.0.6, 2023-01-31
## Bug Fixes
* Don't validate JSON records against the table schema. [#47](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/47)

# 0.0.5, 2023-01-31
## New features
* Add support for Amazon MSK in [#46](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/46)
