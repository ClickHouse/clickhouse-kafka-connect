# 1.3.1
* Ensure resources are auto-closed in exactlyOnce

# 1.3.0
* Added additional debug logging around KeeperMap and exactlyOnce

# 1.2.9
* Add `ignorePartitionsWhenBatching` flag to adjust batching when not using exactlyOnce
* Add version information to MBean

# 1.2.8
* Reduce archive size by removing unnecessary dependencies from the jar
* Fix NPE on V1 client when Clickhouse has table with Array(Nested...) field

# 1.2.7
* Bump clickhouse-java to 0.8.0
* Fix for handling of nullable fields in tuples ClickHouse data type

# 1.2.6
* Detect if table schema has changed and refresh the schema
* Allow bypassing field cleanup

# 1.2.5
* Remove redis state provide since we are using KeeperMap for state storage
* Remove unused avro property from `build.gradle.kts`
* Trim schemaless data to only pass the fields that are in the table
* Allow bypassing the schema validation

# 1.2.4
* Adjusting underlying client version to 0.7.0
* Bugfix for UINT handling

# 1.2.3
* Tweaking schema validation to allow for UINT

# 1.2.2
* Adding a new property `tolerateStateMismatch` to allow for the connector to continue processing even if the state stored in ClickHouse does not match the current offset in Kafka

# 1.2.1
* Adding some additional logging details to help debug issues

# 1.2.0
* Adding a KeyToValue transformation to allow for key to be stored in a separate column in ClickHouse

# 1.1.4
* Bugfix to address field value to column name mapping for Tuples

# 1.1.3
* Update to java-client 0.6.3
* Bugfix to address commas in the column name of enums

## 1.1.2
* Adding a "dateTimeFormat" configuration option to allow for custom date/time formatting with String schema values
* Adding ephemeral column support and adding an error message

## 1.1.1
* Bugfix to address string encoding issue
* Bugfix to address issue with nested types and flatten_nested setting conflict
* Bugfix to avoid storing keeper state in same column name if virtual topic is enabled
* Updated java-client to 0.6.1
* Bugfix to let create missing KeeperMap entries if there are some records present already
* Added a flag to allow bypassing RowBinary and RowBinaryWithDefaults format for schema insertions
* Bugfix to remove erroneous error messages about complex type handling

## 1.1.0
* Updated java-client to 0.6.0-patch4
* Added config 'keeperOnCluster' to help self-hosted use exactly-once
* Added support for Tuple type
* Added support for Variant type
* Added support for Nested type
* Refactored Column class so that we use Builder pattern using Lombok
* Refactored recursive Map type parsing to iterative approach using describe_include_subcolumns=1
* Adjusted logging to reduce volume while keeping important information
* Adjusted tests to be more reliable and self cleaning

## 1.0.17
* Added support for ClickHouse Enum type #370
* Added extra break down of time measurement for insert operations 

## 1.0.16
* Removed 1002 from the retriable exceptions list as it's a catch-all for all other exceptions
* Change Java Client to 0.6.0-patch3 and switch tests to use same Base class

## 1.0.15
* Added code 107 to the exception list and added more DLQ logging
* Added ExtractTopic transform to our source so that it would work on MSK

## 1.0.14
* Fixed print vs log bug
* Fixed bug in overlap logic, added more tests to support this
* Fixed null-pointer bug in stop() method
* Updated dependencies

## 1.0.13
* Fix missing jdbcConnectionProperties setup in ClickHouseWriter.getMutationRequest

## 1.0.12
* Added support for multiple databases in single stream using a virtual topic  #41
* Add support for configuring JDBC properties in connection URL (i.e. `?auto_discovery=true`)
* Added minimum version check for ClickHouse (currently 23.3)
* Added support for fixed_string type

## 1.0.11
* Added support for RowBinaryWithDefaults
* Updated dependencies
* Adjusting default values for some settings (like insert_quorum)
* Added string support for DateTime64

## 1.0.10
* Fixed writing into nullable Decimal column by @mlivirov in #276
* Adjusting the deduplication token hash by @Paultagoras in #280
* Fixed exception for when an array type column gets null value by @mlivirov in #279
* Support for data ingestion into Array(DateTime64(3)) type columns by @alexshanabrook in #260

## 1.0.9
* Added more logging to help debug future issues
* Restored send_progress_in_http_headers flag

## 1.0.8
* Remove send_progress_in_http_headers flag as it conflicts
* Updated tests
* Updated dependencies

## 1.0.7
* Fix Handle ClickHouse JDBC Client Timeout Exception by @ygrzjh in #252
* Make sure an HTTP request is successfully processed by ClickHouse 

## 1.0.6
* Added additional logging to help debug future issues

## 1.0.5
* Added 'zkPath' and 'zkDatabase' properties to customize exactly-once state storage

## 1.0.4
* Bugfix

## 1.0.3
* Added support for proxy configurations
* Additional test cases
* Additional error codes added to retry list (242, 319, 999)

## 1.0.2
* Updated dependencies
* Add BYTES type support
* Add Decimal type support
* Additional exception handling
* Retriable Exceptions are now always retried (even when errors.tolerance=all)
* Support for org.apache.kafka.connect.storage.StringConverter

## 1.0.1
* Added support for `tableRefreshInterval` to re-fetch table changes from ClickHouse

## 1.0.0
* Additional tests for ExactlyOnce
* Allows customized ClickHouse settings using `clickhouse.settings` property
* Tweaked deduplication behavior to account for dynamic fields
* Added support for `errors.tolerance` and the DLQ

## 0.0.18
* Support inline schema with org.apache.kafka.connect.data.Timestamp type 
* Support inline schema with org.apache.kafka.connect.data.Time type

## 0.0.17
* Updating Logo

## 0.0.16
* Updated state handling so that warnings are posted, rather than errors + exceptions.
* Adding 285 TOO_FEW_LIVE_REPLICAS to the retry list
* Tweaking the code to switch to JSON handling when default values are present. This should be more reliable (if the
data structure is simple enough) while supporting defaults. This is a temporary solution until we can implement a
longer-term fix in core code.

## 0.0.15
* Added 202 (TOO_MANY_SIMULTANEOUS_QUERIES) Code to retry list [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/109) 
* Added 252 (TOO_MANY_PARTS) to the retry list


## 0.0.14
* Fix for LowCardinality(t) and LowCardinality(Nullable(t)) [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/105)

## 0.0.13
* Fix null exception when getting empty record

## 0.0.12
* Add UNEXPECTED_END_OF_FILE as RetriableException #98

## 0.0.11
* Implemented retry mechanism to fix [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/74)
** Some ClickHouse errors (159 - TIMEOUT_EXCEEDED; 164 - READONLY; 203 - NO_FREE_CONNECTION; 209 - SOCKET_TIMEOUT; 210 - NETWORK_ERROR; 425 - SYSTEM_ERROR) as well as SocketTimeoutException and UnknownHostException will result in the connector retrying the operation (based on configuration).
This should help mitigate temporary (but unavoidable) hiccups in network operations, though this list will likely be tweaked over time as well.

## 0.0.10
* Nullable date columns fix [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/60)

## 0.0.9
### Bug Fixes
* Implicit date conversion [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/57). [#63](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/63)
* Handle null columns [Issue](https://github.com/ClickHouse/clickhouse-kafka-connect/issues/62). [#65](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/65)

## 0.0.8
### Bug Fixes
* Support nullable values in SchemalessRecordConverter. [#55](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/55)
* Add Emojis tests & fix utf-8 bug. [#52](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/52)
* Add manifest.json. [#44](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/44)

## 0.0.7
### Bug Fixes
* Fix index error in describeTable. [#49](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/49)

## 0.0.6
### Bug Fixes
* Don't validate JSON records against the table schema. [#47](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/47)

## 0.0.5
### New features
* Add support for Amazon MSK in [#46](https://github.com/ClickHouse/clickhouse-kafka-connect/pull/46)
