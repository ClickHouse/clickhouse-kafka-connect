

## Installation
- Copy & Unzip the Archive Of ClickHouse Sink to Connect Plugin directory (Can be found at "/usr/share/java,/usr/share/confluent-hub-components") (clickhouse-kafka-connect-1.8.0-SNAPSHOT.zip)
- Restart connect worker to load ClickHouse Sink
- Open Control Center & 
- Add ClickHouseSink & Configure 

### Limitation 
- Currently, exactly once semantics are missing until we deploy KeeperMap
- Table name should same as Topic name
- Table should be already created in ClickHouse 
-  
