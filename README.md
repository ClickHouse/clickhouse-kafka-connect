## Installation
- Run ./gradlew createConfluentArchive
- Copy & Unzip the Archive Of ClickHouse Sink to Connect Plugin directory (Can be found at "/usr/share/java,/usr/share/confluent-hub-components") (clickhouse-kafka-connect-1.8.0-SNAPSHOT.zip)
- Restart connect worker to load ClickHouse Sink
- Open Control Center 
- Add ClickHouseSink & Configure 

## Features
- Exactly-once semantics with ClickHouse KeeperMap new Table (storing state progress)    
- Handling Schema & Schemaless out of the box
- Supporting major basic types of ClickHouse

### Limitation 
- Batching is done by configuring the Kafka Consumer properties   
- Table name should same as Topic name
- Table should be already created in ClickHouse 
- Not Supported ClickHouse complex types JSON, Tuple, Map
- Currently, Exactly-once semantics implemented only with KeeperMap (Not with redis)
- Reset topic for replaying events is not implemented (need to delete the state before resting the topic)
