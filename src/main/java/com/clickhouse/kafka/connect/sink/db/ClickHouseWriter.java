package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.client.*;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.data.BinaryStreamUtils;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkTask;
import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.sink.db.mapping.Type;
import com.clickhouse.kafka.connect.util.Mask;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ClickHouseWriter implements DBWriter{

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseWriter.class);

    private ClickHouseHelperClient chc = null;
    private ClickHouseSinkConfig csc = null;

    private Map<String, Table> mapping = null;

    private boolean isBinary = false;

    public ClickHouseWriter() {
        this.mapping = new HashMap<>();
    }

    @Override
    public boolean start(ClickHouseSinkConfig csc) {
        this.csc = csc;
        String hostname = csc.getHostname();
        int port = csc.getPort();
        String database = csc.getDatabase();
        String username = csc.getUsername();
        String password = csc.getPassword();
        boolean sslEnabled = csc.isSslEnabled();
        int timeout = csc.getTimeout();

        LOGGER.info(String.format("hostname: [%s] port [%d] database [%s] username [%s] password [%s] sslEnabled [%s] timeout [%d]", hostname, port, database, username, Mask.passwordMask(password), sslEnabled, timeout));

        chc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port)
                .setDatabase(database)
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .setRetry(csc.getRetry())
                .build();

        if (!chc.ping()) {
            LOGGER.error("Unable to ping Clickhouse server.");
            return false;
        }

        LOGGER.info("Ping is successful.");

        List<Table> tableList = chc.extractTablesMapping();
        if (tableList.isEmpty()) {
            LOGGER.error("Did not find any tables in destination Please create before running.");
            return false;
        }

        for (Table table: tableList) {
            this.mapping.put(table.getName(), table);
        }
        return true;
    }

    @Override
    public void stop() {

    }

    public void setBinary(boolean binary) {
        isBinary = binary;
    }

    // TODO: we need to refactor that
    private String convertHelper(Object v) {
        if (v instanceof List) {
            String value = ((List<?>) v).stream().map( vv -> vv.toString()).collect(Collectors.joining(",","[","]"));
            return value;

        } else {
            return v.toString();
        }
    }
    private String convertWithStream(List<Object> values, String prefixChar, String suffixChar, String delimiterChar, String trimChar) {
        return values
                .stream().map (
                    v ->
                            trimChar + convertHelper(v) + trimChar
                )
                .collect(Collectors.joining(delimiterChar, prefixChar, suffixChar));
    }

    private String extractFields(List<Field> fields, String prefixChar, String suffixChar, String delimiterChar, String trimChar) {
        return fields.stream().map(v -> trimChar + v.name() + trimChar).collect(Collectors.joining(delimiterChar, prefixChar, suffixChar));
    }

    public ClickHouseNode getServer() {
        return chc.getServer();
    }

    @Override
    public void doInsert(List<Record> records) {
        if ( records.isEmpty() )
            return;
        Record first = records.get(0);
        switch (first.getSchemaType()) {
            case SCHEMA:
                doInsertRawBinary(records);
                break;
            case SCHEMA_LESS:
                doInsertSimple(records);
                break;
        }
    }


    private boolean validateDataSchema(Table table, Record record) {
        boolean validSchema = true;
        for (Column col : table.getColumns() ) {
            String colName = col.getName();
            Type type = col.getType();
            Data obj =  record.getJsonMap().get(colName);
            if (obj == null) {
                validSchema = false;
                LOGGER.error(String.format("Table column name [%s] is not found in data record.", colName));
            }
            String colTypeName = type.name();
            String dataTypeName = obj.getFieldType().getName().toUpperCase();
            if (!colTypeName.equals(dataTypeName)) {
                validSchema = false;
                LOGGER.error(String.format("Table column name [%s] type [%s] is not matching data column type [%s]", col.getName(), colTypeName,dataTypeName));
            }
        }
        return validSchema;
    }
    public void doInsertRawBinary(List<Record> records) {
        long s1 = System.currentTimeMillis();

        if ( records.isEmpty() )
            return;
        int batchSize = records.size();

        Record first = records.get(0);
        String topic = first.getTopic();
        LOGGER.info(String.format("Number of records to insert %d to table name %s", batchSize, topic));
        Table table = this.mapping.get(topic);
        if (table == null) {
            //TODO to pick the correct exception here
            throw new RuntimeException(String.format("Table %s does not exists", topic));
        }
        if ( !validateDataSchema(table, first) )
            throw new RuntimeException();
        // Let's test first record
        // Do we have all elements from the table inside the record

        long s2 = System.currentTimeMillis();
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP)) {
            ClickHouseRequest.Mutation request = client.connect(chc.getServer())
            .write()
                    .table(table.getName())
                    .format(ClickHouseFormat.RowBinary)
                    // this is needed to get meaningful response summary
                    .set("insert_quorum", 2)
                    .set("send_progress_in_http_headers", 1);

            ClickHouseConfig config = request.getConfig();
            CompletableFuture<ClickHouseResponse> future;

            try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config, null)) {
                // start the worker thread which transfer data from the input into ClickHouse
                future = request.data(stream.getInputStream()).send();
                // write bytes into the piped stream
                for (Record record: records ) {
                    for (Column col : table.getColumns() ) {
                        String name = col.getName();
                        Type colType = col.getType();
                        Data value = record.getJsonMap().get(name);
                        // TODO: the mapping need to be more efficient
                        switch (colType) {
                            case INT8:
                                BinaryStreamUtils.writeInt8(stream,((Byte)value.getObject()).byteValue());
                                break;
                            case INT16:
                                BinaryStreamUtils.writeInt16(stream,((Short)value.getObject()).shortValue());
                                break;
                            case INT32:
                                BinaryStreamUtils.writeInt32(stream,((Integer)value.getObject()).intValue());
                                break;
                            case INT64:
                                BinaryStreamUtils.writeInt64(stream,((Long)value.getObject()).longValue());
                                break;
                            case FLOAT32:
                                BinaryStreamUtils.writeFloat32(stream,((Float)value.getObject()).floatValue());
                                break;
                            case FLOAT64:
                                BinaryStreamUtils.writeFloat64(stream,((Double)value.getObject()).doubleValue());
                                break;
                            case BOOLEAN:
                                BinaryStreamUtils.writeBoolean(stream, ((Boolean)value.getObject()).booleanValue());
                                break;
                            case STRING:
                                BinaryStreamUtils.writeString(stream, ((String)value.getObject()).getBytes());
                                break;
                        }
                    }

                }
                // We need to close the stream before getting a response
                stream.close();
                ClickHouseResponseSummary summary;
                try (ClickHouseResponse response = future.get()) {
                    summary = response.getSummary();
                    long rows = summary.getWrittenRows();

                } catch (Exception e) {
                    LOGGER.error(String.format("Try to insert %d rows", records.size()), e);
                    throw new RuntimeException();
                }
            } catch (Exception ce) {
                ce.printStackTrace();
                throw new RuntimeException();
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }

        long s3 = System.currentTimeMillis();
        LOGGER.info("batchSize {} data ms {} send {}", batchSize, s2 - s1, s3 - s2);

    }
    public void doInsertSimple(List<Record> records) {
        // TODO: here we will need to make refactor (not to use query & string , but we can make this optimization later )
        long s1 = System.currentTimeMillis();

        if ( records.isEmpty() )
            return;
        int batchSize = records.size();

        Record first = records.get(0);
        String topic = first.getTopic();
        LOGGER.info(String.format("Number of records to insert %d to table name %s", batchSize, topic));
        // Build the insert SQL
        StringBuffer sb = new StringBuffer();
        sb.append(String.format("INSERT INTO %s ", topic));
        sb.append(extractFields(first.getFields(), "(", ")", ",", ""));
        sb.append(" VALUES ");
        LOGGER.debug("sb {}", sb);
        for (Record record: records ) {
            LOGGER.debug("records {}", record.getJsonMap().keySet().stream().collect(Collectors.joining(",", "[", "]")));
            List<Object> values = record.getFields().
                    stream().
                    map(field -> record.getJsonMap().get(field.name())).
                    collect(Collectors.toList());
            String valueStr = convertWithStream(values, "(", ")", ",", "'");
            sb.append(valueStr + ",");
        }
        String insertStr = sb.deleteCharAt(sb.length() - 1).toString();
        long s2 = System.currentTimeMillis();
        //ClickHouseClient.load(server, ClickHouseFormat.RowBinaryWithNamesAndTypes)
        LOGGER.debug("*****************");
        LOGGER.debug(insertStr);
        LOGGER.debug("*****************");
        chc.query(insertStr, ClickHouseFormat.RowBinaryWithNamesAndTypes);
        /*
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(chc.getServer())  // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format
                     .option(ClickHouseClientOption.CONNECTION_TIMEOUT, csc.getTimeout())
                     .option(ClickHouseClientOption.SOCKET_TIMEOUT, csc.getTimeout())
                     .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                     .query(insertStr)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();
            long totalRows = summary.getTotalRowsToRead();
            LOGGER.info("totalRows {}", totalRows);

        } catch (ClickHouseException e) {
            LOGGER.debug(insertStr);
            LOGGER.error(String.format("INSERT ErrorCode %d ", e.getErrorCode()), e);
            throw new RuntimeException(e);
        }

         */
        long s3 = System.currentTimeMillis();
        LOGGER.info("batchSize {} data ms {} send {}", batchSize, s2 - s1, s3 - s2);
    }

    @Override
    public long recordsInserted() {
        return 0;
    }
}
