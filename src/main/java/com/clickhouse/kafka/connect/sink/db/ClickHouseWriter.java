package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.client.*;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.data.Data;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.sink.db.mapping.Column;
import com.clickhouse.kafka.connect.sink.db.mapping.Table;
import com.clickhouse.kafka.connect.sink.db.mapping.Type;
import com.clickhouse.kafka.connect.sink.dlq.DuplicateException;
import com.clickhouse.kafka.connect.sink.dlq.ErrorReporter;
import com.clickhouse.kafka.connect.util.Mask;

import com.clickhouse.kafka.connect.util.Utils;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ClickHouseWriter implements DBWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseWriter.class);

    private ClickHouseHelperClient chc = null;
    private ClickHouseSinkConfig csc = null;

    private Map<String, Table> mapping = null;
    private AtomicBoolean isUpdateMappingRunning = new AtomicBoolean(false);

    private boolean isBinary = false;

    public ClickHouseWriter() {
        this.mapping = new HashMap<String, Table>();
    }

    @Override
    public boolean start(ClickHouseSinkConfig csc) {
        LOGGER.trace("Starting ClickHouseWriter");
        this.csc = csc;
        chc = new ClickHouseHelperClient.ClickHouseClientBuilder(csc.getHostname(), csc.getPort())
                .setDatabase(csc.getDatabase())
                .setUsername(csc.getUsername())
                .setPassword(csc.getPassword())
                .sslEnable(csc.isSslEnabled())
                .setTimeout(csc.getTimeout())
                .setRetry(csc.getRetry())
                .build();

        if (!chc.ping()) {
            LOGGER.error("Unable to ping Clickhouse server.");
            return false;
        }

        LOGGER.debug("Ping was successful.");

        this.updateMapping();
        if (mapping.isEmpty()) {
            LOGGER.error("Did not find any tables in destination Please create before running.");
            return false;
        }

        return true;
    }

    public void updateMapping() {
        // Do not start a new update cycle if one is already in progress
        if (this.isUpdateMappingRunning.get()) {
            return;
        }
        this.isUpdateMappingRunning.set(true);

        LOGGER.debug("Update table mapping.");

        try {
            // Getting tables from ClickHouse
            List<Table> tableList = this.chc.extractTablesMapping(this.mapping);
            if (tableList.isEmpty()) {
                return;
            }

            HashMap<String, Table> mapping = new HashMap<String, Table>();

            // Adding new tables to mapping
            // TODO: check Kafka Connect's topics name or topics regex config and
            // only add tables to in-memory mapping that matches the topics we consume.
            for (Table table : tableList) {
                mapping.put(table.getName(), table);
            }

            this.mapping = mapping;
        } finally {
            this.isUpdateMappingRunning.set(false);
        }
    }

    @Override
    public void stop() {
        LOGGER.debug("Stopping ClickHouseWriter");
    }

    public void setBinary(boolean binary) {
        isBinary = binary;
    }

    // TODO: we need to refactor that
    private String convertHelper(Object v) {
        if (v instanceof List) {
            return ((List<?>) v).stream().map(vv -> vv.toString()).collect(Collectors.joining(",", "[", "]"));

        } else {
            return v.toString();
        }
    }

    private String convertWithStream(List<Object> values, String prefixChar, String suffixChar, String delimiterChar, String trimChar) {
        return values
                .stream().map(
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

    public void doInsert(List<Record> records) {
        doInsert(records, null);
    }

    @Override
    public void doInsert(List<Record> records, ErrorReporter errorReporter) {
        if (records.isEmpty())
            return;

        try {
            Record first = records.get(0);
            String topic = first.getTopic();
            Table table = this.mapping.get(Utils.getTableName(topic, csc.getTopicToTableMap()));
            LOGGER.debug("Actual Min Offset: {} Max Offset: {} Partition: {}",
                    first.getRecordOffsetContainer().getOffset(),
                    records.get(records.size() - 1).getRecordOffsetContainer().getOffset(),
                    first.getRecordOffsetContainer().getPartition());
            LOGGER.debug("Table: {}", table);

            switch (first.getSchemaType()) {
                case SCHEMA:
                    if (table.hasDefaults()) {
                        LOGGER.debug("Default value present, switching to JSON insert instead.");
                        doInsertJson(records);
                    } else {
                        doInsertRawBinary(records);
                    }
                    break;
                case SCHEMA_LESS:
                    doInsertJson(records);
                    break;
                case STRING_SCHEMA:
                    doInsertString(records);
                    break;
            }
        } catch (Exception e) {
            LOGGER.trace("Passing the exception to the exception handler.");
            Utils.handleException(e, csc.getErrorsTolerance());
            if (csc.getErrorsTolerance() && errorReporter != null) {
                LOGGER.debug("Sending records to DLQ.");
                records.forEach(r -> Utils.sendTODlq(errorReporter, r, e));
            }
        }
    }

    private boolean validateDataSchema(Table table, Record record, boolean onlyFieldsName) {
        boolean validSchema = true;
        for (Column col : table.getColumns()) {
            String colName = col.getName();
            Type type = col.getType();
            boolean isNullable = col.isNullable();
            if (!isNullable) {
                Data obj = record.getJsonMap().get(colName);
                if (obj == null) {
                    validSchema = false;
                    LOGGER.error(String.format("Table column name [%s] was not found.", colName));
                } else if (!onlyFieldsName) {
                    String colTypeName = type.name();
                    String dataTypeName = obj.getFieldType().getName().toUpperCase();
                    // TODO: make extra validation for Map/Array type
                    LOGGER.debug(String.format("Column type name [%s] and data type name [%s]", colTypeName, dataTypeName));
                    switch (colTypeName) {
                        case "Date":
                        case "Date32":
                        case "DateTime":
                        case "DateTime64":
                        case "UUID":
                            break;//I notice we just break here, rather than actually validate the type
                        default:
                            if (!colTypeName.equals(dataTypeName)) {
                                if (!(colTypeName.equals("STRING") && dataTypeName.equals("BYTES"))) {
                                    validSchema = false;
                                    LOGGER.error(String.format("Table column name [%s] type [%s] is not matching data column type [%s]", col.getName(), colTypeName, dataTypeName));
                                }
                            }
                    }
                }
            }
        }
        return validSchema;
    }

    private void doWriteDates(Type type, ClickHousePipedOutputStream stream, Data value) throws IOException {
        // TODO: develop more specific tests to have better coverage
        if (value.getObject() == null) {
            BinaryStreamUtils.writeNull(stream);
            return;
        }
        boolean unsupported = false;
        switch (type) {
            case Date:
                if (value.getFieldType().equals(Schema.Type.INT32)) {
                    if (value.getObject().getClass().getName().endsWith(".Date")) {
                        Date date = (Date) value.getObject();
                        int timeInDays = (int) TimeUnit.MILLISECONDS.toDays(date.getTime());
                        BinaryStreamUtils.writeUnsignedInt16(stream, timeInDays);
                    } else {
                        BinaryStreamUtils.writeUnsignedInt16(stream, (Integer) value.getObject());
                    }
                } else {
                    unsupported = true;
                }
                break;
            case Date32:
                if (value.getFieldType().equals(Schema.Type.INT32)) {
                    if (value.getObject().getClass().getName().endsWith(".Date")) {
                        Date date = (Date) value.getObject();
                        int timeInDays = (int) TimeUnit.MILLISECONDS.toDays(date.getTime());
                        BinaryStreamUtils.writeInt32(stream, timeInDays);
                    } else {
                        BinaryStreamUtils.writeInt32(stream, (Integer) value.getObject());
                    }
                } else {
                    unsupported = true;
                }
                break;
            case DateTime:
                if (value.getFieldType().equals(Schema.Type.INT32) || value.getFieldType().equals(Schema.Type.INT64)) {
                    if (value.getObject().getClass().getName().endsWith(".Date")) {
                        Date date = (Date) value.getObject();
                        long epochSecond = date.toInstant().getEpochSecond();
                        BinaryStreamUtils.writeUnsignedInt32(stream, epochSecond);
                    } else {
                        BinaryStreamUtils.writeUnsignedInt32(stream, (Long) value.getObject());
                    }
                } else {

                    unsupported = true;
                }
                break;
            case DateTime64:
                if (value.getFieldType().equals(Schema.Type.INT64)) {
                    if (value.getObject().getClass().getName().endsWith(".Date")) {
                        Date date = (Date) value.getObject();
                        long time = date.getTime();
                        BinaryStreamUtils.writeInt64(stream, time);
                    } else {
                        BinaryStreamUtils.writeInt64(stream, (Long) value.getObject());
                    }
                } else {
                    unsupported = true;
                }
                break;
        }
        if (unsupported) {
            String msg = String.format("Not implemented conversion from %s to %s", value.getFieldType(), type);
            LOGGER.error(msg);
            throw new DataException(msg);
        }
    }

    private void doWritePrimitive(Type columnType, Schema.Type dataType, ClickHousePipedOutputStream stream, Object value) throws IOException {
        LOGGER.trace("Writing primitive type: {}, value: {}", columnType, value);

        if (value == null) {
            BinaryStreamUtils.writeNull(stream);
            return;
        }
        switch (columnType) {
            case INT8:
                BinaryStreamUtils.writeInt8(stream, (Byte) value);
                break;
            case INT16:
                BinaryStreamUtils.writeInt16(stream, (Short) value);
                break;
            case INT32:
                if (value.getClass().getName().endsWith(".Date")) {
                    Date date = (Date) value;
                    int time = (int) date.getTime();
                    BinaryStreamUtils.writeInt32(stream, time);
                } else {
                    BinaryStreamUtils.writeInt32(stream, (Integer) value);
                }
                break;
            case INT64:
                if (value.getClass().getName().endsWith(".Date")) {
                    Date date = (Date) value;
                    long time = date.getTime();
                    BinaryStreamUtils.writeInt64(stream, time);
                } else {
                    BinaryStreamUtils.writeInt64(stream, (Long) value);
                }
                break;
            case UINT8:
                BinaryStreamUtils.writeUnsignedInt8(stream, (Byte) value);
                break;
            case UINT16:
                BinaryStreamUtils.writeUnsignedInt16(stream, (Short) value);
                break;
            case UINT32:
                BinaryStreamUtils.writeUnsignedInt32(stream, (Integer) value);
                break;
            case UINT64:
                BinaryStreamUtils.writeUnsignedInt64(stream, (Long) value);
                break;
            case FLOAT32:
                BinaryStreamUtils.writeFloat32(stream, (Float) value);
                break;
            case FLOAT64:
                BinaryStreamUtils.writeFloat64(stream, (Double) value);
                break;
            case BOOLEAN:
                BinaryStreamUtils.writeBoolean(stream, (Boolean) value);
                break;
            case STRING:
                if (Schema.Type.BYTES.equals(dataType)) {
                    BinaryStreamUtils.writeString(stream, (byte[]) value);
                } else {
                    BinaryStreamUtils.writeString(stream, ((String) value).getBytes());
                }
                break;
            case UUID:
                BinaryStreamUtils.writeUuid(stream, UUID.fromString((String) value));
                break;
        }
    }

    private void doWriteCol(Record record, Column col, ClickHousePipedOutputStream stream) throws IOException {
        LOGGER.trace("Writing column {} to stream", col.getName());
        LOGGER.trace("Column type is {}", col.getType());

        String name = col.getName();
        Type colType = col.getType();
        boolean filedExists = record.getJsonMap().containsKey(name);
        if (filedExists) {
            Data value = record.getJsonMap().get(name);
            LOGGER.trace("Column value is {}", value);
            // TODO: the mapping need to be more efficient
            // If column is nullable && the object is also null add the not null marker
            if (col.isNullable() && value.getObject() != null) {
                BinaryStreamUtils.writeNonNull(stream);
            }
            if (!col.isNullable() && value.getObject() == null) {
                // this the situation when the col is not isNullable, but the data is null here we need to drop the records
                throw new RuntimeException(("col.isNullable() is false and value is empty"));
            }
            switch (colType) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case UINT8:
                case UINT16:
                case UINT32:
                case UINT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case UUID:
                case STRING:
                    doWritePrimitive(colType, value.getFieldType(), stream, value.getObject());
                    break;
                case Date:
                case Date32:
                case DateTime:
                case DateTime64:
                    doWriteDates(colType, stream, value);
                    break;
                case MAP:
                    Map<?, ?> mapTmp = (Map<?, ?>) value.getObject();
                    int mapSize = mapTmp.size();
                    BinaryStreamUtils.writeVarInt(stream, mapSize);
                    mapTmp.forEach((key, value1) -> {
                        try {
                            doWritePrimitive(col.getMapKeyType(), value.getFieldType(), stream, key);
                            doWritePrimitive(col.getMapValueType(), value.getFieldType(), stream, value1);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    break;
                case ARRAY:
                    List<?> arrObject = (List<?>) value.getObject();
                    int sizeArrObject = arrObject.size();
                    BinaryStreamUtils.writeVarInt(stream, sizeArrObject);
                    arrObject.forEach(v -> {
                        try {
                            doWritePrimitive(col.getSubType().getType(), value.getFieldType(), stream, v);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    break;
            }
        } else {
            // no filled and not nullable
            LOGGER.error("Column {} is not nullable and no value is provided", name);
            throw new RuntimeException();
        }
    }


    public void doInsertRawBinary(List<Record> records) throws IOException, ExecutionException, InterruptedException {
        long s1 = System.currentTimeMillis();

        if (records.isEmpty())
            return;
        int batchSize = records.size();

        Record first = records.get(0);
        String topic = first.getTopic();
        LOGGER.info("Inserting {} records into topic {}", batchSize, topic);
        Table table = this.mapping.get(Utils.getTableName(topic, csc.getTopicToTableMap()));
        if (table == null) {
            //TODO to pick the correct exception here
            throw new RuntimeException(String.format("Table %s does not exist.", topic));
        }

        if (!validateDataSchema(table, first, false))
            throw new RuntimeException();
        // Let's test first record
        // Do we have all elements from the table inside the record

        long s2 = System.currentTimeMillis();
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP)) {
            ClickHouseRequest.Mutation request = client.read(chc.getServer())
                    .option(ClickHouseClientOption.PRODUCT_NAME, "clickhouse-kafka-connect/"+ClickHouseClientOption.class.getPackage().getImplementationVersion())
                    .write()
                    .table(table.getName())
                    .format(ClickHouseFormat.RowBinary)
                    // this is needed to get meaningful response summary
                    .set("insert_deduplication_token", first.getRecordOffsetContainer().getOffset() + first.getTopicAndPartition());

            for (String clickhouseSetting : csc.getClickhouseSettings().keySet()) {//THIS ASSUMES YOU DON'T ADD insert_deduplication_token
                request.set(clickhouseSetting, csc.getClickhouseSettings().get(clickhouseSetting));
            }

            ClickHouseConfig config = request.getConfig();
            CompletableFuture<ClickHouseResponse> future;

            try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config)) {
                // start the worker thread which transfer data from the input into ClickHouse
                future = request.data(stream.getInputStream()).execute();
                // write bytes into the piped stream
                for (Record record : records) {
                    if (record.getSinkRecord().value() != null) {
                        for (Column col : table.getColumns())
                            doWriteCol(record, col, stream);
                    }
                }
                // We need to close the stream before getting a response
                stream.close();
                ClickHouseResponseSummary summary;
                try (ClickHouseResponse response = future.get()) {
                    summary = response.getSummary();
                    long rows = summary.getWrittenRows();

                } catch (Exception e) {
                    LOGGER.debug("Reading results after closing stream to ensure insert happened failed.", e);
                    throw e;
                }
            } catch (Exception e) {
                LOGGER.trace("Exception", e);
                throw e;
            }
        } catch (Exception e) {
            LOGGER.trace("Exception", e);
            throw e;
        }

        long s3 = System.currentTimeMillis();
        LOGGER.info("batchSize {} data ms {} send {}", batchSize, s2 - s1, s3 - s2);
    }


    public void doInsertJson(List<Record> records) throws IOException, ExecutionException, InterruptedException {
        //https://devqa.io/how-to-convert-java-map-to-json/
        Gson gson = new Gson();
        long s1 = System.currentTimeMillis();
        long s2 = 0;
        long s3 = 0;

        if (records.isEmpty())
            return;
        int batchSize = records.size();

        Record first = records.get(0);
        String topic = first.getTopic();
        LOGGER.info(String.format("Number of records to insert %d to table name %s", batchSize, topic));
        Table table = this.mapping.get(Utils.getTableName(topic, csc.getTopicToTableMap()));
        if (table == null) {
            if (csc.getSuppressTableExistenceException()) {
                LOGGER.error("Table {} does not exist - see docs for more details about table names and topic names.", topic);
                return;
            } else {
                //TODO to pick the correct exception here
                LOGGER.error("Table {} does not exist - see docs for more details about table names and topic names.", topic);
                throw new RuntimeException(String.format("Table %s does not exists", topic));
            }
        }

        // We don't validate the schema for JSON inserts.  ClickHouse will ignore unknown fields based on the
        // input_format_skip_unknown_fields setting, and missing fields will use ClickHouse defaults

        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP)) {
            ClickHouseRequest.Mutation request = client.read(chc.getServer())
                    .option(ClickHouseClientOption.PRODUCT_NAME, "clickhouse-kafka-connect/"+ClickHouseClientOption.class.getPackage().getImplementationVersion())
                    .write()
                    .table(table.getName())
                    .format(ClickHouseFormat.JSONEachRow)
                    // this is needed to get meaningful response summary
                    .set("insert_deduplication_token", first.getRecordOffsetContainer().getOffset() + first.getTopicAndPartition());

            for (String clickhouseSetting : csc.getClickhouseSettings().keySet()) {//THIS ASSUMES YOU DON'T ADD insert_deduplication_token
                request.set(clickhouseSetting, csc.getClickhouseSettings().get(clickhouseSetting));
            }


            ClickHouseConfig config = request.getConfig();
            request.option(ClickHouseClientOption.WRITE_BUFFER_SIZE, 8192);

            CompletableFuture<ClickHouseResponse> future;

            try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(config)) {
                // start the worker thread which transfer data from the input into ClickHouse
                future = request.data(stream.getInputStream()).execute();
                // write bytes into the piped stream
                for (Record record : records) {
                    if (record.getSinkRecord().value() != null) {
                        Map<String, Object> data;
                        switch (record.getSchemaType()) {
                            case SCHEMA:
                                data = new HashMap<>(16);
                                Struct struct = (Struct) record.getSinkRecord().value();
                                for (Field field : struct.schema().fields()) {
                                    data.put(field.name(), struct.get(field));//Doesn't handle multi-level object depth
                                }
                                break;
                            default:
                                data = (Map<String, Object>) record.getSinkRecord().value();
                                break;
                        }

                        java.lang.reflect.Type gsonType = new TypeToken<HashMap>() {
                        }.getType();
                        String gsonString = gson.toJson(data, gsonType);
                        LOGGER.trace("topic {} partition {} offset {} payload {}",
                                record.getTopic(),
                                record.getRecordOffsetContainer().getPartition(),
                                record.getRecordOffsetContainer().getOffset(),
                                gsonString);
                        BinaryStreamUtils.writeBytes(stream, gsonString.getBytes(StandardCharsets.UTF_8));
                    } else {
                        LOGGER.warn(String.format("Getting empty record skip the insert topic[%s] offset[%d]", record.getTopic(), record.getSinkRecord().kafkaOffset()));
                    }
                }

                stream.close();
                ClickHouseResponseSummary summary;
                s2 = System.currentTimeMillis();
                try (ClickHouseResponse response = future.get()) {
                    summary = response.getSummary();
                    long rows = summary.getWrittenRows();
                    LOGGER.debug("Number of rows inserted: {}", rows);
                } catch (Exception e) {//This is mostly for auto-closing
                    LOGGER.trace("Exception", e);
                    throw e;
                }
            } catch (Exception e) {//This is mostly for auto-closing
                LOGGER.trace("Exception", e);
                throw e;
            }
        } catch (Exception e) {//This is mostly for auto-closing
            LOGGER.trace("Exception", e);
            throw e;
        }
        s3 = System.currentTimeMillis();
        LOGGER.info("batchSize {} data ms {} send {}", batchSize, s2 - s1, s3 - s2);
    }

    public void doInsertString(List<Record> records) throws IOException, ExecutionException, InterruptedException {
        //https://devqa.io/how-to-convert-java-map-to-json/
        long s1 = System.currentTimeMillis();
        long s2 = 0;
        long s3 = 0;

        if (records.isEmpty())
            return;
        int batchSize = records.size();

        Record first = records.get(0);
        String topic = first.getTopic();
        LOGGER.info(String.format("Number of records to insert %d to table name %s", batchSize, topic));
        Table table = this.mapping.get(Utils.getTableName(topic, csc.getTopicToTableMap()));
        if (table == null) {
            if (csc.getSuppressTableExistenceException()) {
                LOGGER.error("Table {} does not exist - see docs for more details about table names and topic names.", topic);
                return;
            } else {
                //TODO to pick the correct exception here
                LOGGER.error("Table {} does not exist - see docs for more details about table names and topic names.", topic);
                throw new RuntimeException(String.format("Table %s does not exists", topic));
            }
        }

        // We don't validate the schema for JSON inserts.  ClickHouse will ignore unknown fields based on the
        // input_format_skip_unknown_fields setting, and missing fields will use ClickHouse defaults
        ClickHouseFormat clickHouseFormat = null;
        switch (csc.getInsertFormat()) {
            case CSV:
                clickHouseFormat = ClickHouseFormat.CSV;
                break;
            case TSV:
                clickHouseFormat = ClickHouseFormat.TSV;
                break;
            default:
                clickHouseFormat = ClickHouseFormat.JSONEachRow;
        }

        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP)) {
            ClickHouseRequest.Mutation request = client.read(chc.getServer())
                    .option(ClickHouseClientOption.PRODUCT_NAME, "clickhouse-kafka-connect/"+ClickHouseClientOption.class.getPackage().getImplementationVersion())
                    .write()
                    .table(table.getName())
                    .format(clickHouseFormat)
                    // this is needed to get meaningful response summary
                    .set("insert_deduplication_token", first.getRecordOffsetContainer().getOffset() + first.getTopicAndPartition());

            for (String clickhouseSetting : csc.getClickhouseSettings().keySet()) {//THIS ASSUMES YOU DON'T ADD insert_deduplication_token
                request.set(clickhouseSetting, csc.getClickhouseSettings().get(clickhouseSetting));
            }


            ClickHouseConfig config = request.getConfig();
            request.option(ClickHouseClientOption.WRITE_BUFFER_SIZE, 8192);

            CompletableFuture<ClickHouseResponse> future;

            try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(config)) {
                // start the worker thread which transfer data from the input into ClickHouse
                future = request.data(stream.getInputStream()).execute();
                // write bytes into the piped stream
                for (Record record : records) {
                    if (record.getSinkRecord().value() != null) {
                        String data = (String)record.getSinkRecord().value();
                        BinaryStreamUtils.writeBytes(stream, data.getBytes(StandardCharsets.UTF_8));
                    } else {
                        LOGGER.warn(String.format("Getting empty record skip the insert topic[%s] offset[%d]", record.getTopic(), record.getSinkRecord().kafkaOffset()));
                    }
                }

                stream.close();
                ClickHouseResponseSummary summary;
                s2 = System.currentTimeMillis();
                try (ClickHouseResponse response = future.get()) {
                    summary = response.getSummary();
                    long rows = summary.getWrittenRows();
                    LOGGER.debug("Number of rows inserted: {}", rows);
                } catch (Exception e) {//This is mostly for auto-closing
                    LOGGER.trace("Exception", e);
                    throw e;
                }
            } catch (Exception e) {//This is mostly for auto-closing
                LOGGER.trace("Exception", e);
                throw e;
            }
        } catch (Exception e) {//This is mostly for auto-closing
            LOGGER.trace("Exception", e);
            throw e;
        }
        s3 = System.currentTimeMillis();
        LOGGER.info("batchSize {} data ms {} send {}", batchSize, s2 - s1, s3 - s2);
    }
    @Override
    public long recordsInserted() {
        return 0;
    }
}
