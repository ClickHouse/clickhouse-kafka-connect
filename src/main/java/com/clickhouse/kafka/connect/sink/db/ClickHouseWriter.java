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

import com.clickhouse.kafka.connect.util.QueryIdentifier;
import com.clickhouse.kafka.connect.util.Utils;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.math.BigDecimal;
import java.util.*;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ClickHouseWriter implements DBWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseWriter.class);

    private ClickHouseHelperClient chc = null;
    private ClickHouseSinkConfig csc = null;

    private Map<String, Table> mapping = null;
    private AtomicBoolean isUpdateMappingRunning = new AtomicBoolean(false);

    public ClickHouseWriter() {
        this.mapping = new HashMap<String, Table>();
    }

    @Override
    public boolean start(ClickHouseSinkConfig csc) {
        LOGGER.trace("Starting ClickHouseWriter");
        this.csc = csc;
        chc = new ClickHouseHelperClient.ClickHouseClientBuilder(csc.getHostname(), csc.getPort(), csc.getProxyType(), csc.getProxyHost(), csc.getProxyPort())
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

        try {
            String chVersion = chc.version();
            LOGGER.info("Connected to ClickHouse version: {}", chVersion);
            String[] versionParts = chVersion.split("\\.");
            if (versionParts.length < 2) {
                LOGGER.error("Unable to determine ClickHouse server version.");
                return false;
            }

            int majorVersion = Integer.parseInt(versionParts[0]);
            int minorVersion = Integer.parseInt(versionParts[1]);
            if (majorVersion < 22 || (majorVersion == 22 && minorVersion < 5)) {
                LOGGER.error("ClickHouse server version is too old to use this connector. Please upgrade to version 22.5 or newer.");
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Unable to determine ClickHouse server version.", e);
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



    public ClickHouseNode getServer() {
        return chc.getServer();
    }

    public void doInsert(List<Record> records, QueryIdentifier queryId) throws IOException, ExecutionException, InterruptedException {
        doInsert(records, queryId, null);
    }

    @Override
    public void doInsert(List<Record> records, QueryIdentifier queryId, ErrorReporter errorReporter) throws IOException, ExecutionException, InterruptedException {
        if (records.isEmpty())
            return;

        Record first = records.get(0);
        String topic = first.getTopic();
        Table table = getTable(topic);
        if (table == null) { return; }//We checked the error flag in getTable, so we don't need to check it again here
        LOGGER.info("Trying to insert [{}] records to table name [{}] (QueryId: [{}])", records.size(), table.getName(), queryId.getQueryId());

        switch (first.getSchemaType()) {
            case SCHEMA:
                doInsertRawBinary(records, table, queryId, table.hasDefaults());
                break;
            case SCHEMA_LESS:
                doInsertJson(records, table, queryId);
                break;
            case STRING_SCHEMA:
                doInsertString(records, table, queryId);
                break;
        }
    }

    private boolean validateDataSchema(Table table, Record record, boolean onlyFieldsName) {
        boolean validSchema = true;
        for (Column col : table.getColumns()) {
            String colName = col.getName();
            Type type = col.getType();
            boolean isNullable = col.isNullable();
            boolean hasDefault = col.hasDefault();
            if (!isNullable && !hasDefault) {
                Map<String, Schema> schemaMap = record.getFields().stream().collect(Collectors.toMap(Field::name, Field::schema));
                var objSchema = schemaMap.get(colName);
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
                                    if (!("DECIMAL".equalsIgnoreCase(colTypeName) && objSchema.name().equals("org.apache.kafka.connect.data.Decimal"))) {
                                        validSchema = false;
                                        LOGGER.error(String.format("Table column name [%s] type [%s] is not matching data column type [%s]", col.getName(), colTypeName, dataTypeName));
                                    }
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
            case DateTime64:
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


    private void doWriteCol(Record record, Column col, ClickHousePipedOutputStream stream, boolean defaultsSupport) throws IOException {
        LOGGER.trace("Writing column {} to stream", col.getName());
        LOGGER.trace("Column type is {}", col.getType());
        String name = col.getName();
        Type colType = col.getType();
        boolean filedExists = record.getJsonMap().containsKey(name);
        if (filedExists) {
            Data value = record.getJsonMap().get(name);
            LOGGER.trace("Column value is {}", value);
            // TODO: the mapping need to be more efficient
            if (defaultsSupport) {
                if (value.getObject() != null) {//Because we now support defaults, we have to send nonNull
                    BinaryStreamUtils.writeNonNull(stream);//Write 0 for no default

                    if (col.isNullable()) {//If the column is nullable
                        BinaryStreamUtils.writeNonNull(stream);//Write 0 for not null
                    }
                } else {//So if the object is null
                    if (col.hasDefault()) {
                        BinaryStreamUtils.writeNull(stream);//Send 1 for default
                        return;
                    } else if (col.isNullable()) {//And the column is nullable
                        BinaryStreamUtils.writeNonNull(stream);
                        BinaryStreamUtils.writeNull(stream);//Then we send null, write 1
                        return;//And we're done
                    } else if (colType == Type.ARRAY) {//If the column is an array
                        BinaryStreamUtils.writeNonNull(stream);//Then we send nonNull
                    } else {
                        throw new RuntimeException(String.format("An attempt to write null into not nullable column '%s'", col.getName()));
                    }
                }
            } else {
                // If column is nullable && the object is also null add the not null marker
                if (col.isNullable() && value.getObject() != null) {
                    BinaryStreamUtils.writeNonNull(stream);
                }
                if (!col.isNullable() && value.getObject() == null) {
                    if (colType == Type.ARRAY)
                        BinaryStreamUtils.writeNonNull(stream);
                    else
                        throw new RuntimeException(String.format("An attempt to write null into not nullable column '%s'", col.getName()));
                }
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
                case Decimal:
                    if (value.getObject() == null) {
                        BinaryStreamUtils.writeNull(stream);
                        return;
                    } else {
                        BigDecimal decimal = (BigDecimal) value.getObject();
                        BinaryStreamUtils.writeDecimal(stream, decimal, col.getPrecision(), col.getScale());
                    }
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

                    if (arrObject == null) {
                        if (defaultsSupport) {
                            BinaryStreamUtils.writeNonNull(stream);
                        }
                        doWritePrimitive(colType, value.getFieldType(), stream, new ArrayList<>());
                    } else {
                        int sizeArrObject = arrObject.size();
                        BinaryStreamUtils.writeVarInt(stream, sizeArrObject);
                        arrObject.forEach(v -> {
                            try {
                                if (col.getSubType().isNullable() && v != null) {
                                    BinaryStreamUtils.writeNonNull(stream);
                                }
                                doWritePrimitive(col.getSubType().getType(), value.getFieldType(), stream, v);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }
                    break;
            }
        } else {
            if (col.hasDefault()) {
                BinaryStreamUtils.writeNull(stream);
            } else if (col.isNullable()) {
                // set null since there is no value
                if (defaultsSupport) {//Only set this if we're using defaults
                    BinaryStreamUtils.writeNonNull(stream);
                }
                BinaryStreamUtils.writeNull(stream);
            } else {
                // no filled and not nullable
                LOGGER.error("Column {} is not nullable and no value is provided", name);
                throw new RuntimeException();
            }
        }
    }


    protected void doInsertRawBinary(List<Record> records, Table table, QueryIdentifier queryId, boolean supportDefaults) throws IOException, ExecutionException, InterruptedException {
        long s1 = System.currentTimeMillis();
        long s2 = 0;
        long s3 = 0;

        Record first = records.get(0);

        if (!validateDataSchema(table, first, false))
            throw new RuntimeException();
        // Let's test first record
        // Do we have all elements from the table inside the record

        s2 = System.currentTimeMillis();
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest.Mutation request;
            if (supportDefaults) {
                request = getMutationRequest(client, ClickHouseFormat.RowBinaryWithDefaults, table.getName(), queryId);
            } else {
                request = getMutationRequest(client, ClickHouseFormat.RowBinary, table.getName(), queryId);
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
                            doWriteCol(record, col, stream, supportDefaults);
                    }
                }
                // We need to close the stream before getting a response
                stream.close();
                try (ClickHouseResponse response = future.get()) {
                    ClickHouseResponseSummary summary = response.getSummary();
                    LOGGER.info("Response Summary - Written Bytes: [{}], Written Rows: [{}] - (QueryId: [{}])", summary.getWrittenBytes(), summary.getWrittenRows(), queryId.getQueryId());
                }
            }
        }

        s3 = System.currentTimeMillis();
        LOGGER.info("batchSize: {} data ms: {} send ms: {} (QueryId: [{}])", records.size(), s2 - s1, s3 - s2, queryId.getQueryId());
    }


    protected void doInsertJson(List<Record> records, Table table, QueryIdentifier queryId) throws IOException, ExecutionException, InterruptedException {
        //https://devqa.io/how-to-convert-java-map-to-json/
        Gson gson = new Gson();
        long s1 = System.currentTimeMillis();
        long s2 = 0;
        long s3 = 0;


        Record first = records.get(0);

        // We don't validate the schema for JSON inserts.  ClickHouse will ignore unknown fields based on the
        // input_format_skip_unknown_fields setting, and missing fields will use ClickHouse defaults


        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest.Mutation request = getMutationRequest(client, ClickHouseFormat.JSONEachRow, table.getName(), queryId);
            ClickHouseConfig config = request.getConfig();
            CompletableFuture<ClickHouseResponse> future;

            try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(config)) {
                // start the worker thread which transfer data from the input into ClickHouse
                future = request.data(stream.getInputStream()).execute();
                // write bytes into the piped stream
                java.lang.reflect.Type gsonType = new TypeToken<HashMap>() {}.getType();
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
                s2 = System.currentTimeMillis();
                try (ClickHouseResponse response = future.get()) {
                    ClickHouseResponseSummary summary = response.getSummary();
                    LOGGER.info("Response Summary - Written Bytes: [{}], Written Rows: [{}] - (QueryId: [{}])", summary.getWrittenBytes(), summary.getWrittenRows(), queryId.getQueryId());
                }
            }
        }
        s3 = System.currentTimeMillis();
        LOGGER.info("batchSize: {} data ms: {} send ms: {} (QueryId: [{}])", records.size(), s2 - s1, s3 - s2, queryId.getQueryId());
    }

    protected void doInsertString(List<Record> records, Table table, QueryIdentifier queryId) throws IOException, ExecutionException, InterruptedException {
        byte[] endingLine = new byte[]{'\n'};
        long s1 = System.currentTimeMillis();
        long s2 = 0;
        long s3 = 0;

        Record first = records.get(0);

        // We don't validate the schema for JSON inserts.  ClickHouse will ignore unknown fields based on the
        // input_format_skip_unknown_fields setting, and missing fields will use ClickHouse defaults
        ClickHouseFormat clickHouseFormat = null;
        switch (csc.getInsertFormat()) {
            case NONE:
                throw new RuntimeException("using org.apache.kafka.connect.storage.StringConverter, but did not enable.");
            case CSV:
                clickHouseFormat = ClickHouseFormat.CSV;
                break;
            case TSV:
                clickHouseFormat = ClickHouseFormat.TSV;
                break;
            default:
                clickHouseFormat = ClickHouseFormat.JSONEachRow;
        }

        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest.Mutation request = getMutationRequest(client, clickHouseFormat, table.getName(), queryId);
            ClickHouseConfig config = request.getConfig();
            CompletableFuture<ClickHouseResponse> future;

            try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(config)) {
                // start the worker thread which transfer data from the input into ClickHouse
                future = request.data(stream.getInputStream()).execute();
                // write bytes into the piped stream
                for (Record record : records) {
                    if (record.getSinkRecord().value() != null) {
                        String data = (String)record.getSinkRecord().value();
                        LOGGER.debug(String.format("data: %s", data));
                        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
                        BinaryStreamUtils.writeBytes(stream, bytes);
                        switch (csc.getInsertFormat()) {
                            case CSV:
                            case TSV:
                                if (bytes[bytes.length-1] != '\n')
                                    BinaryStreamUtils.writeBytes(stream, endingLine);
                                break;
                        }

                    } else {
                        LOGGER.warn(String.format("Getting empty record skip the insert topic[%s] offset[%d]", record.getTopic(), record.getSinkRecord().kafkaOffset()));
                    }
                }

                stream.close();
                s2 = System.currentTimeMillis();
                try (ClickHouseResponse response = future.get()) {
                    ClickHouseResponseSummary summary = response.getSummary();
                    LOGGER.info("Response Summary - Written Bytes: [{}], Written Rows: [{}] - (QueryId: [{}])", summary.getWrittenBytes(), summary.getWrittenRows(), queryId.getQueryId());
                }
            }
        }
        s3 = System.currentTimeMillis();
        LOGGER.info("batchSize: {} data ms: {} send ms: {} (QueryId: [{}])", records.size(), s2 - s1, s3 - s2, queryId.getQueryId());
    }






    private ClickHouseClient getClient() {
        return ClickHouseClient.builder()
                .options(chc.getDefaultClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
    }
    private ClickHouseRequest.Mutation getMutationRequest(ClickHouseClient client, ClickHouseFormat format, String tableName, QueryIdentifier queryId) {
        ClickHouseRequest.Mutation request = client.read(chc.getServer())
                .write()
                .table(tableName, queryId.getQueryId())
                .format(format)
                .set("insert_deduplication_token", queryId.getDeduplicationToken());

        for (String clickhouseSetting : csc.getClickhouseSettings().keySet()) {//THIS ASSUMES YOU DON'T ADD insert_deduplication_token
            request.set(clickhouseSetting, csc.getClickhouseSettings().get(clickhouseSetting));
        }

        request.option(ClickHouseClientOption.WRITE_BUFFER_SIZE, 8192);

        return request;
    }
    private Table getTable(String topic) {
        String tableName = Utils.getTableName(topic, csc.getTopicToTableMap());
        Table table = this.mapping.get(tableName);
        if (table == null) {
            if (csc.getSuppressTableExistenceException()) {
                LOGGER.warn("Table [{}] does not exist, but error was suppressed.", tableName);
            } else {
                //TODO to pick the correct exception here
                LOGGER.error("Table [{}] does not exist - see docs for more details about table names and topic names.", tableName);
                throw new RuntimeException(String.format("Table %s does not exist", tableName));
            }
        }

        return table;//It'll only be null if we suppressed the error
    }


    @Override
    public long recordsInserted() {
        return 0;
    }
}
