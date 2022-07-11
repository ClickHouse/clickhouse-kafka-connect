package com.clickhouse.kafka.connect.sink;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.DBWriter;
import com.clickhouse.kafka.connect.sink.db.InMemoryDBWriter;
import com.clickhouse.kafka.connect.sink.kafka.RangeContainer;
import com.clickhouse.kafka.connect.sink.processing.Processing;
import com.clickhouse.kafka.connect.sink.state.State;
import com.clickhouse.kafka.connect.sink.state.StateProvider;
import com.clickhouse.kafka.connect.sink.state.StateRecord;
import com.clickhouse.kafka.connect.sink.state.provider.RedisStateProvider;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ClickHouseSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSinkTask.class);

    private ClickHouseNode server = null;

    private Processing processing = null;
    private StateProvider stateProvider = null;
    private DBWriter dbWriter = null;
    private int pingTimeOut = 100;

    private boolean singleTopic = true;

    @Override
    public String version() {
        return "0.0.1";
    }


    private String convertWithStream(List<Object> values, String prefixChar, String suffixChar, String delimiterChar, String trimChar) {
        return values.stream().map(v -> trimChar + v.toString() + trimChar).collect(Collectors.joining(delimiterChar, prefixChar, suffixChar));
    }

    private String extractFields(List<Field> fields, String prefixChar, String suffixChar, String delimiterChar, String trimChar) {
        return fields.stream().map(v -> trimChar + v.name() + trimChar).collect(Collectors.joining(delimiterChar, prefixChar, suffixChar));
    }


    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("start SinkTask: ");
        String hostname = props.get(ClickHouseSinkConnector.HOSTNAME);
        int port = Integer.valueOf(props.get(ClickHouseSinkConnector.PORT)).intValue();
        String database = props.get(ClickHouseSinkConnector.DATABASE);
        String username = props.get(ClickHouseSinkConnector.USERNAME);
        String password = props.get(ClickHouseSinkConnector.PASSWORD);

        LOGGER.info(String.format("hostname: [%s] port [%d] database [%s] username [%s] password [%s]", hostname, port, database, username, password));

        String url = String.format("https://%s:%d/%s", hostname, port, database);

        LOGGER.info("url: " + url);

        if (username != null && password != null) {
            LOGGER.info(String.format("Adding username [%s] password [%s]  ", username, password));
            Map<String, String> options = new HashMap<>();
            options.put("user", username);
            options.put("password", password);
            server = ClickHouseNode.of(url, options);
        } else {
            server = ClickHouseNode.of(url);
        }


        ClickHouseClient clientPing = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);

        if (clientPing.ping(server, pingTimeOut)) {
            LOGGER.info("Ping is successful.");
        } else {
            LOGGER.error("Unable to ping Clickhouse server.");
        }

        stateProvider = new RedisStateProvider();
        dbWriter = new InMemoryDBWriter();
        processing = new Processing(stateProvider,dbWriter);
    }


    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            LOGGER.info("No records in put API");
            return;
        }
        // Group by topic & partition
        Map<String, List<Record>> dataRecords = records.stream()
                .map(v -> Record.convert(v))
                .collect(Collectors.groupingBy(Record::getTopicAndPartition));

        // TODO - Multi process
        for (String topicAndPartition : dataRecords.keySet()) {
            // Running on etch topic & partition
            List<Record> rec = dataRecords.get(topicAndPartition);
            processing.doLogic(rec);
        }


        if (true) return;

        long s1 = System.currentTimeMillis();
        final SinkRecord first = records.iterator().next();
        StringBuffer sb = new StringBuffer();
        sb.append("INSERT INTO stock_v1 ");
        sb.append(extractFields(first.valueSchema().fields(), "(", ")", ",", ""));
        sb.append(" VALUES ");
        //sb.append("")
        int batchSize = records.size();

        LOGGER.info(String.format("Number of records to put %d", batchSize));
        for (SinkRecord record : records) {
            /*
            LOGGER.info(String.format("topic [%s], kafkaPartition [%d], offset [%d] value ",
                    record.topic(),
                    record.kafkaPartition(),
                    record.kafkaOffset())
                    );
            */

            List<Object> values = record.valueSchema().fields().stream().map(field -> ((Struct) record.value()).get(field)).collect(Collectors.toList());
            String valueStr = convertWithStream(values, "(", ")", ",", "'");
            sb.append(valueStr + ",");
//            for ( Field field : record.valueSchema().fields() ) {
//                String name = field.name();
//                LOGGER.info(" name {} value {} ", name, ((Struct) record.value()).get(field));
//            }


        }
        String insertStr = sb.deleteCharAt(sb.length() - 1).toString();
        long s2 = System.currentTimeMillis();
        //LOGGER.info(insertStr);

        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(server) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format
                     .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                     .query(insertStr)
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();
            long totalRows = summary.getTotalRowsToRead();
            LOGGER.info("totalRows {}", totalRows);

        } catch (ClickHouseException e) {
            LOGGER.error(insertStr);
            LOGGER.error("INSERT ", e);
            throw new RuntimeException(e);
        }
        long s3 = System.currentTimeMillis();
        LOGGER.info("batchSize {} data ms {} send {}", batchSize, s2 - s1, s3 - s2);
    }

    // TODO: can be removed ss
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        LOGGER.trace("Test");
    }

    @Override
    public void stop() {

    }
}
