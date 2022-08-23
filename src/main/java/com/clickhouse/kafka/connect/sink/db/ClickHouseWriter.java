package com.clickhouse.kafka.connect.sink.db;

import com.clickhouse.client.*;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkTask;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClickHouseWriter implements DBWriter{

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseWriter.class);

    //private ClickHouseNode server = null;
    private int pingTimeOut = 30*1000;

    private ClickHouseHelperClient chc = null;

    @Override
    public boolean start(ClickHouseSinkConfig csc) {
        String hostname = csc.getHostname();
        int port = csc.getPort();
        String database = csc.getDatabase();
        String username = csc.getUsername();
        String password = csc.getPassword();
        boolean sslEnabled = csc.isSslEnabled();
        int timeout = csc.getTimeout();

        LOGGER.info(String.format("hostname: [%s] port [%d] database [%s] username [%s] password [%s] sslEnabled [%s] timeout [%d]", hostname, port, database, username, password, sslEnabled, pingTimeOut));

        chc = new ClickHouseHelperClient.ClickHouseClientBuilder(hostname, port)
                .setDatabase(database)
                .setUsername(username)
                .setPassword(password)
                .sslEnable(sslEnabled)
                .setTimeout(timeout)
                .build();


//        String protocol = "http";
//        if (sslEnabled == true )
//            protocol += "s";
//
//        String url = String.format("%s://%s:%d/%s", protocol, hostname, port, database);
//
//        LOGGER.info("url: " + url);
//
//        if (username != null && password != null) {
//            LOGGER.info(String.format("Adding username [%s] password [%s]  ", username, password));
//            Map<String, String> options = new HashMap<>();
//            options.put("user", username);
//            options.put("password", password);
//            this.server = ClickHouseNode.of(url, options);
//        } else {
//            this.server = ClickHouseNode.of(url);
//        }
//
//
//        ClickHouseClient clientPing = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);

//        if (clientPing.ping(server, pingTimeOut)) {
//            LOGGER.info("Ping is successful.");
//            return true;
//        }

        if (chc.ping()) {
            LOGGER.info("Ping is successful.");
            return true;
        }

        LOGGER.error("Unable to ping Clickhouse server.");
        return false;
    }

    @Override
    public void stop() {

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
        LOGGER.info("sb {}", sb);
        for (Record record: records ) {
            LOGGER.info("records {}", record.getJsonMap().keySet().stream().collect(Collectors.joining(",", "[", "]")));
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
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(chc.getServer())  // or client.connect(endpoints)
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

    @Override
    public long recordsInserted() {
        return 0;
    }
}
