package com.clickhouse.kafka.connect;

import com.clickhouse.client.*;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args)  {
        System.out.println("Hello World!"); // Display the string.
        String username = "default";
        String password = "CaxGTmBZpCsg";
        String hostname = "flx46079nl.us-west-2.aws.clickhouse-dev.com";
        int port = 8443;
        String database = "default";


        Map<String, String> options = new HashMap<>();
        options.put("user", username);
        options.put("password",  password);

        ClickHouseNode server = ClickHouseNode.of("https://flx46079nl.us-west-2.aws.clickhouse-dev.com:8443/default", options);


        ClickHouseClient clientPing = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);

        System.out.println(clientPing.ping(server, 10000));



        /*
        client.connect(server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query()
        */

        String data = "INSERT INTO stock_v1 (side,quantity,symbol,price,acco" +
                "unt,userid) VALUES ('BUY','842','ZBZX','215','ABC123','User_1'),('SELL','3191','ZTEST','344','XYZ789','User_7'),('BUY','4433','ZTEST','665','LMN456','User_1'),('BUY','2125','ZBZX','775','XYZ789\n" +
                "','User_6')";

        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
             ClickHouseResponse response = client.connect(server) // or client.connect(endpoints)
                     // you'll have to parse response manually if using a different format
                     //.format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                     .query(data)
                     .executeAndWait()) {
            // or response.stream() if you prefer stream API

            for (ClickHouseRecord r : response.records()) {
                System.out.println("Test");
            }

            ClickHouseResponseSummary summary = response.getSummary();
            long totalRows = summary.getTotalRowsToRead();

        } catch (Exception e) {
            System.out.println("Error");
            System.out.println(e.getMessage());
        }
        //        if ( client.ping(server, 3000) ) {
//            // TODO: lets continue with the code
//            System.out.println("ping works");
//        } else {
//            System.out.println("No ping to ClickHouse server. ");
//        }

    }
}
