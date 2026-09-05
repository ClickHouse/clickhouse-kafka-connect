package com.clickhouse.kafka.connect.sink.db.helper;

import com.clickhouse.client.config.ClickHouseProxyType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ClickHouseHelperClientConfigTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void clientCompressionIsPropagatedToV2Client(boolean clientCompression) {
        try (ClickHouseHelperClient client = new ClickHouseHelperClient.ClickHouseClientBuilder(
                "localhost", 8123, ClickHouseProxyType.IGNORE, "", -1)
                .setClientCompression(clientCompression)
                .build()) {
            Assertions.assertEquals(
                    String.valueOf(clientCompression),
                    client.getClient().getConfiguration().get("decompress"));
        }
    }
}
