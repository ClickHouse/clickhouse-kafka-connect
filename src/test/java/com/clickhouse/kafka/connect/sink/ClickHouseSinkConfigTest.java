package com.clickhouse.kafka.connect.sink;

import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ClickHouseSinkConfigTest {

    @Test
    public void clientCompressionDefaultsToFalseWhenUnset() {
        ClickHouseSinkConfig config = new ClickHouseSinkConfig(baseProps());

        Assertions.assertFalse(config.isClientCompression());
    }

    @Test
    public void clientCompressionRejectsV1Client() {
        Map<String, String> props = baseProps();
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, "V1");
        props.put(ClickHouseSinkConfig.CLIENT_COMPRESSION, "true");

        ConfigException error = Assertions.assertThrows(ConfigException.class,
                () -> new ClickHouseSinkConfig(props));

        Assertions.assertTrue(error.getMessage().contains(ClickHouseSinkConfig.CLIENT_COMPRESSION));
        Assertions.assertTrue(error.getMessage().contains(ClickHouseSinkConnector.CLIENT_VERSION));
    }

    @Test
    public void clientCompressionRejectsUnsetClientVersionBecauseItDefaultsToV1() {
        Map<String, String> props = baseProps();
        props.remove(ClickHouseSinkConnector.CLIENT_VERSION);
        props.put(ClickHouseSinkConfig.CLIENT_COMPRESSION, "true");

        ConfigException error = Assertions.assertThrows(ConfigException.class,
                () -> new ClickHouseSinkConfig(props));

        Assertions.assertTrue(error.getMessage().contains(ClickHouseSinkConfig.CLIENT_COMPRESSION));
        Assertions.assertTrue(error.getMessage().contains(ClickHouseSinkConnector.CLIENT_VERSION));
    }

    @Test
    public void validateReportsClientCompressionErrors() {
        Map<String, String> props = baseProps();
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, "V1");
        props.put(ClickHouseSinkConfig.CLIENT_COMPRESSION, "true");

        Config config = new ClickHouseSinkConnector().validate(props);
        ConfigValue configValue = config.configValues().stream()
                .filter(value -> value.name().equals(ClickHouseSinkConfig.CLIENT_COMPRESSION))
                .findFirst()
                .orElseThrow();

        Assertions.assertFalse(configValue.errorMessages().isEmpty());
    }

    private Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnector.HOSTNAME, "localhost");
        props.put(ClickHouseSinkConnector.PORT, "8123");
        props.put(ClickHouseSinkConnector.DATABASE, "default");
        props.put(ClickHouseSinkConnector.USERNAME, "default");
        props.put(ClickHouseSinkConnector.PASSWORD, "");
        props.put(ClickHouseSinkConnector.SSL_ENABLED, "false");
        props.put(ClickHouseSinkConnector.CLIENT_VERSION, "V2");
        return props;
    }
}
