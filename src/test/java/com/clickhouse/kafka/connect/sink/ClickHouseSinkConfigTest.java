package com.clickhouse.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClickHouseSinkConfigTest {

    private static ClickHouseSinkConfig configWith(String additionalRetriableErrorCodes) {
        Map<String, String> props = new HashMap<>();
        if (additionalRetriableErrorCodes != null) {
            props.put(ClickHouseSinkConfig.ADDITIONAL_RETRIABLE_ERROR_CODES, additionalRetriableErrorCodes);
        }
        return new ClickHouseSinkConfig(props);
    }

    @Test
    @DisplayName("additionalRetriableErrorCodes defaults to empty")
    public void additionalRetriableErrorCodesDefault() {
        assertTrue(configWith(null).getAdditionalRetriableErrorCodes().isEmpty());
        assertTrue(configWith("").getAdditionalRetriableErrorCodes().isEmpty());
        assertTrue(configWith("  ").getAdditionalRetriableErrorCodes().isEmpty());
    }

    @Test
    @DisplayName("additionalRetriableErrorCodes parses, trims and de-duplicates")
    public void additionalRetriableErrorCodesParsed() {
        assertEquals(Set.of(216, 999), configWith("216,999").getAdditionalRetriableErrorCodes());
        assertEquals(Set.of(216), configWith(" 216 , 216 , ").getAdditionalRetriableErrorCodes());
    }

    @ParameterizedTest(name = "value {0}")
    @ValueSource(strings = {"abc", "21.6", "-1", "0", "99999999999"})
    @DisplayName("additionalRetriableErrorCodes rejects invalid values in both the constructor and the validator")
    public void additionalRetriableErrorCodesInvalid(String value) {
        assertThrows(IllegalArgumentException.class, () -> configWith(value));
        ClickHouseSinkConfig.ErrorCodeListValidator validator = new ClickHouseSinkConfig.ErrorCodeListValidator();
        assertThrows(ConfigException.class, () -> validator.ensureValid(ClickHouseSinkConfig.ADDITIONAL_RETRIABLE_ERROR_CODES, Arrays.asList(value.split(","))));
    }

    @Test
    @DisplayName("additionalRetriableErrorCodes validator accepts the Connect-parsed list form")
    public void additionalRetriableErrorCodesValidatorAcceptsList() {
        ClickHouseSinkConfig.ErrorCodeListValidator validator = new ClickHouseSinkConfig.ErrorCodeListValidator();
        validator.ensureValid(ClickHouseSinkConfig.ADDITIONAL_RETRIABLE_ERROR_CODES, Arrays.asList("216", " 999"));
        validator.ensureValid(ClickHouseSinkConfig.ADDITIONAL_RETRIABLE_ERROR_CODES, Arrays.asList());
    }
}
