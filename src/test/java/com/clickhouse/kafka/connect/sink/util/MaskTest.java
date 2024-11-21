package com.clickhouse.kafka.connect.sink.util;

import com.clickhouse.kafka.connect.util.Mask;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MaskTest {

    @Test
    @DisplayName("MaskPasswordBiggerThan6")
    public void MaskPasswordBiggerThan6() {
        String password = "DATBwWKXvYQnce";
        String maskedPassword = "***BwWKXvYQ***";
        assertEquals(maskedPassword, Mask.passwordMask(password));
    }

    @Test
    @DisplayName("MaskPasswordSmallerThan6")
    public void MaskPasswordSmallerThan6() {
        String password = "DATBw";
        String maskedPassword = "*****";
        assertEquals(maskedPassword, Mask.passwordMask(password));
    }


}
