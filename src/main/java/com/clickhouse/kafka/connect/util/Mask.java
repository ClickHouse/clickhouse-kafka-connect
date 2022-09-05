package com.clickhouse.kafka.connect.util;

public class Mask {

    public static String passwordMask(String password) {
        if (password.length() <= 6) {
            return "*".repeat(password.length());
        }
        String tmpPassword = "***" + password.substring(3, password.length() - 3) + "***";
        return tmpPassword;
    }
}
