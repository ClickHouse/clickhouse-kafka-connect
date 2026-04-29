package com.clickhouse.kafka.connect.sink.helper;

public enum ClickHouseCloudCommand {
    START("start"),
    STOP("stop");

    private final String command;

    ClickHouseCloudCommand(String command) {
        this.command = command;
    }

    public String toRequestBody() {
        return "{\"command\": \"" + command + "\"}";
    }
}
