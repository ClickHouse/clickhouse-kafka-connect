package com.clickhouse.kafka.connect.sink.helper;

import com.clickhouse.client.api.ServerException;

import java.lang.reflect.InvocationTargetException;

/**
 * Builds client V2 {@link ServerException}s for tests, independent of the clickhouse-java version on the classpath.
 *
 * <p>clickhouse-java 0.9.6 (the version in the catalog) only has {@code ServerException(int, String, int)}.
 * From 0.9.7 on (ClickHouse/clickhouse-java#2673) only {@code ServerException(int, String, int, String queryId)}
 * exists, so a direct constructor call compiles against one of them and breaks the {@code test-head.yaml}
 * build against the other. Once the catalog moves to 0.9.7 or later, replace this with a direct call.
 */
public final class ServerExceptions {

    private ServerExceptions() {
    }

    /**
     * @param code                  ClickHouse server error code
     * @param message               exception message
     * @param transportProtocolCode transport status, e.g. the HTTP status
     */
    public static ServerException of(int code, String message, int transportProtocolCode) {
        try {
            try {
                // clickhouse-java 0.9.7 and later
                return ServerException.class
                        .getConstructor(int.class, String.class, int.class, String.class)
                        .newInstance(code, message, transportProtocolCode, null);
            } catch (NoSuchMethodException e) {
                // clickhouse-java 0.9.6 and earlier
                return ServerException.class
                        .getConstructor(int.class, String.class, int.class)
                        .newInstance(code, message, transportProtocolCode);
            }
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("No known ServerException constructor in this clickhouse-java version", e);
        }
    }
}
