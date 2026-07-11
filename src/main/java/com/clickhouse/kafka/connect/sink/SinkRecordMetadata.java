package com.clickhouse.kafka.connect.sink;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class SinkRecordMetadata {
    private static final ClassValue<OriginalMetadataMethods> ORIGINAL_METADATA_METHODS =
            new ClassValue<>() {
                @Override
                protected OriginalMetadataMethods computeValue(Class<?> type) {
                    return OriginalMetadataMethods.from(type);
                }
            };

    private SinkRecordMetadata() {
    }

    static TopicPartition topicPartition(SinkRecord record) {
        String topic = topic(record);
        Integer partition = partition(record);
        if (topic == null || partition == null) {
            throw new ConnectException("Cannot determine the source Kafka topic and partition");
        }
        return new TopicPartition(topic, partition);
    }

    public static String topic(SinkRecord record) {
        OriginalMetadataMethods methods = ORIGINAL_METADATA_METHODS.get(record.getClass());
        return (String) invoke(methods.topic, record, record.topic());
    }

    static Integer partition(SinkRecord record) {
        OriginalMetadataMethods methods = ORIGINAL_METADATA_METHODS.get(record.getClass());
        return (Integer) invoke(methods.partition, record, record.kafkaPartition());
    }

    static long offset(SinkRecord record) {
        OriginalMetadataMethods methods = ORIGINAL_METADATA_METHODS.get(record.getClass());
        return (Long) invoke(methods.offset, record, record.kafkaOffset());
    }

    private static Object invoke(Method method, SinkRecord record, Object fallback) {
        if (method == null) {
            return fallback;
        }
        try {
            return method.invoke(record);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new ConnectException("Failed to read original Kafka record metadata", e);
        }
    }

    private static final class OriginalMetadataMethods {
        private final Method topic;
        private final Method partition;
        private final Method offset;

        private OriginalMetadataMethods(Method topic, Method partition, Method offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        private static OriginalMetadataMethods from(Class<?> type) {
            try {
                return new OriginalMetadataMethods(
                        type.getMethod("originalTopic"),
                        type.getMethod("originalKafkaPartition"),
                        type.getMethod("originalKafkaOffset"));
            } catch (NoSuchMethodException e) {
                // Original source metadata was added in Kafka 3.6. Older runtimes use current metadata.
                return new OriginalMetadataMethods(null, null, null);
            }
        }
    }
}
