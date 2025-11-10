package com.clickhouse.kafka.connect.util.jmx;

import com.clickhouse.kafka.connect.sink.ProxySinkTask;
import com.clickhouse.kafka.connect.sink.Version;
import com.codahale.metrics.ExponentialMovingAverages;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public class SinkTaskStatistics implements SinkTaskStatisticsMBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxySinkTask.class);

    private final AtomicLong receivedRecords;
    private final AtomicLong recordProcessingTime;
    private final AtomicLong taskProcessingTime;
    private final AtomicLong insertedRecords;
    private final AtomicLong failedRecords;
    private final AtomicLong receivedBatches;
    private final ExponentialMovingAverages receiveLag;
    private final int taskId;
    private final Map<String, TopicStatistics> topicStatistics;
    private final Deque<String> topicMBeans;


    public SinkTaskStatistics(int taskId) {
        this.taskId = taskId;
        this.topicMBeans = new ConcurrentLinkedDeque<>();
        this.topicStatistics = new ConcurrentHashMap<>();

        this.receivedRecords = new AtomicLong(0);
        this.recordProcessingTime = new AtomicLong(0);
        this.taskProcessingTime = new AtomicLong(0);
        this.insertedRecords = new AtomicLong(0);
        this.receivedBatches = new AtomicLong(0);
        this.failedRecords = new AtomicLong(0);
        this.receiveLag = new ExponentialMovingAverages();
    }

    public void registerMBean() {
        MBeanServerUtils.registerMBean(this, getMBeanName(taskId));
    }

    public void unregisterMBean() {
        MBeanServerUtils.unregisterMBean(getMBeanName(taskId));
        for (String topicMBean : topicMBeans) {
            MBeanServerUtils.unregisterMBean(topicMBean);
        }
    }

    @Override
    public long getReceivedRecords() {
        return receivedRecords.get();
    }

    @Override
    public long getRecordProcessingTime() {
        return recordProcessingTime.get();
    }

    @Override
    public long getTaskProcessingTime() {
        return taskProcessingTime.get();
    }

    @Override
    public long getInsertedRecords() {
        return insertedRecords.get();
    }

    @Override
    public long getFailedRecords() {
        return failedRecords.get();
    }

    @Override
    public long getReceivedBatches() {
        return receivedBatches.get();
    }

    @Override
    public long getMeanReceiveLag() {
        return Double.valueOf(receiveLag.getM1Rate()).longValue();
    }

    public void receivedRecords(final Collection<SinkRecord> records) {
        this.receivedRecords.addAndGet(records.size());
        this.receivedBatches.addAndGet(1);

        try {
            long receiveTime = System.currentTimeMillis();
            long eventTime = receiveTime;
            Optional<SinkRecord> first = records.stream().findFirst();
            if (first.isPresent()) {
                eventTime = first.get().timestamp();
            }
            receiveLag.update(receiveTime - eventTime);
            receiveLag.tickIfNecessary();
        } catch (Exception e) {
            LOGGER.warn("Failed to calculate receive lag", e);
        }
    }

    public void recordProcessingTime(ExecutionTimer timer) {
        this.recordProcessingTime.addAndGet(timer.nanosElapsed());
    }

    public void taskProcessingTime(ExecutionTimer timer) {
        this.taskProcessingTime.addAndGet(timer.nanosElapsed());
    }


    public void recordTopicStats(int n, String table, long eventReceiveLag) {
        insertedRecords.addAndGet(n);
        topicStatistics.computeIfAbsent(table, this::createTopicStatistics).recordsInserted(n);
        topicStatistics.computeIfAbsent(table, this::createTopicStatistics).batchInserted(1);
    }

    public void recordTopicStatsOnFailure(int n, String table, long eventReceiveLag) {
        failedRecords.addAndGet(n);
        topicStatistics.computeIfAbsent(table, this::createTopicStatistics).recordsFailed(n);
        topicStatistics.computeIfAbsent(table, this::createTopicStatistics).batchesFailed(1);
    }

    private TopicStatistics createTopicStatistics(String topic) {
        TopicStatistics topicStatistics = new TopicStatistics();
        String topicMBeanName = getTopicMBeanName(taskId, topic);
        topicMBeans.add(topicMBeanName);
        return MBeanServerUtils.registerMBean(topicStatistics, topicMBeanName);
    }

    public static String getMBeanName(int taskId) {
        return String.format("com.clickhouse:type=ClickHouseKafkaConnector,name=SinkTask%d,version=%s", taskId, Version.ARTIFACT_VERSION);
    }

    public static String getTopicMBeanName(int taskId, String topic) {
        return String.format("com.clickhouse:type=ClickHouseKafkaConnector,name=SinkTask%d,version=%s,topic=%s", taskId, Version.ARTIFACT_VERSION, topic);
    }

    public void insertTime(long t, String topic) {
        topicStatistics.computeIfAbsent(topic, this::createTopicStatistics).insertTime(t);
    }
}
