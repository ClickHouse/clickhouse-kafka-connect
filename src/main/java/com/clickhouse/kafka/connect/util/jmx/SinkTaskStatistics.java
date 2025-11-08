package com.clickhouse.kafka.connect.util.jmx;

import com.clickhouse.kafka.connect.sink.Version;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public class SinkTaskStatistics implements SinkTaskStatisticsMBean {
    private final AtomicLong receivedRecords;
    private final AtomicLong recordProcessingTime;
    private final AtomicLong taskProcessingTime;
    private final AtomicLong insertedRecords;
    private final Map<String, TopicStatistics> topicStatistics;
    private final int taskId;

    private final Deque<String> topicMBeans;

    public SinkTaskStatistics(int taskId) {
        this.taskId = taskId;
        this.topicMBeans = new ConcurrentLinkedDeque<>();
        this.receivedRecords = new AtomicLong(0);
        this.recordProcessingTime = new AtomicLong(0);
        this.taskProcessingTime = new AtomicLong(0);
        this.insertedRecords = new AtomicLong(0);
        this.topicStatistics = new ConcurrentHashMap<>();
    }

    public void registerMBean() {
        MBeanServerUtils.registerMBean(this, getMBeanNAme(taskId));
    }

    public void unregisterMBean() {
        MBeanServerUtils.unregisterMBean(getMBeanNAme(taskId));
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

    public void receivedRecords(final int n) {
        this.receivedRecords.addAndGet(n);
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
        topicStatistics.computeIfAbsent(table, this::createTopicStatistics).eventReceiveLag(eventReceiveLag);
    }

    public void recordTopicStatsOnFailure(int n, String table, long eventReceiveLag) {
        insertedRecords.addAndGet(n);
        topicStatistics.computeIfAbsent(table, this::createTopicStatistics).recordsFailed(n);
        topicStatistics.computeIfAbsent(table, this::createTopicStatistics).batchesFailed(1);
        topicStatistics.computeIfAbsent(table, this::createTopicStatistics).eventReceiveLag(eventReceiveLag);
    }

    private TopicStatistics createTopicStatistics(String topic) {
        TopicStatistics topicStatistics = new TopicStatistics();
        String topicMBeanName = getTopicMBeanName(taskId, topic);
        topicMBeans.add(topicMBeanName);
        return MBeanServerUtils.registerMBean(topicStatistics, topicMBeanName);
    }

    public static String getMBeanNAme(int taskId) {
        return String.format("com.clickhouse:type=ClickHouseKafkaConnector,name=SinkTask%d,version=%s", taskId, Version.ARTIFACT_VERSION);
    }

    public static String getTopicMBeanName(int taskId, String topic) {
        return String.format("com.clickhouse:type=ClickHouseKafkaConnector,name=SinkTask%d,version=%s,topic=%s", taskId, Version.ARTIFACT_VERSION, topic);
    }

    public void insertTime(long t, String topic) {
        topicStatistics.computeIfAbsent(topic, this::createTopicStatistics).insertTime(t);
    }
}
