package com.antithesis.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;

import com.antithesis.sdk.Random;

// Workload configuration for the test framework
public class WorkloadConfig {
    private String bootstrapServers;
    private String consumerIsolationLevel = "read_committed";
    private boolean enableDebug = false;
    private boolean enableTransactions = false;
    private boolean strictMode = false;
    private String logDir = "./logs";
    
    private RandomRange topicCount = new RandomRange(1, 3);
    private RandomRange topicPartitionCount = new RandomRange(1, 6);
    private RandomRange producerCount = new RandomRange(1, 3);
    private RandomRange consumerGroupCount = new RandomRange(1, 3);
    private RandomRange consumerGroupMembersCount = new RandomRange(1, 7);
    
    private RandomRange producerStartupDelayMs = new RandomRange(0, 1000);
    private RandomRange producerUniqueSequenceCount = new RandomRange(1, 10);
    private RandomRange producerSequenceLength = new RandomRange(1, 10);
    private RandomRange producerSequenceDelayMs = new RandomRange(0, 50);
    
    private RandomRange consumerGroupMemberStartupDelayMs = new RandomRange(0, 1000);
    private RandomRange consumerGroupMemberProcessDelayMs = new RandomRange(10, 50);

    private RandomRange producerSendTimeoutMs = new RandomRange(500, 5000);
    private RandomRange messageTimeoutMs = new RandomRange(2000, 10000);
    private RandomRange transactionTimeoutMs = new RandomRange(1000, 12000);
    
    public WorkloadConfig() {}
    
    // Load configuration from file
    public static WorkloadConfig fromFile(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(path), WorkloadConfig.class);
    }

    // Getters and setters
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getConsumerIsolationLevel() {
        return consumerIsolationLevel;
    }

    public void setConsumerIsolationLevel(String consumerIsolationLevel) {
        this.consumerIsolationLevel = consumerIsolationLevel;
    }

    public boolean isEnableDebug() {
        return enableDebug;
    }

    public void setEnableDebug(boolean enableDebug) {
        this.enableDebug = enableDebug;
    }

    public boolean isEnableTransactions() {
        return enableTransactions;
    }

    public void setEnableTransactions(boolean enableTransactions) {
        this.enableTransactions = enableTransactions;
    }

    public RandomRange getProducerStartupDelayMs() {
        return producerStartupDelayMs;
    }

    public void setProducerStartupDelayMs(RandomRange producerStartupDelayMs) {
        this.producerStartupDelayMs = producerStartupDelayMs;
    }

    public RandomRange getProducerUniqueSequenceCount() {
        return producerUniqueSequenceCount;
    }

    public void setProducerUniqueSequenceCount(RandomRange producerUniqueSequenceCount) {
        this.producerUniqueSequenceCount = producerUniqueSequenceCount;
    }

    public RandomRange getProducerSequenceLength() {
        return producerSequenceLength;
    }

    public void setProducerSequenceLength(RandomRange producerSequenceLength) {
        this.producerSequenceLength = producerSequenceLength;
    }

    public RandomRange getProducerSequenceDelayMs() {
        return producerSequenceDelayMs;
    }

    public void setProducerSequenceDelayMs(RandomRange producerSequenceDelayMs) {
        this.producerSequenceDelayMs = producerSequenceDelayMs;
    }

    public RandomRange getConsumerGroupMemberStartupDelayMs() {
        return consumerGroupMemberStartupDelayMs;
    }

    public void setConsumerGroupMemberStartupDelayMs(RandomRange consumerGroupMemberStartupDelayMs) {
        this.consumerGroupMemberStartupDelayMs = consumerGroupMemberStartupDelayMs;
    }

    public RandomRange getConsumerGroupMemberProcessDelayMs() {
        return consumerGroupMemberProcessDelayMs;
    }

    public void setConsumerGroupMemberProcessDelayMs(RandomRange consumerGroupMemberProcessDelayMs) {
        this.consumerGroupMemberProcessDelayMs = consumerGroupMemberProcessDelayMs;
    }

    public RandomRange getProducerSendTimeoutMs() {
        return producerSendTimeoutMs;
    }

    public void setProducerSendTimeoutMs(RandomRange producerSendTimeoutMs) {
        this.producerSendTimeoutMs = producerSendTimeoutMs;
    }

    public RandomRange getMessageTimeoutMs() {
        return messageTimeoutMs;
    }

    public void setMessageTimeoutMs(RandomRange messageTimeoutMs) {
        this.messageTimeoutMs = messageTimeoutMs;
    }

    public RandomRange getTransactionTimeoutMs() {
        return transactionTimeoutMs;
    }

    public void setTransactionTimeoutMs(RandomRange transactionTimeoutMs) {
        this.transactionTimeoutMs = transactionTimeoutMs;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public void setStrictMode(boolean strictMode) {
        this.strictMode = strictMode;
    }

    public String getLogDir() {
        return logDir;
    }

    public void setLogDir(String logDir) {
        this.logDir = logDir;
    }

    public RandomRange getTopicCount() {
        return topicCount;
    }

    public void setTopicCount(RandomRange topicCount) {
        this.topicCount = topicCount;
    }

    public RandomRange getTopicPartitionCount() {
        return topicPartitionCount;
    }

    public void setTopicPartitionCount(RandomRange topicPartitionCount) {
        this.topicPartitionCount = topicPartitionCount;
    }

    public RandomRange getProducerCount() {
        return producerCount;
    }

    public void setProducerCount(RandomRange producerCount) {
        this.producerCount = producerCount;
    }

    public RandomRange getConsumerGroupCount() {
        return consumerGroupCount;
    }

    public void setConsumerGroupCount(RandomRange consumerGroupCount) {
        this.consumerGroupCount = consumerGroupCount;
    }

    public RandomRange getConsumerGroupMembersCount() {
        return consumerGroupMembersCount;
    }

    public void setConsumerGroupMembersCount(RandomRange consumerGroupMembersCount) {
        this.consumerGroupMembersCount = consumerGroupMembersCount;
    }

    // Helper class for random ranges
    public static class RandomRange {
        private int min;
        private int max;

        // Default constructor for Jackson
        public RandomRange() {
            this.min = 0;
            this.max = 0;
        }

        public RandomRange(int min, int max) {
            this.min = min;
            this.max = max;
        }

        public int getRandomValue() {
            if (min == max) {
                return min;
            }

            return (int) (max - (Random.getRandom() % (max - min + 1)));
        }

        public int getMin() {
            return min;
        }

        public void setMin(int min) {
            this.min = min;
        }

        public int getMax() {
            return max;
        }

        public void setMax(int max) {
            this.max = max;
        }
    }
}