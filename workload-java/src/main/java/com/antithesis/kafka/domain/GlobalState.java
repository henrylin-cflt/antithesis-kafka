package com.antithesis.kafka.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


// Global state tracking
public class GlobalState {
    private final AtomicBoolean allProducersCompleted;
    private final AtomicBoolean allMessagesReadByObserver;
    private final Map<String, Map<Integer, Long>> topicPartitionOffsets;
    private final Map<String, Map<String, Map<Integer, Long>>> consumerGroupsReadOffsets;

    public GlobalState() {
        this.allProducersCompleted = new AtomicBoolean(false);
        this.allMessagesReadByObserver = new AtomicBoolean(false);
        this.topicPartitionOffsets = new ConcurrentHashMap<>();
        this.consumerGroupsReadOffsets = new ConcurrentHashMap<>();
    }

    public AtomicBoolean getAllProducersCompleted() { return allProducersCompleted; }
    public AtomicBoolean getAllMessagesReadByObserver() { return allMessagesReadByObserver; }
    public Map<String, Map<Integer, Long>> getTopicPartitionOffsets() { return topicPartitionOffsets; }
    public Map<String, Map<String, Map<Integer, Long>>> getConsumerGroupsReadOffsets() { return consumerGroupsReadOffsets; }
}

