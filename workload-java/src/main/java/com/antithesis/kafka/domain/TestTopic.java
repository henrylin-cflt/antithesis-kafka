package com.antithesis.kafka.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

// Test topic configuration
public class TestTopic {
    private String name;
    private int numPartitions;
    private int replicationFactor;

    public TestTopic() {}

    public TestTopic(String name, int numPartitions, int replicationFactor) {
        this.name = name;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public int getNumPartitions() { return numPartitions; }
    public void setNumPartitions(int numPartitions) { this.numPartitions = numPartitions; }
    public int getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(int replicationFactor) { this.replicationFactor = replicationFactor; }

    @Override
    public String toString() {
        return String.format("TestTopic{name='%s', partitions=%d, replication=%d}",
            name, numPartitions, replicationFactor);
    }
}