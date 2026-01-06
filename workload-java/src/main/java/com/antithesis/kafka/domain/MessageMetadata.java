package com.antithesis.kafka.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

// Message metadata
public class MessageMetadata implements Comparable<MessageMetadata> {
    @JsonProperty("topic_name")
    private String topicName;
    
    @JsonProperty("topic_partition")
    private int topicPartition;
    
    @JsonProperty("topic_partition_offset")
    private long topicPartitionOffset;

    public MessageMetadata() {}

    public MessageMetadata(String topicName, int topicPartition, long topicPartitionOffset) {
        this.topicName = topicName;
        this.topicPartition = topicPartition;
        this.topicPartitionOffset = topicPartitionOffset;
    }

    public String getTopicName() { return topicName; }
    public void setTopicName(String topicName) { this.topicName = topicName; }
    public int getTopicPartition() { return topicPartition; }
    public void setTopicPartition(int topicPartition) { this.topicPartition = topicPartition; }
    public long getTopicPartitionOffset() { return topicPartitionOffset; }
    public void setTopicPartitionOffset(long topicPartitionOffset) { this.topicPartitionOffset = topicPartitionOffset; }

    @Override
    public String toString() {
        return String.format("topic = '%s', partition = %d, offset = %d", 
            topicName, topicPartition, topicPartitionOffset);
    }

    @Override
    public int compareTo(MessageMetadata other) {
        int topicCmp = this.topicName.compareTo(other.topicName);
        if (topicCmp != 0) return topicCmp;
        
        int partitionCmp = Integer.compare(this.topicPartition, other.topicPartition);
        if (partitionCmp != 0) return partitionCmp;
        
        return Long.compare(this.topicPartitionOffset, other.topicPartitionOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageMetadata that = (MessageMetadata) o;
        return topicPartition == that.topicPartition &&
               topicPartitionOffset == that.topicPartitionOffset &&
               Objects.equals(topicName, that.topicName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName, topicPartition, topicPartitionOffset);
    }
}