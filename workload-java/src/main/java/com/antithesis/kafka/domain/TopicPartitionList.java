package com.antithesis.kafka.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

// Topic partition list
public class TopicPartitionList {
    private Map<String, Map<Integer, List<Long>>> partitions;

    public TopicPartitionList() {
        this.partitions = new TreeMap<>();
    }

    public TopicPartitionList(Map<String, Map<Integer, List<Long>>> partitions) {
        this.partitions = partitions;
    }

    public Map<String, Map<Integer, List<Long>>> getPartitions() {
        return partitions;
    }

    public void setPartitions(Map<String, Map<Integer, List<Long>>> partitions) {
        this.partitions = partitions;
    }

    // Parse from JSON string
    public static TopicPartitionList fromJson(String json) throws Exception {
        com.fasterxml.jackson.databind.ObjectMapper mapper = 
            new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(json);
        
        Map<String, Map<Integer, List<Long>>> partitions = new TreeMap<>();
        
        root.fields().forEachRemaining(topicEntry -> {
            String topicName = topicEntry.getKey();
            Map<Integer, List<Long>> topicPartitions = new TreeMap<>();
            
            topicEntry.getValue().fields().forEachRemaining(partitionEntry -> {
                int partition = Integer.parseInt(partitionEntry.getKey());
                List<Long> offsets = new ArrayList<>();
                
                partitionEntry.getValue().forEach(offset -> 
                    offsets.add(offset.asLong()));
                
                topicPartitions.put(partition, offsets);
            });
            
            partitions.put(topicName, topicPartitions);
        });
        
        return new TopicPartitionList(partitions);
    }

    // Convert from Kafka TopicPartition collection
    public static TopicPartitionList fromKafka(
            Collection<org.apache.kafka.common.TopicPartition> kafkaPartitions) {
        Map<String, Map<Integer, List<Long>>> partitions = new TreeMap<>();
        
        for (org.apache.kafka.common.TopicPartition tp : kafkaPartitions) {
            partitions.computeIfAbsent(tp.topic(), k -> new TreeMap<>())
                     .computeIfAbsent(tp.partition(), k -> new ArrayList<>())
                     .add(-1L); // Kafka doesn't provide offset in assignment
        }
        
        return new TopicPartitionList(partitions);
    }
}
