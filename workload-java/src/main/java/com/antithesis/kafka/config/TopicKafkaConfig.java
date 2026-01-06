package com.antithesis.kafka.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.*;

// Topic configuration
public class TopicKafkaConfig extends BaseKafkaConfig {
    
    public TopicKafkaConfig() {
        super();
    }

    // Override set to return correct type for chaining
    public TopicKafkaConfig set(String key, String value) {
        setProperty(key, value);
        return this;
    }

    public static TopicKafkaConfig fromJson(String json) throws Exception {
        Map<String, String> props = parseJson(json);
        TopicKafkaConfig config = new TopicKafkaConfig();
        props.forEach(config::set);
        return config;
    }

    public TopicKafkaConfig withMinInSyncReplicas(int replicas) {
        return set("min.insync.replicas", String.valueOf(replicas));
    }

    public TopicKafkaConfig withFlushMessages(int messages) {
        return set("flush.messages", String.valueOf(messages));
    }

    public Map<String, String> toConfigMap() {
        return new TreeMap<>(properties);
    }
}