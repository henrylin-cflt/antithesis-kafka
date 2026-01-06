package com.antithesis.kafka.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.*;

// Consumer configuration
public class ConsumerKafkaConfig extends BaseKafkaConfig {
    
    public ConsumerKafkaConfig() {
        super();
    }

    // Override set to return correct type for chaining
    public ConsumerKafkaConfig set(String key, String value) {
        setProperty(key, value);
        return this;
    }

    public static ConsumerKafkaConfig fromJson(String json) throws Exception {
        Map<String, String> props = parseJson(json);
        ConsumerKafkaConfig config = new ConsumerKafkaConfig();
        props.forEach(config::set);
        return config;
    }

    // Builder-style configuration methods
    public ConsumerKafkaConfig withClientId(String clientId) {
        return set(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    }

    public ConsumerKafkaConfig withGroupId(String groupId) {
        return set(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }

    public ConsumerKafkaConfig withBootstrapServers(String servers) {
        return set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    }

    public ConsumerKafkaConfig withAutoOffsetReset(String reset) {
        return set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, reset);
    }

    public ConsumerKafkaConfig withEnableAutoCommit(boolean enable) {
        return set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(enable));
    }

    public ConsumerKafkaConfig withIsolationLevel(String level) {
        return set(ConsumerConfig.ISOLATION_LEVEL_CONFIG, level);
    }
}