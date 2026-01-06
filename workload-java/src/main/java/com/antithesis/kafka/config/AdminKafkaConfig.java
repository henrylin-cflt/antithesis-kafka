package com.antithesis.kafka.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.*;

// Admin client configuration
public class AdminKafkaConfig extends BaseKafkaConfig {
    
    public AdminKafkaConfig() {
        super();
    }

    // Override set to return correct type for chaining
    public AdminKafkaConfig set(String key, String value) {
        setProperty(key, value);
        return this;
    }

    public AdminKafkaConfig withBootstrapServers(String servers) {
        return set(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    }

    public AdminKafkaConfig withRequestTimeout(int timeoutMs) {
        return set(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(timeoutMs));
    }
}