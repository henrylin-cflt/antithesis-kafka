package com.antithesis.kafka.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.*;

// Producer configuration
public class ProducerKafkaConfig extends BaseKafkaConfig {
    
    public ProducerKafkaConfig() {
        super();
    }

    // Override set to return correct type for chaining
    public ProducerKafkaConfig set(String key, String value) {
        setProperty(key, value);
        return this;
    }

    public static ProducerKafkaConfig fromJson(String json) throws Exception {
        Map<String, String> props = parseJson(json);
        ProducerKafkaConfig config = new ProducerKafkaConfig();
        props.forEach(config::set);
        return config;
    }

    // Builder-style configuration methods
    public ProducerKafkaConfig withClientId(String clientId) {
        return set(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    }

    public ProducerKafkaConfig withBootstrapServers(String servers) {
        return set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    }

    public ProducerKafkaConfig withTransactionalId(String transactionalId) {
        return set(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    }

    public ProducerKafkaConfig withEnableIdempotence(boolean enable) {
        return set(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(enable));
    }

    public ProducerKafkaConfig withAcks(String acks) {
        return set(ProducerConfig.ACKS_CONFIG, acks);
    }

    public ProducerKafkaConfig withRetries(int retries) {
        return set(ProducerConfig.RETRIES_CONFIG, String.valueOf(retries));
    }
}
