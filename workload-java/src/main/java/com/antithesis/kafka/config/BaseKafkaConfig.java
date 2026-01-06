package com.antithesis.kafka.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.*;

// Base configuration class
public abstract class BaseKafkaConfig {
    protected final Map<String, String> properties;

    public BaseKafkaConfig() {
        this.properties = new TreeMap<>();
    }

    protected void setProperty(String key, String value) {
        properties.put(key, value);
    }

    public Map<String, String> getProperties() {
        return new TreeMap<>(properties);
    }

    public Properties toProperties() {
        Properties props = new Properties();
        props.putAll(properties);
        return props;
    }

    // Helper method for subclasses to parse JSON
    protected static Map<String, String> parseJson(String json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);
        Map<String, String> config = new TreeMap<>();
        
        root.fields().forEachRemaining(entry -> 
            config.put(entry.getKey(), entry.getValue().asText()));
        
        return config;
    }
}
