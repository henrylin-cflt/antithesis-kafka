package com.antithesis.kafka.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

// Message data
public class MessageData {
    @JsonProperty("message_key")
    private String key;
    
    @JsonProperty("message_payload")
    private String payload;

    public MessageData() {}

    public MessageData(String key, String payload) {
        this.key = key;
        this.payload = payload;
    }

    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("payload = '").append(payload).append("'");
        if (key != null) {
            sb.append(", key = '").append(key).append("'");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageData that = (MessageData) o;
        return Objects.equals(key, that.key) && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, payload);
    }
}
