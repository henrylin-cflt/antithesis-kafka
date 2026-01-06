package com.antithesis.kafka.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

// Complete message
public class Message {
    private MessageMetadata metadata;
    private MessageData data;

    public Message() {}

    public Message(MessageMetadata metadata, MessageData data) {
        this.metadata = metadata;
        this.data = data;
    }

    public MessageMetadata getMetadata() { return metadata; }
    public void setMetadata(MessageMetadata metadata) { this.metadata = metadata; }
    public MessageData getData() { return data; }
    public void setData(MessageData data) { this.data = data; }

    @Override
    public String toString() {
        return metadata + ", " + data;
    }
}