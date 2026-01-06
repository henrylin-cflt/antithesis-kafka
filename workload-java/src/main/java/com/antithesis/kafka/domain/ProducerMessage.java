package com.antithesis.kafka.domain;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Iterator;
import java.util.Objects;

// Producer message wrapper
public class ProducerMessage {
    private final String id;
    private final long messageVersion;
    private final SequenceItem sequenceItem;

    public ProducerMessage(String id, long messageVersion, SequenceItem sequenceItem) {
        this.id = id;
        this.messageVersion = messageVersion;
        this.sequenceItem = sequenceItem;
    }

    public String getId() {
        return id;
    }

    public long getMessageVersion() {
        return messageVersion;
    }

    public SequenceItem getSequenceItem() {
        return sequenceItem;
    }

    @Override
    public String toString() {
        return String.format("%s:%d|%s", id, messageVersion, sequenceItem);
    }

    // Parse from string format "id:version|sequence"
    public static ProducerMessage fromString(String value) {
        String[] parts = value.split("\\|", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid producer message format: " + value);
        }

        String[] metadata = parts[0].split(":", 2);
        if (metadata.length != 2) {
            throw new IllegalArgumentException("Invalid producer message metadata: " + parts[0]);
        }

        String id = metadata[0];
        long version = Long.parseLong(metadata[1]);
        SequenceItem sequenceItem = SequenceItem.fromString(parts[1]);

        return new ProducerMessage(id, version, sequenceItem);
    }
}