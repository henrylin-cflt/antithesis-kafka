package com.antithesis.kafka.domain;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Iterator;
import java.util.Objects;

// Sequence iterator for generating test sequences
public class Sequence implements Iterator<SequenceItem> {
    private SequenceItem currentState;

    public Sequence(String name, long length) {
        this.currentState = new SequenceItem.NotStarted(name, length);
    }

    @Override
    public boolean hasNext() {
        return currentState != null && currentState.next() != null;
    }

    @Override
    public SequenceItem next() {
        if (currentState == null) {
            throw new IllegalStateException("No more elements in sequence");
        }
        currentState = currentState.next();
        return currentState;
    }
}