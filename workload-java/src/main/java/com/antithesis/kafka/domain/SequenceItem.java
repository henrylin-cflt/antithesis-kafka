package com.antithesis.kafka.domain;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Iterator;
import java.util.Objects;

// Base class for sequence items
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = SequenceItem.NotStarted.class, name = "not_started"),
    @JsonSubTypes.Type(value = SequenceItem.Header.class, name = "header"),
    @JsonSubTypes.Type(value = SequenceItem.Body.class, name = "body"),
    @JsonSubTypes.Type(value = SequenceItem.Footer.class, name = "footer")
})
public abstract class SequenceItem {
    protected final String name;
    protected final long length;

    protected SequenceItem(String name, long length) {
        this.name = name;
        this.length = length;
    }

    public String getName() {
        return name;
    }

    public long getLength() {
        return length;
    }

    public abstract SequenceItem next();

    // Parse from string format "name:section:length[:index]"
    public static SequenceItem fromString(String value) {
        String[] parts = value.split(":");
        if (parts.length < 3) {
            throw new IllegalArgumentException("Invalid sequence format: " + value);
        }

        String name = parts[0];
        String section = parts[1];
        long length = Long.parseLong(parts[2]);

        switch (section) {
            case "header":
                return new Header(name, length);
            case "body":
                if (parts.length < 4) {
                    throw new IllegalArgumentException("Body sequence missing index: " + value);
                }
                long index = Long.parseLong(parts[3]);
                return new Body(name, length, index);
            case "footer":
                return new Footer(name, length);
            case "not-started":
                return new NotStarted(name, length);
            default:
                throw new IllegalArgumentException("Invalid sequence section: " + section);
        }
    }

    // NotStarted state
    public static class NotStarted extends SequenceItem {
        public NotStarted(String name, long length) {
            super(name, length);
        }

        @Override
        public SequenceItem next() {
            return new Header(name, length);
        }

        @Override
        public String toString() {
            return String.format("%s:not-started:%d", name, length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof NotStarted)) return false;
            NotStarted that = (NotStarted) o;
            return length == that.length && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, length);
        }
    }

    // Header state
    public static class Header extends SequenceItem {
        public Header(String name, long length) {
            super(name, length);
        }

        @Override
        public SequenceItem next() {
            if (length > 0) {
                return new Body(name, length, 0);
            } else {
                return new Footer(name, length);
            }
        }

        @Override
        public String toString() {
            return String.format("%s:header:%d", name, length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Header)) return false;
            Header header = (Header) o;
            return length == header.length && Objects.equals(name, header.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, length);
        }
    }

    // Body state with index
    public static class Body extends SequenceItem {
        private final long index;

        public Body(String name, long length, long index) {
            super(name, length);
            this.index = index;
        }

        public long getIndex() {
            return index;
        }

        @Override
        public SequenceItem next() {
            if (index + 1 < length) {
                return new Body(name, length, index + 1);
            } else {
                return new Footer(name, length);
            }
        }

        @Override
        public String toString() {
            return String.format("%s:body:%d:%d", name, length, index);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Body)) return false;
            Body body = (Body) o;
            return length == body.length && index == body.index && Objects.equals(name, body.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, length, index);
        }
    }

    // Footer state (terminal)
    public static class Footer extends SequenceItem {
        public Footer(String name, long length) {
            super(name, length);
        }

        @Override
        public SequenceItem next() {
            return null; // Terminal state
        }

        @Override
        public String toString() {
            return String.format("%s:footer:%d", name, length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Footer)) return false;
            Footer footer = (Footer) o;
            return length == footer.length && Objects.equals(name, footer.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, length);
        }
    }
}