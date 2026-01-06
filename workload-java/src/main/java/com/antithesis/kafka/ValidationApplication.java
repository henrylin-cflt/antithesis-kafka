package com.antithesis.kafka.validation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import com.antithesis.sdk.Assert;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Validation application that reads workload logs and validates correctness.
 */
public class ValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(ValidationApplication.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: ValidationApplication <log-dir-path>");
            System.exit(1);
        }

        String logDirPath = args[0];
        Path logDir = Paths.get(logDirPath);

        if (!Files.exists(logDir) || !Files.isDirectory(logDir)) {
            throw new IllegalArgumentException("Invalid log directory: " + logDirPath);
        }

        // Find all workload log files
        File[] logFiles = logDir.toFile().listFiles((dir, name) -> 
            name.startsWith("kafka-workload") && name.endsWith(".log"));

        if (logFiles == null || logFiles.length == 0) {
            log.info("No workload log files found in {}", logDirPath);
            return;
        }

        // Validate each workload log
        for (File logFile : logFiles) {
            validateWorkload(logFile);
        }
    }

    private static void validateWorkload(File logFile) throws IOException {
        log.info("Verifying workload log: {}", logFile.getName());

        // Create validators
        List<TestValidator> validators = Arrays.asList(
            new ProducerMessageOrderingValidator(),
            new ProducerIdempotenceValidator(),
            new ConsumerMessageOrderingValidator(),
            new MessageIntegrityValidator(), 
            new MessageCompletenessValidator()
        );

        WorkloadLogReader logReader = new WorkloadLogReader(logFile);
        int lineNumber = 0;
        int checkedLines = 0;
        boolean workloadEnded = false;

        // Read and validate each line
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                checkedLines++;

                // Parse JSON log line
                LogEvent event = LogEvent.fromJson(line);
                
                if (event != null) {
                    // Run all validators
                    for (TestValidator validator : validators) {
                        validator.validateEvent(lineNumber, event);
                    }

                    // Check if workload ended
                    if ("workload_ended".equals(event.getEventType())) {
                        workloadEnded = true;
                    }
                }
            }
        }

        // Report validation results
        for (TestValidator validator : validators) {
            List<ValidationFailure> failures = validator.getFailures();
            if (!failures.isEmpty()) {
                log.error("Validator {} found {} failures:", 
                    validator.getName(), failures.size());
                for (ValidationFailure failure : failures) {
                    log.error("  Line {}: [{}] {}", 
                        failure.lineNumber, failure.code, failure.error);
                }
            } else {
                log.info("Validator {} passed", validator.getName());
            }
        }

        log.info("Verified workload log: {}, checked {} lines", 
            logFile.getName(), checkedLines);

        // Rename verified log file
        if (workloadEnded) {
            File verifiedFile = new File(logFile.getAbsolutePath() + ".verified");
            if (logFile.renameTo(verifiedFile)) {
                log.info("Renamed to {}", verifiedFile.getName());
            }
        }
    }
}

/**
 * Interface for test validators
 */
interface TestValidator {
    String getName();
    void validateEvent(int lineNumber, LogEvent event);
    List<ValidationFailure> getFailures();
}

/**
 * Validation failure record
 */
class ValidationFailure {
    final int lineNumber;
    final String code;
    final String error;

    ValidationFailure(int lineNumber, String code, String error) {
        this.lineNumber = lineNumber;
        this.code = code;
        this.error = error;
    }
}

/**
 * Log event from workload
 */
class LogEvent {
    private String timestamp;
    private String eventType;
    private Map<String, Object> fields;

    public static LogEvent fromJson(String json) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = 
                new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode node = mapper.readTree(json);
            
            LogEvent event = new LogEvent();
            event.timestamp = node.has("timestamp") ? node.get("timestamp").asText() : null;
            event.eventType = node.has("event") ? node.get("event").asText() : null;
            event.fields = new HashMap<>();
            
            node.fields().forEachRemaining(entry -> 
                event.fields.put(entry.getKey(), entry.getValue()));
            
            return event;
        } catch (Exception e) {
            return null;
        }
    }

    public String getEventType() { return eventType; }
    public String getTimestamp() { return timestamp; }
    public Map<String, Object> getFields() { return fields; }
    
    public String getField(String name) {
        Object value = fields.get(name);
        return value != null ? value.toString() : null;
    }
}

/**
 * Workload log reader utility
 */
class WorkloadLogReader {
    private final File logFile;

    WorkloadLogReader(File logFile) {
        this.logFile = logFile;
    }

    public File getLogFile() {
        return logFile;
    }
}

/**
 * Example validator: Producer Message Ordering
 * Ensures messages from a producer are in sequence order
 */
class ProducerMessageOrderingValidator implements TestValidator {
    private final List<ValidationFailure> failures = new ArrayList<>();
    private final Map<String, Long> lastVersionByProducer = new HashMap<>();

    @Override
    public String getName() {
        return "ProducerMessageOrderingValidator";
    }

    @Override
    public void validateEvent(int lineNumber, LogEvent event) {
        if (!"message_write_succeeded".equals(event.getEventType())) {
            return;
        }

        String producerId = event.getField("producer_id");
        String payload = event.getField("message_payload");
        
        if (producerId == null || payload == null) {
            return;
        }

        try {
            // Parse message format: "producerId:version|sequence"
            String[] parts = payload.split("\\|");
            if (parts.length > 0) {
                String[] metadata = parts[0].split(":");
                if (metadata.length >= 2) {
                    long version = Long.parseLong(metadata[1]);
                    
                    Long lastVersion = lastVersionByProducer.get(producerId);
                    if (lastVersion != null && version <= lastVersion) {
                        // TODO: this is wrong, should be looking at offset...
                        failures.add(new ValidationFailure(
                            lineNumber,
                            "OUT_OF_ORDER",
                            String.format("Producer %s version %d came after %d", 
                                producerId, version, lastVersion)
                        ));
                    }
                    
                    lastVersionByProducer.put(producerId, version);
                }
            }
        } catch (Exception e) {
            // Invalid message format
        }
    }

    @Override
    public List<ValidationFailure> getFailures() {
        return failures;
    }
}

/**
 * Example validator: Producer Idempotence
 * Ensures no duplicate messages from producers
 */
class ProducerIdempotenceValidator implements TestValidator {
    private final List<ValidationFailure> failures = new ArrayList<>();
    private final Set<String> seenMessages = new HashSet<>();

    @Override
    public String getName() {
        return "ProducerIdempotenceValidator";
    }

    @Override
    public void validateEvent(int lineNumber, LogEvent event) {
        if (!"message_write_succeeded".equals(event.getEventType())) {
            return;
        }

        String topic = event.getField("topic_name");
        String partition = event.getField("topic_partition");
        String offset = event.getField("topic_partition_offset");
        
        if (topic == null || partition == null || offset == null) {
            return;
        }

        String messageId = String.format("%s:%s:%s", topic, partition, offset);
        
        if (seenMessages.contains(messageId)) {
            failures.add(new ValidationFailure(
                lineNumber,
                "DUPLICATE",
                String.format("Duplicate message at %s", messageId)
            ));
        }
        
        seenMessages.add(messageId);
    }

    @Override
    public List<ValidationFailure> getFailures() {
        return failures;
    }
}

/**
 * Example validator: Consumer Message Ordering
 * Ensures consumers read messages in offset order per partition
 */
class ConsumerMessageOrderingValidator implements TestValidator {
    private final List<ValidationFailure> failures = new ArrayList<>();
    private final Map<String, Long> lastOffsetByPartition = new HashMap<>();

    @Override
    public String getName() {
        return "ConsumerMessageOrderingValidator";
    }

    @Override
    public void validateEvent(int lineNumber, LogEvent event) {
        if (!"message_read_succeeded".equals(event.getEventType())) {
            return;
        }

        String topic = event.getField("topic_name");
        String partition = event.getField("topic_partition");
        String offsetStr = event.getField("topic_partition_offset");
        
        if (topic == null || partition == null || offsetStr == null) {
            return;
        }

        try {
            long offset = Long.parseLong(offsetStr);
            String partitionKey = topic + ":" + partition;
            
            Long lastOffset = lastOffsetByPartition.get(partitionKey);
            if (lastOffset != null && offset <= lastOffset) {
                failures.add(new ValidationFailure(
                    lineNumber,
                    "OUT_OF_ORDER",
                    String.format("Partition %s offset %d came after %d", 
                        partitionKey, offset, lastOffset)
                ));
            }
            
            lastOffsetByPartition.put(partitionKey, offset);
        } catch (NumberFormatException e) {
            // Invalid offset format
        }
    }

    @Override
    public List<ValidationFailure> getFailures() {
        return failures;
    }
}

/**
 * Example validator: Message Integrity
 * Ensures message payloads are valid and parseable
 */
class MessageIntegrityValidator implements TestValidator {
    private final List<ValidationFailure> failures = new ArrayList<>();

    @Override
    public String getName() {
        return "MessageIntegrityValidator";
    }

    @Override
    public void validateEvent(int lineNumber, LogEvent event) {
        String eventType = event.getEventType();
        if (!"message_write_succeeded".equals(eventType) && 
            !"message_read_succeeded".equals(eventType)) {
            return;
        }

        String payload = event.getField("message_payload");
        if (payload == null) {
            failures.add(new ValidationFailure(
                lineNumber,
                "NULL_PAYLOAD",
                "Message has null payload"
            ));
            return;
        }

        // Validate message format: "producerId:version|sequenceName:section:length[:index]"
        String[] parts = payload.split("\\|");
        if (parts.length != 2) {
            failures.add(new ValidationFailure(
                lineNumber,
                "INVALID_FORMAT",
                String.format("Invalid message format: %s", payload)
            ));
        }
    }

    @Override
    public List<ValidationFailure> getFailures() {
        return failures;
    }
}

/**
 * Comprehensive validator: Message Read/Write Completeness
 * Tracks all written and read messages, validates at workload end
 */
class MessageCompletenessValidator implements TestValidator {
    private final List<ValidationFailure> failures = new ArrayList<>();
    
    // Structure: topic -> partition -> offset -> ReadWriteState
    private final Map<String, Map<Integer, Map<Long, ReadWriteState>>> messages = new HashMap<>();
    
    private boolean workloadEnded = false;

    private static class ReadWriteState {
        MessageLocation readLocation;
        MessageLocation writeLocation;
        
        ReadWriteState() {}
    }
    
    private static class MessageLocation {
        final int lineNumber;
        final String timestamp;
        final String messageHash;
        
        MessageLocation(int lineNumber, String timestamp, String messageHash) {
            this.lineNumber = lineNumber;
            this.timestamp = timestamp;
            this.messageHash = messageHash;
        }
        
        String location() {
            return String.format("line=%d, timestamp='%s'", lineNumber, timestamp);
        }
    }

    @Override
    public String getName() {
        return "MessageCompletenessValidator";
    }

    @Override
    public void validateEvent(int lineNumber, LogEvent event) {
        String eventType = event.getEventType();
        
        if ("message_write_succeeded".equals(eventType)) {
            handleMessageWrite(lineNumber, event);
        } else if ("message_read_succeeded".equals(eventType)) {
            handleMessageRead(lineNumber, event);
        } else if ("workload_ended".equals(eventType)) {
            workloadEnded = true;
            validateCompleteness();
        }
    }
    
    private void handleMessageWrite(int lineNumber, LogEvent event) {
        String topic = event.getField("topic_name");
        String partitionStr = event.getField("topic_partition");
        String offsetStr = event.getField("topic_partition_offset");
        String payload = event.getField("message_payload");
        String timestamp = event.getTimestamp();
        
        if (topic == null || partitionStr == null || offsetStr == null || payload == null) {
            return;
        }
        
        try {
            int partition = Integer.parseInt(partitionStr);
            long offset = Long.parseLong(offsetStr);
            
            ReadWriteState state = messages
                .computeIfAbsent(topic, k -> new HashMap<>())
                .computeIfAbsent(partition, k -> new HashMap<>())
                .computeIfAbsent(offset, k -> new ReadWriteState());
            
            if (state.writeLocation != null) {
                // Duplicate write detected
                failures.add(new ValidationFailure(
                    lineNumber,
                    "DUPLICATE_WRITE",
                    String.format("Message at %s:%d:%d was written twice. First at %s, second at line %d",
                        topic, partition, offset, state.writeLocation.location(), lineNumber)
                ));
            } else {
                state.writeLocation = new MessageLocation(lineNumber, timestamp, payload);
            }
            
        } catch (NumberFormatException e) {
            // Invalid format
        }
    }
    
    private void handleMessageRead(int lineNumber, LogEvent event) {
        String topic = event.getField("topic_name");
        String partitionStr = event.getField("topic_partition");
        String offsetStr = event.getField("topic_partition_offset");
        String payload = event.getField("message_payload");
        String timestamp = event.getTimestamp();
        
        if (topic == null || partitionStr == null || offsetStr == null || payload == null) {
            return;
        }
        
        try {
            int partition = Integer.parseInt(partitionStr);
            long offset = Long.parseLong(offsetStr);
            
            ReadWriteState state = messages
                .computeIfAbsent(topic, k -> new HashMap<>())
                .computeIfAbsent(partition, k -> new HashMap<>())
                .computeIfAbsent(offset, k -> new ReadWriteState());
            
            if (state.readLocation != null) {
                // Duplicate read detected (same consumer reading twice or multiple consumers)
                // This might be OK depending on your semantics, but we'll flag it
                failures.add(new ValidationFailure(
                    lineNumber,
                    "DUPLICATE_READ",
                    String.format("Message at %s:%d:%d was read multiple times. First at %s, again at line %d",
                        topic, partition, offset, state.readLocation.location(), lineNumber)
                ));
            } else {
                state.readLocation = new MessageLocation(lineNumber, timestamp, payload);
                
                // Validate read matches write (if write already happened)
                if (state.writeLocation != null) {
                    if (!state.writeLocation.messageHash.equals(payload)) {
                        failures.add(new ValidationFailure(
                            lineNumber,
                            "PAYLOAD_MISMATCH",
                            String.format("Message at %s:%d:%d read payload doesn't match written payload. Written at %s, read at line %d",
                                topic, partition, offset, state.writeLocation.location(), lineNumber)
                        ));
                    }
                }
            }
            
        } catch (NumberFormatException e) {
            // Invalid format
        }
    }
    
    private void validateCompleteness() {
        if (!workloadEnded) {
            return;
        }
        
        // Check all messages for completeness
        for (Map.Entry<String, Map<Integer, Map<Long, ReadWriteState>>> topicEntry : messages.entrySet()) {
            String topic = topicEntry.getKey();
            
            for (Map.Entry<Integer, Map<Long, ReadWriteState>> partitionEntry : topicEntry.getValue().entrySet()) {
                int partition = partitionEntry.getKey();
                
                for (Map.Entry<Long, ReadWriteState> offsetEntry : partitionEntry.getValue().entrySet()) {
                    long offset = offsetEntry.getKey();
                    ReadWriteState state = offsetEntry.getValue();
                    
                    if (state.readLocation != null && state.writeLocation != null) {
                        // Both read and write - this is good, already validated incrementally
                        continue;
                    } else if (state.readLocation != null && state.writeLocation == null) {
                        // Read but never written - CRITICAL ERROR
                        failures.add(new ValidationFailure(
                            state.readLocation.lineNumber,
                            "READ_NEVER_WRITTEN",
                            String.format("Message at %s:%d:%d was read but never written. Read at %s",
                                topic, partition, offset, state.readLocation.location())
                        ));
                    } else if (state.readLocation == null && state.writeLocation != null) {
                        // Written but never read - CRITICAL ERROR
                        failures.add(new ValidationFailure(
                            state.writeLocation.lineNumber,
                            "WRITTEN_NEVER_READ",
                            String.format("Message at %s:%d:%d was written but never read. Written at %s",
                                topic, partition, offset, state.writeLocation.location())
                        ));
                        ObjectMapper mapper = new ObjectMapper();
                        ObjectNode result_details = mapper.createObjectNode();
                        result_details.put("topic", topic);
                        result_details.put("partition", partition);
                        result_details.put("offset", offset);
                        result_details.put("write_location", state.writeLocation.location());
                        Assert.unreachable("Written message never read", result_details);
                    } else {
                        // Neither read nor write - should be unreachable
                        throw new IllegalStateException(
                            String.format("Invalid state for message at %s:%d:%d - neither read nor written",
                                topic, partition, offset)
                        );
                    }
                }
            }
        }
    }

    @Override
    public List<ValidationFailure> getFailures() {
        return failures;
    }
}

/**
 * Validator: Application Message Partitioning
 * Ensures messages with the same key go to the same partition
 */
class ApplicationMessagePartitioningValidator implements TestValidator {
    private final List<ValidationFailure> failures = new ArrayList<>();
    
    // Track key -> set of partitions it was seen on
    private final Map<String, Map<String, Set<Integer>>> keyPartitions = new HashMap<>();

    @Override
    public String getName() {
        return "ApplicationMessagePartitioningValidator";
    }

    @Override
    public void validateEvent(int lineNumber, LogEvent event) {
        if (!"message_write_succeeded".equals(event.getEventType())) {
            return;
        }

        String topic = event.getField("topic_name");
        String partitionStr = event.getField("topic_partition");
        String key = event.getField("message_key");
        
        // Skip messages without keys
        if (topic == null || partitionStr == null || key == null || "null".equals(key)) {
            return;
        }
        
        try {
            int partition = Integer.parseInt(partitionStr);
            
            Set<Integer> partitions = keyPartitions
                .computeIfAbsent(topic, k -> new HashMap<>())
                .computeIfAbsent(key, k -> new HashSet<>());
            
            partitions.add(partition);
            
            // If same key appears on multiple partitions, that's an error
            if (partitions.size() > 1) {
                failures.add(new ValidationFailure(
                    lineNumber,
                    "KEY_PARTITION_VIOLATION",
                    String.format("Key '%s' in topic '%s' appeared on multiple partitions: %s",
                        key, topic, partitions)
                ));
            }
        } catch (NumberFormatException e) {
            // Invalid partition
        }
    }

    @Override
    public List<ValidationFailure> getFailures() {
        return failures;
    }
}