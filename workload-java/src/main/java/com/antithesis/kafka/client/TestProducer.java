package com.antithesis.kafka.client;

import com.antithesis.kafka.config.ProducerKafkaConfig;
import com.antithesis.kafka.config.WorkloadConfig;
import com.antithesis.kafka.domain.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import com.antithesis.sdk.Assert;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TestProducer {
    private static final Logger log = LoggerFactory.getLogger(TestProducer.class);
    
    private final String id;
    private final KafkaProducer<String, String> producer;
    private final GlobalState globalState;
    private final ProducerKafkaConfig producerConfig;
    private final boolean transactionsEnabled;

    public TestProducer(String id, WorkloadConfig config, GlobalState globalState) {
        this.id = id;
        this.globalState = globalState;
        this.transactionsEnabled = config.isEnableTransactions();
        
        // Build producer configuration
        ProducerKafkaConfig producerConfig = new ProducerKafkaConfig();
        producerConfig
            .withClientId(id)
            .withBootstrapServers(config.getBootstrapServers())
            .set("message.timeout.ms", String.valueOf(config.getMessageTimeoutMs().getRandomValue()))
            .set("transaction.timeout.ms", String.valueOf(config.getTransactionTimeoutMs().getRandomValue()))
            .withAcks("all")
            .withEnableIdempotence(true);

        // Key and value serializers
        producerConfig
            .set("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            .set("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Only set transactional.id if transactions are enabled
        if (transactionsEnabled) {
            String transactionalId = id + "-tx";
            producerConfig.withTransactionalId(transactionalId);
        }

        if (config.isEnableDebug()) {
            producerConfig.set("debug", "protocol");
        }

        this.producerConfig = producerConfig;
        this.producer = new KafkaProducer<>(producerConfig.toProperties());

        // Initialize transactions if enabled
        if (transactionsEnabled) {
            try {
                producer.initTransactions();
            } catch (KafkaException e) {
                log.error("Failed to initialize transactions", e);
                throw e;
            }
        }

        log.info("{\"timestamp\":\"{}\",\"event\":\"producer_created\",\"producer_id\":\"{}\",\"enable_transactions\":{},\"config\":{}}",
            Instant.now(), id, transactionsEnabled, producerConfig.getProperties());
    }

    public CompletableFuture<Void> produce(WorkloadConfig config, List<TestTopic> topics) {
        return CompletableFuture.runAsync(() -> {
            try {
                // Startup delay
                Thread.sleep(config.getProducerStartupDelayMs().getRandomValue());
                
                log.info("{\"timestamp\":\"{}\",\"event\":\"producer_started\",\"producer_id\":\"{}\"}",
                    Instant.now(), id);

                int uniqueSequenceCount = config.getProducerUniqueSequenceCount().getRandomValue();
                
                for (int i = 0; i < uniqueSequenceCount; i++) {
                    int sequenceLength = config.getProducerSequenceLength().getRandomValue();
                    String sequenceName = UUID.randomUUID().toString();
                    
                    // Random topic selection
                    Random random = new Random();
                    TestTopic topic = topics.get(random.nextInt(topics.size()));
                    String topicName = topic.getName();
                    
                    // Random key decision
                    String key = random.nextBoolean() ? UUID.randomUUID().toString() : null;
                    
                    produceSequence(config, topicName, key, sequenceName, sequenceLength);
                }

                log.info("{\"timestamp\":\"{}\",\"event\":\"producer_stopped\",\"producer_id\":\"{}\"}",
                    Instant.now(), id);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Producer interrupted", e);
            } catch (Exception e) {
                log.error("Producer error", e);
            } finally {
                close();
            }
        });
    }

    private void produceSequence(WorkloadConfig config, String topic, String key, 
                                  String sequenceName, int sequenceLength) {
        if (transactionsEnabled) {
            produceSequenceWithTransaction(config, topic, key, sequenceName, sequenceLength);
        } else {
            produceSequenceWithoutTransaction(config, topic, key, sequenceName, sequenceLength);
        }
    }

    private void produceSequenceWithTransaction(WorkloadConfig config, String topic, 
                                                String key, String sequenceName, int sequenceLength) {
        // Begin transaction
        try {
            producer.beginTransaction();
            log.info("{\"timestamp\":\"{}\",\"event\":\"transaction_begun\",\"producer_id\":\"{}\",\"topic_name\":\"{}\",\"sequence_name\":\"{}\"}",
                Instant.now(), id, topic, sequenceName);
        } catch (KafkaException e) {
            log.error("{\"timestamp\":\"{}\",\"event\":\"transaction_begin_failed\",\"producer_id\":\"{}\",\"topic_name\":\"{}\",\"sequence_name\":\"{}\",\"error\":\"{}\"}",
                Instant.now(), id, topic, sequenceName, e.getMessage());
            return;
        }

        Sequence sequence = new Sequence(sequenceName, sequenceLength);
        int messagesSent = 0;
        List<PendingMessage> pendingMessages = new ArrayList<>();

        // Send messages in transaction
        while (sequence.hasNext()) {
            SequenceItem nextValue = sequence.next();
            int version = 0;
            boolean sent = false;
            
            while (!sent) {
                version++;
                String payload = String.format("%s:%d|%s", id, version, nextValue);
                
                try {
                    Thread.sleep(config.getProducerSequenceDelayMs().getRandomValue());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                ProducerRecord<String, String> record = 
                    new ProducerRecord<>(topic, key, payload);

                try {
                    // TODO: try not blocking on send... which requires more graceful handling 
                    RecordMetadata metadata = producer.send(record).get(config.getProducerSendTimeoutMs().getRandomValue(), TimeUnit.SECONDS);
                    messagesSent++;
                    pendingMessages.add(new PendingMessage(
                        metadata.partition(), metadata.offset(), payload, key));
                    sent = true;
                } catch (Exception e) {
                    log.error("{\"timestamp\":\"{}\",\"event\":\"message_write_failed\",\"producer_id\":\"{}\",\"topic_name\":\"{}\",\"message_key\":\"{}\",\"message_payload\":\"{}\",\"error\":\"{}\"}",
                        Instant.now(), id, topic, key, payload, e.getMessage());
                }
            }
        }

        // Decide whether to commit or abort (30% abort chance)
        Random random = new Random();
        boolean shouldAbort = random.nextInt(100) < 30;

        if (shouldAbort) {
            // Abort transaction with retry
            while (true) {
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    ObjectNode result_details = mapper.createObjectNode();
                    producer.abortTransaction();
                    result_details.put("producer_id", id);
                    result_details.put("topic", topic);
                    result_details.put("sequenceName", sequenceName);
                    result_details.put("messagesSent", messagesSent);
                    Assert.reachable("transaction_aborted", result_details);
                    
                    log.info("{\"timestamp\":\"{}\",\"event\":\"transaction_aborted\",\"producer_id\":\"{}\",\"topic_name\":\"{}\",\"sequence_name\":\"{}\",\"messages_sent\":{}}",
                        Instant.now(), id, topic, sequenceName, messagesSent);
                    break;
                } catch (KafkaException e) {
                    if (e.getMessage().contains("Timed out") || 
                        e.getMessage().contains("retry call to resume")) {
                        continue;
                    } else {
                        log.error("{\"timestamp\":\"{}\",\"event\":\"transaction_abort_failed\",\"producer_id\":\"{}\",\"topic_name\":\"{}\",\"sequence_name\":\"{}\",\"error\":\"{}\"}",
                            Instant.now(), id, topic, sequenceName, e.getMessage());
                        break;
                    }
                }
            }
        } else {
            // Commit transaction with retry
            while (true) {
                try {
                    producer.commitTransaction();
                    
                    // Log successful messages after commit
                    for (PendingMessage msg : pendingMessages) {
                        synchronized (globalState) {
                            globalState.getTopicPartitionOffsets()
                                .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                                .put(msg.partition, msg.offset);
                        }
                        
                        ObjectMapper mapper = new ObjectMapper();
                        ObjectNode result_details = mapper.createObjectNode();
                        result_details.put("producer_id", id);
                        result_details.put("topic", topic);
                        result_details.put("partition", msg.partition);
                        result_details.put("offset", msg.offset);
                        result_details.put("key", msg.key);
                        result_details.put("payload", msg.payload);
                        Assert.reachable("message_write_succeeded", result_details);
                        log.info("{\"timestamp\":\"{}\",\"event\":\"message_write_succeeded\",\"producer_id\":\"{}\",\"topic_name\":\"{}\",\"topic_partition\":{},\"topic_partition_offset\":{},\"message_key\":\"{}\",\"message_payload\":\"{}\"}",
                            Instant.now(), id, topic, msg.partition, msg.offset, msg.key, msg.payload);
                    }
                    
                    ObjectMapper mapper = new ObjectMapper();
                    ObjectNode result_details = mapper.createObjectNode();
                    result_details.put("producer_id", id);
                    result_details.put("topic", topic);
                    result_details.put("sequenceName", sequenceName);
                    result_details.put("messagesSent", messagesSent);
                    Assert.reachable("transaction_committed", result_details);
                    log.info("{\"timestamp\":\"{}\",\"event\":\"transaction_committed\",\"producer_id\":\"{}\",\"topic_name\":\"{}\",\"sequence_name\":\"{}\",\"messages_sent\":{}}",
                        Instant.now(), id, topic, sequenceName, messagesSent);
                    break;
                } catch (KafkaException e) {
                    if (e.getMessage().contains("Timed out") || 
                        e.getMessage().contains("retry call to resume")) {
                        continue;
                    } else {
                        log.error("{\"timestamp\":\"{}\",\"event\":\"transaction_commit_failed\",\"producer_id\":\"{}\",\"topic_name\":\"{}\",\"sequence_name\":\"{}\",\"error\":\"{}\"}",
                            Instant.now(), id, topic, sequenceName, e.getMessage());
                        break;
                    }
                }
            }
        }
    }

    private void produceSequenceWithoutTransaction(WorkloadConfig config, String topic, 
                                                   String key, String sequenceName, int sequenceLength) {
        Sequence sequence = new Sequence(sequenceName, sequenceLength);
        
        while (sequence.hasNext()) {
            SequenceItem nextValue = sequence.next();
            int version = 0;
            boolean sent = false;
            
            while (!sent) {
                version++;
                String payload = String.format("%s:%d|%s", id, version, nextValue);
                
                try {
                    Thread.sleep(config.getProducerSequenceDelayMs().getRandomValue());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                ProducerRecord<String, String> record = 
                    new ProducerRecord<>(topic, key, payload);

                try {
                    // TODO: try not blocking on send... which requires more graceful future handling
                    // RecordMetadata metadata = producer.send(record);
                    RecordMetadata metadata = producer.send(record).get(config.getProducerSendTimeoutMs().getRandomValue(), TimeUnit.MILLISECONDS);
                    
                    synchronized (globalState) {
                        globalState.getTopicPartitionOffsets()
                            .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                            .put(metadata.partition(), metadata.offset());
                    }
                    
                    log.info("{\"timestamp\":\"{}\",\"event\":\"message_write_succeeded\",\"producer_id\":\"{}\",\"topic_name\":\"{}\",\"topic_partition\":{},\"topic_partition_offset\":{},\"message_key\":\"{}\",\"message_payload\":\"{}\"}",
                        Instant.now(), id, topic, metadata.partition(), metadata.offset(), key, payload);
                    sent = true;
                } catch (Exception e) {
                    log.error("{\"timestamp\":\"{}\",\"event\":\"message_write_failed\",\"producer_id\":\"{}\",\"topic_name\":\"{}\",\"message_key\":\"{}\",\"message_payload\":\"{}\",\"error\":\"{}\"}",
                        Instant.now(), id, topic, key, payload, e.getMessage());
                }
            }
        }
    }

    public void close() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
    }

    // Helper class for pending messages
    private static class PendingMessage {
        final int partition;
        final long offset;
        final String payload;
        final String key;

        PendingMessage(int partition, long offset, String payload, String key) {
            this.partition = partition;
            this.offset = offset;
            this.payload = payload;
            this.key = key;
        }
    }
}