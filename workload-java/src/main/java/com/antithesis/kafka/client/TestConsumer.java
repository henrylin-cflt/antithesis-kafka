package com.antithesis.kafka.client;

import com.antithesis.kafka.config.ConsumerKafkaConfig;
import com.antithesis.kafka.config.WorkloadConfig;
import com.antithesis.kafka.domain.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TestConsumer {
    private static final Logger log = LoggerFactory.getLogger(TestConsumer.class);
    
    private final String id;
    private final String groupId;
    private final boolean enableAutoCommit;
    private final KafkaConsumer<String, String> consumer;
    private final boolean isObserver;
    private final ConsumerKafkaConfig consumerConfig;
    private final GlobalState globalState;
    
    private final AtomicReference<ConsumerState> state = new AtomicReference<>(ConsumerState.NOT_READY);
    private final Map<String, Map<Integer, LastReadOffset>> topicPartitionOffsets = new ConcurrentHashMap<>();
    private final AtomicInteger suspiciousTimeouts = new AtomicInteger(0);

    public enum ConsumerState {
        NOT_READY,
        STARTED,
        STOPPED
    }

    public enum LastReadOffset {
        ASSIGNED, UNASSIGNED;
        
        private long offset;
        
        public static LastReadOffset assigned(long offset) {
            LastReadOffset lro = ASSIGNED;
            lro.offset = offset;
            return lro;
        }
        
        public static LastReadOffset unassigned(long offset) {
            LastReadOffset lro = UNASSIGNED;
            lro.offset = offset;
            return lro;
        }
        
        public long getOffset() { return offset; }
        public void setOffset(long offset) { this.offset = offset; }
        public boolean isAssigned() { return this == ASSIGNED; }  
        public boolean isUnassigned() { return this == UNASSIGNED; }  
    }

    public static class ConsumerPendingReadMetadata {
        public String topicName;
        public int topicPartitionIndex;
        public Long lastReadOffset;
        public long lastWrittenOffset;

        public ConsumerPendingReadMetadata(String topicName, int topicPartitionIndex, 
                                          Long lastReadOffset, long lastWrittenOffset) {
            this.topicName = topicName;
            this.topicPartitionIndex = topicPartitionIndex;
            this.lastReadOffset = lastReadOffset;
            this.lastWrittenOffset = lastWrittenOffset;
        }
    }

    public enum ConsumerReceivedAllMessagesResult {
        PRODUCERS_NOT_COMPLETED,
        CONSUMER_NOT_STARTED,
        PARTITIONS_PENDING_READ,
        NO_MESSAGES_TO_BE_RECEIVED,
        ALL_MESSAGES_RECEIVED;
        
        private List<ConsumerPendingReadMetadata> pendingPartitions;
        
        public static ConsumerReceivedAllMessagesResult partitionsPendingRead(
                List<ConsumerPendingReadMetadata> partitions) {
            ConsumerReceivedAllMessagesResult result = PARTITIONS_PENDING_READ;
            result.pendingPartitions = partitions;
            return result;
        }
        
        public List<ConsumerPendingReadMetadata> getPendingPartitions() {
            return pendingPartitions;
        }
    }

    public TestConsumer(String id, String groupId, WorkloadConfig config, 
                       GlobalState globalState, boolean enableAutoCommit, boolean isObserver) {
        this.id = id;
        this.groupId = groupId;
        this.enableAutoCommit = enableAutoCommit;
        this.isObserver = isObserver;
        this.globalState = globalState;

        // Build consumer configuration
        ConsumerKafkaConfig consumerConfig = new ConsumerKafkaConfig();
        consumerConfig
            .withClientId(groupId + "-" + id)
            .withGroupId(groupId)
            .withBootstrapServers(config.getBootstrapServers())
            .set("allow.auto.create.topics", "true")
            .withEnableAutoCommit(enableAutoCommit)
            .withAutoOffsetReset("earliest")
            .withIsolationLevel(config.getConsumerIsolationLevel())
            .set("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");

        // Serializers
        consumerConfig
            .set("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
            .set("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        if (config.isEnableDebug()) {
            consumerConfig.set("debug", "protocol");
        }

        this.consumerConfig = consumerConfig;
        this.consumer = new KafkaConsumer<>(consumerConfig.toProperties());

        log.info("{\"timestamp\":\"{}\",\"event\":\"consumer_created\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\"}",
            Instant.now(), groupId, id);
    }

    public void subscribe(List<TestTopic> topics) {
        // If there are no topics, the consumer is ready right away
        if (topics.isEmpty()) {
            state.set(ConsumerState.STARTED);
        } else {
            List<String> topicNames = new ArrayList<>();
            for (TestTopic topic : topics) {
                topicNames.add(topic.getName());
                // Create entry for each subscribed topic
                topicPartitionOffsets.put(topic.getName(), new ConcurrentHashMap<>());
            }

            // Subscribe with rebalance listener
            consumer.subscribe(topicNames, new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    handleRebalanceRevoke(partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    handleRebalanceAssign(partitions);
                }
            });

            log.info("{\"timestamp\":\"{}\",\"event\":\"consumer_topics_subscribed\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\",\"topic_names\":{}}",
                Instant.now(), groupId, id, topicNames);
        }
    }

    private void handleRebalanceAssign(Collection<TopicPartition> partitions) {
        // Check and change consumer state if needed
        if (state.get() == ConsumerState.NOT_READY) {
            state.compareAndSet(ConsumerState.NOT_READY, ConsumerState.STARTED);
        }

        log.info("{\"timestamp\":\"{}\",\"event\":\"consumer_post_rebalance_assign\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\",\"topic_partition_list\":\"{}\"}",
            Instant.now(), groupId, id, formatTopicPartitions(partitions));

        for (TopicPartition tp : partitions) {
            topicPartitionOffsets
                .computeIfAbsent(tp.topic(), k -> new ConcurrentHashMap<>())
                .compute(tp.partition(), (k, v) -> {
                    if (v == null) {
                        return LastReadOffset.assigned(-1);
                    }
                    // Convert unassigned to assigned, keep offset
                    return LastReadOffset.assigned(v.getOffset());
                });
        }
    }

    private void handleRebalanceRevoke(Collection<TopicPartition> partitions) {
        log.info("{\"timestamp\":\"{}\",\"event\":\"consumer_post_rebalance_revoke\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\",\"topic_partition_list\":\"{}\"}",
            Instant.now(), groupId, id, formatTopicPartitions(partitions));

        for (TopicPartition tp : partitions) {
            topicPartitionOffsets
                .computeIfAbsent(tp.topic(), k -> new ConcurrentHashMap<>())
                .compute(tp.partition(), (k, v) -> {
                    if (v == null) {
                        return LastReadOffset.unassigned(-1);
                    }
                    // Convert assigned to unassigned, keep offset
                    return LastReadOffset.unassigned(v.getOffset());
                });
        }
    }

    public CompletableFuture<Void> poll(WorkloadConfig config) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (!isObserver) {
                    Thread.sleep(config.getConsumerGroupMemberStartupDelayMs().getRandomValue());
                }

                log.info("{\"timestamp\":\"{}\",\"event\":\"consumer_started\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\"}",
                    Instant.now(), groupId, id);

                while (state.get() != ConsumerState.STOPPED) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

                    if (records.isEmpty()) {
                        suspiciousTimeouts.incrementAndGet();
                        
                        if (suspiciousTimeouts.get() > 24 && 
                            globalState.getAllMessagesReadByObserver().get()) {
                            log.info("Consumer appears to have died: consumer_group_id={}, consumer_id={}, reason=consumer_maybe_dead",
                                groupId, id);
                        }

                        ConsumerReceivedAllMessagesResult result = allMessagesReceived();
                        handleWaitingState(result);
                    } else {
                        suspiciousTimeouts.set(0);
                        
                        for (ConsumerRecord<String, String> record : records) {
                            handleMessage(record, config);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Consumer interrupted", e);
            } catch (Exception e) {
                log.error("Consumer error", e);
            } finally {
                close();
            }
        });
    }

    private void handleMessage(ConsumerRecord<String, String> record, WorkloadConfig config) 
            throws InterruptedException {
        log.info("{\"timestamp\":\"{}\",\"event\":\"message_read_succeeded\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\",\"topic_name\":\"{}\",\"topic_partition\":{},\"topic_partition_offset\":{},\"message_key\":\"{}\",\"message_payload\":\"{}\"}",
            Instant.now(), groupId, id, record.topic(), record.partition(), 
            record.offset(), record.key(), record.value());

        if (!isObserver) {
            Thread.sleep(config.getConsumerGroupMemberProcessDelayMs().getRandomValue());
        }

        if (!enableAutoCommit) {
            commitMessage(record);
        }
    }

    private void commitMessage(ConsumerRecord<String, String> record) {
        try {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
            );
            consumer.commitSync(offsets);

            log.info("{\"timestamp\":\"{}\",\"event\":\"consumer_message_committed\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\",\"topic_name\":\"{}\",\"topic_partition\":{},\"topic_partition_offset\":{}}",
                Instant.now(), groupId, id, record.topic(), record.partition(), record.offset());

            // Update local state
            topicPartitionOffsets
                .computeIfAbsent(record.topic(), k -> new ConcurrentHashMap<>())
                .compute(record.partition(), (k, v) -> LastReadOffset.assigned(record.offset()));

            // Update global state
            synchronized (globalState) {
                globalState.getConsumerGroupsReadOffsets()
                    .computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(record.topic(), k -> new ConcurrentHashMap<>())
                    .put(record.partition(), record.offset());
            }
        } catch (Exception e) {
            log.error("{\"timestamp\":\"{}\",\"event\":\"consumer_message_commit_failure\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\",\"topic_name\":\"{}\",\"topic_partition\":{},\"topic_partition_offset\":{},\"error\":\"{}\"}",
                Instant.now(), groupId, id, record.topic(), record.partition(), 
                record.offset(), e.getMessage());
        }
    }

    private ConsumerReceivedAllMessagesResult allMessagesReceived() {
        if (!globalState.getAllProducersCompleted().get()) {
            return ConsumerReceivedAllMessagesResult.PRODUCERS_NOT_COMPLETED;
        }

        if (state.get() == ConsumerState.NOT_READY) {
            // Check if all subscribed topics are empty
            boolean allSubscribedTopicsAreEmpty = true;
            for (String topicName : topicPartitionOffsets.keySet()) {
                if (globalState.getTopicPartitionOffsets().containsKey(topicName)) {
                    allSubscribedTopicsAreEmpty = false;
                    break;
                }
            }

            if (allSubscribedTopicsAreEmpty) {
                return ConsumerReceivedAllMessagesResult.NO_MESSAGES_TO_BE_RECEIVED;
            } else {
                return ConsumerReceivedAllMessagesResult.CONSUMER_NOT_STARTED;
            }
        }

        List<ConsumerPendingReadMetadata> remainingPartitions = new ArrayList<>();

        Map<String, Map<Integer, Long>> globalOffsets = globalState.getTopicPartitionOffsets();
        for (Map.Entry<String, Map<Integer, Long>> topicEntry : globalOffsets.entrySet()) {
            String topic = topicEntry.getKey();
            
            for (Map.Entry<Integer, Long> partitionEntry : topicEntry.getValue().entrySet()) {
                int partition = partitionEntry.getKey();
                long lastWrittenOffset = partitionEntry.getValue();

                if (lastWrittenOffset < 0) {
                    continue;
                }

                // Skip partitions completed by the consumer group
                Long groupCompletedOffset = globalState.getConsumerGroupsReadOffsets()
                    .getOrDefault(groupId, Collections.emptyMap())
                    .getOrDefault(topic, Collections.emptyMap())
                    .get(partition);
                
                if (groupCompletedOffset != null && groupCompletedOffset >= lastWrittenOffset) {
                    continue;
                }

                Map<Integer, LastReadOffset> topicOffsets = topicPartitionOffsets.get(topic);
                if (topicOffsets != null) {
                    LastReadOffset lastReadOffset = topicOffsets.get(partition);                    
                    if (lastReadOffset != null) {
                        if (lastReadOffset.isAssigned()) {
                            if (lastReadOffset.getOffset() < lastWrittenOffset) {
                                remainingPartitions.add(new ConsumerPendingReadMetadata(
                                    topic, partition, lastReadOffset.getOffset(), lastWrittenOffset));
                            }
                        } else if (lastReadOffset.isUnassigned()) {
                            if (isObserver) {
                                // Observer must wait for partition to be reassigned
                                remainingPartitions.add(new ConsumerPendingReadMetadata(
                                    topic, partition, lastReadOffset.getOffset(), lastWrittenOffset));
                            }
                            // Non-observer: skip unassigned partitions (implicit)
                        }
                    } else if (isObserver) {
                        remainingPartitions.add(new ConsumerPendingReadMetadata(
                            topic, partition, null, lastWrittenOffset));
                    }
                } else if (isObserver) {
                    remainingPartitions.add(new ConsumerPendingReadMetadata(
                        topic, partition, null, lastWrittenOffset));
                }
            }
        }

        if (remainingPartitions.isEmpty()) {
            if (isObserver && !globalState.getAllMessagesReadByObserver().get()) {
                globalState.getAllMessagesReadByObserver().set(true);
            }
            return ConsumerReceivedAllMessagesResult.ALL_MESSAGES_RECEIVED;
        } else {
            return ConsumerReceivedAllMessagesResult.partitionsPendingRead(remainingPartitions);
        }
    }

    public void assign(List<TopicPartition> partitions) {
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);  // Start from beginning
        
        state.set(ConsumerState.STARTED);
        
        for (TopicPartition tp : partitions) {
            topicPartitionOffsets
                .computeIfAbsent(tp.topic(), k -> new ConcurrentHashMap<>())
                .put(tp.partition(), LastReadOffset.assigned(-1));
        }
        
        log.info("Observer assigned {} partitions", partitions.size());
    }

    private void handleWaitingState(ConsumerReceivedAllMessagesResult result) {
        switch (result) {
            case PRODUCERS_NOT_COMPLETED:
                log.info("{\"timestamp\":\"{}\",\"event\":\"consumer_waiting\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\",\"reason\":\"producers_not_completed\"}",
                    Instant.now(), groupId, id);
                break;
            case CONSUMER_NOT_STARTED:
                log.info("{\"timestamp\":\"{}\",\"event\":\"consumer_waiting\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\",\"reason\":\"consumer_not_started\"}",
                    Instant.now(), groupId, id);
                break;
            case PARTITIONS_PENDING_READ:
                log.info("{\"timestamp\":\"{}\",\"event\":\"consumer_waiting\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\",\"reason\":\"partitions_pending_read\",\"partitions\":\"{}\"}",
                    Instant.now(), groupId, id, formatPendingPartitions(result.getPendingPartitions()));
                break;
            case NO_MESSAGES_TO_BE_RECEIVED:
                log.info("{\"timestamp\":\"{}\",\"event\":\"consumer_stopping\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\",\"reason\":\"no_messages_to_be_received\"}",
                    Instant.now(), groupId, id);
                stop();
                break;
            case ALL_MESSAGES_RECEIVED:
                log.info("{\"timestamp\":\"{}\",\"event\":\"consumer_stopping\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\",\"reason\":\"all_messages_received\"}",
                    Instant.now(), groupId, id);
                stop();
                break;
        }
    }

    public void stop() {
        state.set(ConsumerState.STOPPED);
    }

    private void close() {
        if (consumer != null) {
            consumer.close(Duration.ofSeconds(5));
        }
        log.info("{\"timestamp\":\"{}\",\"event\":\"consumer_stopped\",\"consumer_group_id\":\"{}\",\"consumer_id\":\"{}\"}",
            Instant.now(), groupId, id);
    }

    private String formatTopicPartitions(Collection<TopicPartition> partitions) {
        StringBuilder sb = new StringBuilder("{");
        Map<String, List<Integer>> grouped = new HashMap<>();
        
        for (TopicPartition tp : partitions) {
            grouped.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp.partition());
        }
        
        boolean first = true;
        for (Map.Entry<String, List<Integer>> entry : grouped.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(entry.getKey()).append("\":[");
            for (int i = 0; i < entry.getValue().size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(entry.getValue().get(i));
            }
            sb.append("]");
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    private String formatPendingPartitions(List<ConsumerPendingReadMetadata> partitions) {
        if (partitions == null || partitions.isEmpty()) return "[]";
        
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < partitions.size(); i++) {
            if (i > 0) sb.append(",");
            ConsumerPendingReadMetadata p = partitions.get(i);
            sb.append("{\"topic\":\"").append(p.topicName)
              .append("\",\"partition\":").append(p.topicPartitionIndex)
              .append(",\"last_read\":").append(p.lastReadOffset)
              .append(",\"last_written\":").append(p.lastWrittenOffset)
              .append("}");
        }
        sb.append("]");
        return sb.toString();
    }
}