package com.antithesis.kafka;

import com.antithesis.kafka.client.TestAdminClient;
import com.antithesis.kafka.client.TestConsumer;
import com.antithesis.kafka.client.TestProducer;
import com.antithesis.kafka.config.WorkloadConfig;
import com.antithesis.kafka.domain.GlobalState;
import com.antithesis.kafka.domain.TestTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main workload application that orchestrates Kafka producers and consumers
 * for testing purposes.
 */
public class WorkloadApplication {
    private static final Logger log = LoggerFactory.getLogger(WorkloadApplication.class);
    
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: WorkloadApplication <config-path>");
            System.exit(1);
        }

        String configPath = args[0];
        String workloadId = args[1];

        WorkloadConfig config = WorkloadConfig.fromFile(configPath);
        
        log.info("Workload configuration loaded: {}", config);

        List<CompletableFuture<Void>> producerTasks = new ArrayList<>();
        List<CompletableFuture<Void>> consumerTasks = new ArrayList<>();

        // Initialize admin client and wait for cluster
        TestAdminClient adminClient = new TestAdminClient(config.getBootstrapServers());
        adminClient.waitOnCluster();

        if (config.isStrictMode()) {
            adminClient.checkConfig();
        }

        log.info("{\"timestamp\":\"{}\",\"event\":\"workload_started\",\"is_strict_config\":{}}",
            Instant.now(), config.isStrictMode());

        List<TestTopic> topics = new ArrayList<>();
        List<TestProducer> producers = new ArrayList<>();
        List<List<TestConsumer>> consumerGroups = new ArrayList<>();
        GlobalState globalState = new GlobalState();

        // Create topics
        int topicCount = config.getTopicCount().getRandomValue();
        for (int i = 0; i < topicCount; i++) {
            TestTopic topic = new TestTopic(
                UUID.randomUUID().toString(),
                config.getTopicPartitionCount().getRandomValue(),
                3 // replication factor
            );

            if (config.isStrictMode()) {
                adminClient.createTopic(topic);
            } else {
                // Randomly decide whether to create topic
                if (new Random().nextBoolean()) {
                    adminClient.createTopic(topic);
                }
            }
            topics.add(topic);
        }

        // Create consumer groups
        int consumerGroupCount = config.getConsumerGroupCount().getRandomValue();
        for (int groupIdx = 0; groupIdx < consumerGroupCount; groupIdx++) {
            List<TestConsumer> consumerGroup = new ArrayList<>();
            String groupId = String.format("%s-cg%d", workloadId, groupIdx + 1);

            int membersCount = config.getConsumerGroupMembersCount().getRandomValue();
            for (int memberIdx = 0; memberIdx < membersCount; memberIdx++) {
                TestConsumer consumer = new TestConsumer(
                    String.format("c%d", memberIdx + 1),
                    groupId,
                    config,
                    globalState,
                    false, // enable auto commit
                    false // not observer
                );
                consumerGroup.add(consumer);
            }
            consumerGroups.add(consumerGroup);
        }

        // Create producers
        int producerCount = config.getProducerCount().getRandomValue();
        for (int i = 0; i < producerCount; i++) {
            TestProducer producer = new TestProducer(
                String.format("%s-p%d", workloadId, i + 1),
                config,
                globalState
            );
            producers.add(producer);
        }

        // Start producers
        for (TestProducer producer : producers) {
            CompletableFuture<Void> task = producer.produce(config, topics);
            producerTasks.add(task);
        }

        // Start consumer groups
        Random random = new Random();
        for (List<TestConsumer> consumerGroup : consumerGroups) {
            for (TestConsumer consumer : consumerGroup) {
                // Randomly subscribe to topics
                List<TestTopic> consumerTopics = new ArrayList<>();
                for (TestTopic topic : topics) {
                    if (random.nextBoolean()) {
                        consumerTopics.add(topic);
                    }
                }
                consumer.subscribe(consumerTopics);
                
                CompletableFuture<Void> task = consumer.poll(config);
                consumerTasks.add(task);
            }
        }

        // Wait for all producers to complete
        CompletableFuture.allOf(producerTasks.toArray(new CompletableFuture[0])).join();
        globalState.getAllProducersCompleted().set(true);
        
        log.info("{\"timestamp\":\"{}\",\"event\":\"all_producers_completed\"}",
            Instant.now());

        // In WorkloadApplication.java, change observer setup:
        TestConsumer observer = new TestConsumer(
            "o",
            String.format("%s-og", workloadId),
            config,
            globalState,
            false,
            true
        );

        // Manually assign all partitions:
        List<TopicPartition> allPartitions = new ArrayList<>();
        for (TestTopic topic : topics) {
            for (int i = 0; i < topic.getNumPartitions(); i++) {
                allPartitions.add(new TopicPartition(topic.getName(), i));
            }
        }
        observer.assign(allPartitions);  

        CompletableFuture<Void> observerTask = observer.poll(config);
        consumerTasks.add(observerTask);

        // Wait for all consumers to complete
        CompletableFuture.allOf(consumerTasks.toArray(new CompletableFuture[0])).join();

        log.info("{\"timestamp\":\"{}\",\"event\":\"workload_ended\"}",
            Instant.now());

        adminClient.close();
    }

}