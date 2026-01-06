package com.antithesis.kafka.client;

import com.antithesis.kafka.config.AdminKafkaConfig;
import com.antithesis.kafka.config.TopicKafkaConfig;
import com.antithesis.kafka.domain.TestTopic;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class TestAdminClient {
    private static final Logger log = LoggerFactory.getLogger(TestAdminClient.class);
    
    private final String bootstrapServers;
    private AdminClient adminClient;

    public TestAdminClient(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void waitOnCluster() throws Exception {
        if (adminClient != null) {
            return;
        }

        while (true) {
            try {
                AdminKafkaConfig config = new AdminKafkaConfig()
                    .withBootstrapServers(bootstrapServers)
                    .withRequestTimeout(5000);
                
                AdminClient client = AdminClient.create(config.toProperties());

                // Fetch cluster metadata to discover broker IDs
                DescribeClusterResult clusterResult = client.describeCluster();
                Collection<org.apache.kafka.common.Node> brokers = 
                    clusterResult.nodes().get(5, TimeUnit.SECONDS);

                boolean allBrokersUp = true;

                // Check each broker's configuration
                for (org.apache.kafka.common.Node broker : brokers) {
                    ConfigResource resource = new ConfigResource(
                        ConfigResource.Type.BROKER, 
                        String.valueOf(broker.id()));
                    
                    try {
                        DescribeConfigsResult result = client.describeConfigs(
                            Collections.singleton(resource));
                        result.all().get(5, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        allBrokersUp = false;
                        break;
                    }
                }

                if (allBrokersUp) {
                    this.adminClient = client;
                    log.info("Cluster is ready");
                    return;
                }

                client.close(Duration.ofSeconds(1));
            } catch (Exception e) {
                log.debug("Waiting for cluster to be ready: {}", e.getMessage());
            }

            // Avoid busy-looping
            Thread.sleep(5000);
        }
    }

    public void checkConfig() throws Exception {
        ConfigResource consumerOffsetsResource = new ConfigResource(
            ConfigResource.Type.TOPIC, 
            "__consumer_offsets");

        try {
            DescribeConfigsResult result = adminClient.describeConfigs(
                Collections.singleton(consumerOffsetsResource));
            
            Map<ConfigResource, Config> configs = result.all().get(5, TimeUnit.SECONDS);
            Config config = configs.get(consumerOffsetsResource);
            
            if (config != null) {
                ConfigEntry minInSyncReplicas = config.get("min.insync.replicas");
                if (minInSyncReplicas != null && minInSyncReplicas.value() != null) {
                    if (!"3".equals(minInSyncReplicas.value())) {
                        throw new IllegalStateException(
                            "Please configure 'min.insync.replicas' for the topic " +
                            "'__consumer_offsets' to '3'");
                    }
                }
            }
        } catch (ExecutionException | InterruptedException e) {
            log.warn("Could not check __consumer_offsets config: {}", e.getMessage());
        }
    }

    public void createTopic(TestTopic topic) throws Exception {
        while (true) {
            TopicKafkaConfig topicConfig = new TopicKafkaConfig()
                .withMinInSyncReplicas(3)
                .withFlushMessages(1);

            NewTopic newTopic = new NewTopic(
                topic.getName(), 
                topic.getNumPartitions(), 
                (short) topic.getReplicationFactor());
            newTopic.configs(topicConfig.toConfigMap());

            CreateTopicsResult createResult = adminClient.createTopics(
                Collections.singleton(newTopic));

            try {
                createResult.all().get();
                
                log.info("{\"timestamp\":\"{}\",\"event\":\"topic_created\",\"topic_name\":\"{}\",\"topic_num_partitions\":{}}",
                    Instant.now(), topic.getName(), topic.getNumPartitions());
                return;
                
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    log.error("{\"timestamp\":\"{}\",\"event\":\"topic_already_exists\",\"topic_name\":\"{}\"}",
                        Instant.now(), topic.getName());
                    return;
                } else {
                    log.error("{\"timestamp\":\"{}\",\"event\":\"topic_creation_failed\",\"topic_name\":\"{}\",\"error\":\"{}\"}",
                        Instant.now(), topic.getName(), e.getCause().getMessage());
                    // Retry after delay
                    Thread.sleep(5000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
        }
    }

    public void close() {
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
    }
}