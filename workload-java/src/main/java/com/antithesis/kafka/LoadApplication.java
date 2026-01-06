package com.antithesis.kafka;

import com.antithesis.kafka.client.TestAdminClient;
import com.antithesis.kafka.config.WorkloadConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.antithesis.sdk.Lifecycle;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;

/**
 * Load application that waits for the Kafka cluster to be ready.
 * This is typically run before the main workload to ensure the cluster is operational.
 */
public class LoadApplication {
    private static final Logger log = LoggerFactory.getLogger(LoadApplication.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: LoadApplication <config-path>");
            System.exit(1);
        }

        String configPath = args[0];
        WorkloadConfig config = WorkloadConfig.fromFile(configPath);

        setupLogging();

        TestAdminClient adminClient = new TestAdminClient(config.getBootstrapServers());

        log.info("Waiting for Kafka cluster to be ready...");
        adminClient.waitOnCluster();

        if (config.isStrictMode()) {
            log.info("Checking cluster configuration...");
            adminClient.checkConfig();
        }

        log.info("{\"timestamp\":\"{}\",\"event\":\"cluster_started\",\"is_strict_config\":{}}",
            Instant.now(), config.isStrictMode());

        // Signal that setup is complete
        log.info("Cluster setup complete at {}", Instant.now());

        adminClient.close();

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode setupDetails = mapper.createObjectNode();
        setupDetails.put("setup", "complete");

        Lifecycle.setupComplete(setupDetails);
    }

    private static void setupLogging() {
        // Configure JSON logging
        // In a production setup, you would configure logback.xml or similar
        log.info("Logging configured for load application");
    }
}