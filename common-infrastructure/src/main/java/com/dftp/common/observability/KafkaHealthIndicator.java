package com.dftp.common.observability;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Custom HealthIndicator for Apache Kafka ensuring inclusion in the Spring Boot Actuator
 * 'readiness' group.
 */
@Slf4j
@Component("kafka")
@ConditionalOnClass({AdminClient.class, KafkaAdmin.class})
public class KafkaHealthIndicator implements HealthIndicator {

    private final ObjectProvider<KafkaAdmin> kafkaAdminProvider;
    private volatile AdminClient adminClient;

    public KafkaHealthIndicator(ObjectProvider<KafkaAdmin> kafkaAdminProvider) {
        this.kafkaAdminProvider = kafkaAdminProvider;
    }

    private synchronized AdminClient getOrCreateAdminClient() {
        if (this.adminClient == null) {
            KafkaAdmin admin = kafkaAdminProvider.getIfAvailable();
            if (admin != null) {
                this.adminClient = AdminClient.create(admin.getConfigurationProperties());
            }
        }
        return this.adminClient;
    }

    @Override
    public Health health() {
        KafkaAdmin admin = kafkaAdminProvider.getIfAvailable();
        if (admin == null) {
            return Health.unknown().withDetail("reason", "KafkaAdmin not configured").build();
        }

        try {
            AdminClient client = getOrCreateAdminClient();
            if (client == null) {
                return Health.down().withDetail("error", "Failed to create AdminClient").build();
            }

            DescribeClusterResult cluster = client.describeCluster(new DescribeClusterOptions().timeoutMs(3000));
            String clusterId = cluster.clusterId().get(3, TimeUnit.SECONDS);
            int nodeCount = cluster.nodes().get(3, TimeUnit.SECONDS).size();

            return Health.up()
                    .withDetail("clusterId", clusterId)
                    .withDetail("nodes", nodeCount)
                    .build();
        } catch (Exception e) {
            log.warn("Kafka health check failed: {}", e.getMessage());
            closeAdminClient();
            return Health.down(e)
                    .withDetail("error", e.getMessage())
                    .build();
        }
    }

    private synchronized void closeAdminClient() {
        if (this.adminClient != null) {
            try {
                this.adminClient.close(Duration.ofSeconds(1));
            } catch (Exception ex) {
                log.debug("Error closing AdminClient", ex);
            }
            this.adminClient = null;
        }
    }
}
