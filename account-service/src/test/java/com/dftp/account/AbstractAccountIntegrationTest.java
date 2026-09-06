package com.dftp.account;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared singleton Testcontainers base class for all Account Service integration tests.
 *
 * <p>Solves the Spring context cache poisoning problem: when each test class declares
 * its own {@code @Container}, the containers have different lifecycles but Spring
 * caches and reuses application contexts. When an earlier test class's container
 * shuts down, the cached context's HikariPool connections die, breaking background
 * threads (OutboxRelay scheduler, Kafka consumers) for subsequent test classes.</p>
 *
 * <p>By using singleton containers (started once, shared across all test classes),
 * the datasource URL and Kafka bootstrap servers remain stable for the entire
 * Surefire JVM fork.</p>
 */
public abstract class AbstractAccountIntegrationTest {

    static final PostgreSQLContainer<?> postgres;
    static final KafkaContainer kafka;

    static {
        postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("test_db")
                .withUsername("test_user")
                .withPassword("test_pass");
        postgres.start();

        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
        kafka.start();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
}
