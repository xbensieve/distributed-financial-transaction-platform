package com.dftp.template.outbox;

import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class OutboxIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    void atomicCommit_savesOutboxEvent() {
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        
        UUID eventId = UUID.randomUUID();
        template.execute(status -> {
            OutboxEvent event = OutboxEvent.builder()
                    .id(eventId)
                    .aggregateType("TEST")
                    .aggregateId("123")
                    .eventType("TestEvent")
                    .payload("{}")
                    .build();
            outboxEventRepository.save(event);
            return null;
        });

        assertThat(outboxEventRepository.findById(eventId)).isPresent();
    }

    @Test
    void businessTransactionRollback_preventsOutboxInsertion() {
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        UUID eventId = UUID.randomUUID();

        assertThrows(RuntimeException.class, () -> {
            template.execute(status -> {
                OutboxEvent event = OutboxEvent.builder()
                        .id(eventId)
                        .aggregateType("TEST")
                        .aggregateId("123")
                        .eventType("TestEvent")
                        .payload("{}")
                        .build();
                outboxEventRepository.save(event);
                
                // Simulate a business failure that causes the transaction to roll back
                throw new RuntimeException("Simulated business failure");
            });
        });

        // The event should NOT exist in the database because the transaction rolled back
        assertThat(outboxEventRepository.findById(eventId)).isEmpty();
    }
}
