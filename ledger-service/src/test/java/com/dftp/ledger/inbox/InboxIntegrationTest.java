package com.dftp.ledger.inbox;

import com.dftp.common.inbox.InboxMessage;
import com.dftp.common.inbox.InboxMessageRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class InboxIntegrationTest {

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
    private InboxMessageRepository inboxMessageRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    void firstEventAccepted() {
        UUID eventId = UUID.randomUUID();
        InboxMessage message = InboxMessage.builder()
                .eventId(eventId)
                .consumerGroup("test-group")
                .eventType("TestEvent")
                .build();
        
        inboxMessageRepository.saveAndFlush(message);
        assertThat(inboxMessageRepository.existsByEventId(eventId)).isTrue();
    }

    @Test
    void duplicateEventId_throwsException() {
        UUID eventId = UUID.randomUUID();
        
        // First delivery
        InboxMessage message1 = InboxMessage.builder()
                .eventId(eventId)
                .consumerGroup("test-group")
                .eventType("TestEvent")
                .build();
        inboxMessageRepository.saveAndFlush(message1);

        // Second delivery of same message (e.g. Kafka redelivery)
        InboxMessage message2 = InboxMessage.builder()
                .eventId(eventId)
                .consumerGroup("test-group")
                .eventType("TestEvent")
                .build();

        // The database unique constraint on event_id prevents insertion
        assertThrows(DataIntegrityViolationException.class, () -> {
            inboxMessageRepository.saveAndFlush(message2);
        });
    }

    @Test
    void differentEventId_sameTransactionId_accepted() {
        // Here we demonstrate that the Inbox ONLY cares about eventId (Message Identity)
        UUID eventId1 = UUID.randomUUID();
        UUID eventId2 = UUID.randomUUID();
        
        InboxMessage message1 = InboxMessage.builder()
                .eventId(eventId1)
                .consumerGroup("test-group")
                .eventType("TestEvent")
                .build();
        inboxMessageRepository.saveAndFlush(message1);

        InboxMessage message2 = InboxMessage.builder()
                .eventId(eventId2)
                .consumerGroup("test-group")
                .eventType("TestEvent")
                .build();
        inboxMessageRepository.saveAndFlush(message2);

        // Both are accepted. The business layer must use transactionId in the payload to deduplicate business intent.
        assertThat(inboxMessageRepository.existsByEventId(eventId1)).isTrue();
        assertThat(inboxMessageRepository.existsByEventId(eventId2)).isTrue();
    }

    @Test
    @DisplayName("Transactional Rollback: Business failure rolls back Inbox insert, allowing redelivery retry")
    void businessFailure_rollsBackInboxInsert_allowingRedeliveryRetry() {
        TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
        UUID eventId = UUID.randomUUID();

        // 1. First attempt: Inbox INSERT followed by business failure within same transaction
        assertThatThrownBy(() -> {
            txTemplate.execute(status -> {
                int inserted = inboxMessageRepository.insertIfAbsent(eventId, "test-group", "FundsTransfer");
                assertThat(inserted).isEqualTo(1);

                // Simulate business mutation failure in the same transaction
                throw new RuntimeException("Simulated downstream business failure");
            });
        }).isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Simulated downstream business failure");

        // 2. Assert: Inbox row MUST be absent due to transaction rollback
        assertThat(inboxMessageRepository.existsByEventId(eventId))
                .as("Inbox record must not exist when enclosing business transaction rolls back")
                .isFalse();

        // 3. Second attempt (Redelivery / Retry):
        Integer retryResult = txTemplate.execute(status -> {
            int inserted = inboxMessageRepository.insertIfAbsent(eventId, "test-group", "FundsTransfer");
            // Simulate successful business mutation on retry
            return inserted;
        });

        assertThat(retryResult).as("Retry must succeed because previous failed attempt rolled back").isEqualTo(1);
        assertThat(inboxMessageRepository.existsByEventId(eventId)).isTrue();

        // 4. Third attempt (Duplicate delivery after commit):
        Integer duplicateResult = txTemplate.execute(status -> {
            return inboxMessageRepository.insertIfAbsent(eventId, "test-group", "FundsTransfer");
        });

        assertThat(duplicateResult).as("Duplicate after commit must be suppressed").isEqualTo(0);
    }
}
