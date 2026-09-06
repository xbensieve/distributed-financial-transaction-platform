package com.dftp.template.inbox;

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

import static org.assertj.core.api.Assertions.assertThat;
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
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

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

        // The database unique constraint on event_id prevents insertion.
        // Spring Data JPA's save() would do a SELECT then UPDATE for manually assigned IDs.
        // We use jdbcTemplate to force an INSERT.
        assertThrows(DataIntegrityViolationException.class, () -> {
            jdbcTemplate.update("INSERT INTO inbox_messages (event_id, consumer_group, event_type, status, received_at) VALUES (?, ?, ?, ?, ?)", 
                message2.getEventId(), message2.getConsumerGroup(), message2.getEventType(), message2.getStatus(), java.sql.Timestamp.from(message2.getReceivedAt()));
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
}
