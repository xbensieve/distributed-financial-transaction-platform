package com.dftp.template;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.inbox.InboxMessage;
import com.dftp.common.inbox.InboxMessageRepository;
import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.common.outbox.OutboxRelay;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class OutboxInboxReliabilityIntegrationTest {

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
        registry.add("spring.kafka.consumer.group-id", () -> "test-group");
        registry.add("dftp.outbox.relay.polling-interval", () -> "1000"); // Poll quickly for tests
    }

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private InboxMessageRepository inboxMessageRepository;

    @Autowired
    private OutboxRelay outboxRelay;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static CountDownLatch messageLatch;
    private static EventEnvelope<?> receivedEnvelope;
    private static AtomicInteger retryCount = new AtomicInteger(0);

    @BeforeEach
    void setup() {
        outboxEventRepository.deleteAll();
        inboxMessageRepository.deleteAll();
        receivedEnvelope = null;
        retryCount.set(0);
    }

    @KafkaListener(topics = "TestAggregate", groupId = "test-group")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        String payload = record.value();

        if (payload.contains("POISON")) {
            // Simulate poison message handling by letting the JSON parser fail, or throwing exception
            throw new RuntimeException("Simulated Poison Message");
        }

        EventEnvelope<TestBusinessEvent> envelope = objectMapper.readValue(payload, new com.fasterxml.jackson.core.type.TypeReference<>() {});

        if ("FAIL_ONCE".equals(envelope.getEventType()) && retryCount.getAndIncrement() < 1) {
            throw new RuntimeException("Simulated processing failure for retry test");
        }

        try {
            InboxMessage inboxMessage = InboxMessage.builder()
                    .eventId(envelope.getEventId())
                    .consumerGroup("test-group")
                    .eventType(envelope.getEventType())
                    .build();
            inboxMessageRepository.saveAndFlush(inboxMessage);
        } catch (DataIntegrityViolationException e) {
            // Idempotent duplicate intercepted
            ack.acknowledge();
            return;
        }

        receivedEnvelope = envelope;
        
        if (messageLatch != null) {
            messageLatch.countDown();
        }

        ack.acknowledge();
    }

    @Test
    void pendingOutboxEvent_eventuallyPublishedAndConsumed() throws Exception {
        messageLatch = new CountDownLatch(1);
        UUID eventId = UUID.randomUUID();
        
        TestBusinessEvent bizEvent = new TestBusinessEvent("ACC-123", new BigDecimal("100.00"), "Test");
        EventEnvelope<TestBusinessEvent> envelope = EventEnvelope.<TestBusinessEvent>builder()
                .eventId(eventId)
                .eventType("TestEvent")
                .transactionId(UUID.randomUUID().toString())
                .payload(bizEvent)
                .build();

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .id(eventId)
                .aggregateType("TestAggregate")
                .aggregateId("ACC-123")
                .eventType("TestEvent")
                .payload(objectMapper.writeValueAsString(envelope))
                .build();

        outboxEventRepository.save(outboxEvent);

        // Allow OutboxRelay @Scheduled to pick it up and publish
        boolean messageReceived = messageLatch.await(10, TimeUnit.SECONDS);

        assertThat(messageReceived).isTrue();
        assertThat(receivedEnvelope).isNotNull();
        assertThat(receivedEnvelope.getEventId()).isEqualTo(eventId);
        assertThat(receivedEnvelope.getPayload()).isInstanceOf(TestBusinessEvent.class);
        
        // Verify DB status
        OutboxEvent processedEvent = outboxEventRepository.findById(eventId).orElseThrow();
        assertThat(processedEvent.getStatus()).isEqualTo("PUBLISHED");
    }

    @Test
    void relayCrashRecovery_claimedEventsRepublished() throws Exception {
        messageLatch = new CountDownLatch(1);
        UUID eventId = UUID.randomUUID();
        
        TestBusinessEvent bizEvent = new TestBusinessEvent("ACC-123", new BigDecimal("100.00"), "Crash Test");
        EventEnvelope<TestBusinessEvent> envelope = EventEnvelope.<TestBusinessEvent>builder()
                .eventId(eventId)
                .eventType("TestEvent")
                .payload(bizEvent)
                .build();

        // Simulate an event that was CLAIMED but the process crashed before PUBLISHED.
        // We set updatedAt to 3 minutes ago to exceed the 2-minute recovery window.
        OutboxEvent outboxEvent = OutboxEvent.builder()
                .id(eventId)
                .aggregateType("TestAggregate")
                .aggregateId("ACC-123")
                .eventType("TestEvent")
                .status("CLAIMED")
                .updatedAt(Instant.now().minus(3, ChronoUnit.MINUTES)) 
                .payload(objectMapper.writeValueAsString(envelope))
                .build();

        outboxEventRepository.save(outboxEvent);

        boolean messageReceived = messageLatch.await(10, TimeUnit.SECONDS);

        assertThat(messageReceived).as("Relay must recover timed-out CLAIMED events").isTrue();
    }
}
