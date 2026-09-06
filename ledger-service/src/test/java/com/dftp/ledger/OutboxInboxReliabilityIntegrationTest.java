package com.dftp.ledger;

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
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@TestPropertySource(properties = {
    "spring.kafka.consumer.group-id=test-group",
    "dftp.outbox.relay.polling-interval=1000"
})
class OutboxInboxReliabilityIntegrationTest extends AbstractLedgerIntegrationTest {

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

    @Autowired
    private org.springframework.transaction.PlatformTransactionManager transactionManager;

    private static CountDownLatch messageLatch;
    private static EventEnvelope<?> receivedEnvelope;
    private static AtomicInteger retryCount = new AtomicInteger(0);
    private static AtomicInteger businessEffectCount = new AtomicInteger(0);
    private static AtomicInteger duplicateInterceptedCount = new AtomicInteger(0);

    @BeforeEach
    void setup() {
        outboxEventRepository.deleteAll();
        inboxMessageRepository.deleteAll();
        receivedEnvelope = null;
        retryCount.set(0);
        businessEffectCount.set(0);
        duplicateInterceptedCount.set(0);
    }

    @KafkaListener(topics = "TestAggregate", groupId = "test-group")
    @org.springframework.transaction.annotation.Transactional
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

        Integer inserted = new org.springframework.transaction.support.TransactionTemplate(transactionManager).execute(status ->
            inboxMessageRepository.insertIfAbsent(envelope.getEventId(), "test-group", envelope.getEventType())
        );
        if (inserted != null && inserted == 0) {
            // Idempotent duplicate intercepted atomically
            duplicateInterceptedCount.incrementAndGet();
            if (messageLatch != null) {
                messageLatch.countDown();
            }
            ack.acknowledge();
            return;
        }

        // Simulate single business effect
        businessEffectCount.incrementAndGet();
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
        
        // Verify DB status (use awaitility to prevent race condition with relay thread committing)
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            OutboxEvent processedEvent = outboxEventRepository.findById(eventId).orElseThrow();
            assertThat(processedEvent.getStatus()).isEqualTo("PUBLISHED");
        });
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

    @Test
    void outboxCrashWindow_postKafkaAckPreCommit_recoveryAndDuplicateSuppression() throws Exception {
        // SCENARIO: Kafka send succeeded on first try, consumer processed it and inserted Inbox record,
        // BUT publisher crashed before 'PUBLISHED' was committed to DB.
        // Therefore, outbox record remained stuck in 'CLAIMED' state with stale timestamp.
        UUID eventId = UUID.randomUUID();
        
        // 1. Consumer already processed the first message delivery and committed Inbox record
        new org.springframework.transaction.support.TransactionTemplate(transactionManager).execute(status -> {
            return inboxMessageRepository.insertIfAbsent(eventId, "test-group", "TestEvent");
        });
        assertThat(inboxMessageRepository.existsByEventId(eventId)).isTrue();

        // 2. Publisher DB state: stuck in CLAIMED (updated 3 minutes ago, simulating interrupted commit after Kafka ACK)
        TestBusinessEvent bizEvent = new TestBusinessEvent("ACC-CRASH-001", new BigDecimal("250.00"), "Crash Window Test");
        EventEnvelope<TestBusinessEvent> envelope = EventEnvelope.<TestBusinessEvent>builder()
                .eventId(eventId)
                .eventType("TestEvent")
                .payload(bizEvent)
                .build();

        OutboxEvent interruptedOutbox = OutboxEvent.builder()
                .id(eventId)
                .aggregateType("TestAggregate")
                .aggregateId("ACC-CRASH-001")
                .eventType("TestEvent")
                .status("CLAIMED")
                .updatedAt(Instant.now().minus(3, ChronoUnit.MINUTES))
                .payload(objectMapper.writeValueAsString(envelope))
                .build();

        outboxEventRepository.save(interruptedOutbox);

        messageLatch = new CountDownLatch(1);

        // 3. OutboxRelay background worker detects stale CLAIMED row (> 2 minutes),
        // reclaims it, and republishes to Kafka.
        boolean duplicateReceived = messageLatch.await(10, TimeUnit.SECONDS);
        assertThat(duplicateReceived).as("Duplicate event must be delivered to consumer during recovery").isTrue();

        // 4. Verify consumer suppressed duplicate: zero duplicate business effects
        assertThat(duplicateInterceptedCount.get())
                .as("Consumer Inbox must atomically intercept the redelivered duplicate")
                .isGreaterThanOrEqualTo(1);
        assertThat(businessEffectCount.get())
                .as("Business mutation must NOT execute more than once under replay")
                .isEqualTo(0);

        // 5. Verify Outbox row successfully transitions to PUBLISHED after recovery
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            OutboxEvent recovered = outboxEventRepository.findById(eventId).orElseThrow();
            assertThat(recovered.getStatus()).isEqualTo("PUBLISHED");
            assertThat(recovered.getProcessedAt()).isNotNull();
        });
    }
}
