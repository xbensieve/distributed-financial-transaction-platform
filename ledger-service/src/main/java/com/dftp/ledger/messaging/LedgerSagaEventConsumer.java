package com.dftp.ledger.messaging;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.LedgerPostRequested;
import com.dftp.common.event.payload.LedgerPostRejected;
import com.dftp.common.inbox.InboxMessage;
import com.dftp.common.inbox.InboxMessageRepository;
import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.ledger.application.LedgerApplicationService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.math.BigDecimal;

@Slf4j
@Service
@RequiredArgsConstructor
public class LedgerSagaEventConsumer {

    private final InboxMessageRepository inboxMessageRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final LedgerApplicationService ledgerApplicationService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "transaction-events", groupId = "ledger-service-saga-group")
    @Transactional
    public void consume(String message, Acknowledgment acknowledgment) throws Exception {
        try {
            JsonNode rootNode = objectMapper.readTree(message);
            JsonNode eventTypeNode = rootNode.get("eventType");
            if (eventTypeNode == null || eventTypeNode.isNull() || eventTypeNode.asText().isBlank()) {
                log.error("Malformed event: missing eventType in message: {}", message);
                throw new IllegalArgumentException("Malformed event: missing mandatory eventType");
            }
            String eventType = eventTypeNode.asText();
            
            JsonNode eventIdNode = rootNode.get("eventId");
            if (eventIdNode == null || eventIdNode.isNull() || eventIdNode.asText().isBlank()) {
                log.error("Malformed event: missing eventId in message: {}", message);
                throw new IllegalArgumentException("Malformed event: missing mandatory eventId");
            }
            UUID eventId = UUID.fromString(eventIdNode.asText());

            JsonNode versionNode = rootNode.get("eventVersion");
            if (versionNode != null && !versionNode.isNull() && !versionNode.asText().isBlank()) {
                String eventVersion = versionNode.asText();
                if (!eventVersion.startsWith("1")) {
                    log.error("Unsupported event version: {} for eventId: {}", eventVersion, eventId);
                    throw new IllegalArgumentException("Unsupported event version: " + eventVersion);
                }
            }

            JsonNode txNode = rootNode.get("transactionId");
            if (txNode != null && !txNode.isNull()) {
                MDC.put("transactionId", txNode.asText());
            }
            JsonNode corrNode = rootNode.get("correlationId");
            if (corrNode != null && !corrNode.isNull()) {
                MDC.put("correlationId", corrNode.asText());
            }
            JsonNode causeNode = rootNode.get("causationId");
            if (causeNode != null && !causeNode.isNull()) {
                MDC.put("causationId", causeNode.asText());
            }
            MDC.put("eventId", eventId.toString());

            if (!"LedgerPostRequested".equals(eventType)) {
                acknowledgment.acknowledge();
                return;
            }

            int inserted = inboxMessageRepository.insertIfAbsent(eventId, "ledger-service-saga-group", eventType);
            if (inserted == 0) {
                log.info("Duplicate saga event {} received (atomic deduplication), ignoring.", eventId);
                acknowledgment.acknowledge();
                return;
            }

            EventEnvelope<LedgerPostRequested> envelope = objectMapper.readValue(message, new TypeReference<>() {});

            if (envelope.getPayload().getAmount().compareTo(BigDecimal.ZERO) <= 0) {
                log.warn("Permanent Ledger Rejection for event {}: Transaction amount must be greater than zero", eventId);
                publishRejection(envelope, "Transaction amount must be greater than zero");
                acknowledgment.acknowledge();
                return;
            }

            if (envelope.getPayload().getSourceAccountId().equals(envelope.getPayload().getDestinationAccountId())) {
                log.warn("Permanent Ledger Rejection for event {}: Source and destination accounts must be different", eventId);
                publishRejection(envelope, "Source and destination accounts must be different");
                acknowledgment.acknowledge();
                return;
            }

            try {
                ledgerApplicationService.postTransaction(envelope);
            } catch (Exception e) {
                log.error("Failed to post transaction", e);
                throw e;
            }

            acknowledgment.acknowledge();

        } catch (IllegalArgumentException | com.fasterxml.jackson.core.JsonProcessingException e) {
            log.error("Permanent non-retryable ledger event error: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Failed to process ledger event", e);
            throw e;
        } finally {
            MDC.clear();
        }
    }



    public void publishRejection(EventEnvelope<LedgerPostRequested> originalEnvelope, String reason) {
        LedgerPostRejected payload = LedgerPostRejected.builder()
                .reason(reason)
                .build();

        EventEnvelope<LedgerPostRejected> rejectionEnvelope = EventEnvelope.<LedgerPostRejected>builder()
                .eventId(UUID.randomUUID())
                .eventType("LedgerPostRejected")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .correlationId(originalEnvelope.getCorrelationId())
                .transactionId(originalEnvelope.getTransactionId())
                .causationId(originalEnvelope.getEventId().toString())
                .producerService("ledger-service")
                .payload(payload)
                .build();

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .id(rejectionEnvelope.getEventId())
                .aggregateType("ledger-events")
                .aggregateId(originalEnvelope.getTransactionId())
                .eventType(rejectionEnvelope.getEventType())
                .payload(serializeEnvelope(rejectionEnvelope))
                .status("PENDING")
                .build();

        outboxEventRepository.saveAndFlush(outboxEvent);
    }

    @SneakyThrows
    private String serializeEnvelope(EventEnvelope<?> envelope) {
        return objectMapper.writeValueAsString(envelope);
    }
}
