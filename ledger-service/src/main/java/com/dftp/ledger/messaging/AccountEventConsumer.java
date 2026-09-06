package com.dftp.ledger.messaging;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.inbox.InboxMessage;
import com.dftp.common.inbox.InboxMessageRepository;
import com.dftp.ledger.domain.AccountReference;
import com.dftp.ledger.domain.AccountReferenceRepository;
import com.dftp.ledger.domain.event.AccountCreated;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final InboxMessageRepository inboxMessageRepository;
    private final AccountReferenceRepository accountReferenceRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "account-events", groupId = "ledger-service-account-group")
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
            
            if (!"AccountCreated".equals(eventType)) {
                acknowledgment.acknowledge();
                return;
            }

            EventEnvelope<AccountCreated> envelope = objectMapper.readValue(
                    message, new TypeReference<>() {}
            );

            UUID eventId = envelope.getEventId();
            if (eventId == null) {
                log.error("Malformed event: missing eventId in message: {}", message);
                throw new IllegalArgumentException("Malformed event: missing mandatory eventId");
            }

            JsonNode versionNode = rootNode.get("eventVersion");
            if (versionNode != null && !versionNode.isNull() && !versionNode.asText().isBlank()) {
                String eventVersion = versionNode.asText();
                if (!eventVersion.startsWith("1")) {
                    log.error("Unsupported event version: {} for eventId: {}", eventVersion, eventId);
                    throw new IllegalArgumentException("Unsupported event version: " + eventVersion);
                }
            }

            int inserted = inboxMessageRepository.insertIfAbsent(eventId, "ledger-service-account-group", eventType);
            if (inserted == 0) {
                log.info("Duplicate account event {} received (atomic deduplication), ignoring.", eventId);
                acknowledgment.acknowledge();
                return;
            }

            log.info("Processing AccountCreated event: {}", eventId);

            AccountCreated payload = envelope.getPayload();
            accountReferenceRepository.insertIfAbsent(UUID.randomUUID(), payload.getAccountId(), "ACTIVE");

            log.info("Successfully processed account event {}", eventId);
            acknowledgment.acknowledge();

        } catch (IllegalArgumentException | com.fasterxml.jackson.core.JsonProcessingException e) {
            log.error("Permanent non-retryable account event error: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Failed to process account event", e);
            throw e;
        }
    }
}
