package com.dftp.transaction.messaging;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.FundsHeld;
import com.dftp.common.event.payload.FundsHoldRejected;
import com.dftp.common.event.payload.FundsSettled;
import com.dftp.common.event.payload.HoldCompensated;
import com.dftp.common.event.payload.LedgerPostRejected;
import com.dftp.common.inbox.InboxMessage;
import com.dftp.common.inbox.InboxMessageRepository;
import com.dftp.transaction.application.TransactionApplicationService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.dftp.common.observability.TraceContextPropagator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.slf4j.MDC;

import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionSagaEventConsumer {

    private final InboxMessageRepository inboxMessageRepository;
    private final TransactionApplicationService applicationService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = {"account-events", "ledger-events"}, groupId = "transaction-service-saga-group")
    @Transactional
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) throws Exception {
        String message = record.value();
        TraceContextPropagator.extractFromKafkaHeaders(record.headers())
                .ifPresent(TraceContextPropagator::populateMdc);
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

            int inserted = inboxMessageRepository.insertIfAbsent(eventId, "transaction-service-saga-group", eventType);
            if (inserted == 0) {
                log.info("Duplicate saga event {} received (atomic deduplication), ignoring.", eventId);
                acknowledgment.acknowledge();
                return;
            }

            switch (eventType) {
                case "FundsHeld": {
                    EventEnvelope<FundsHeld> envelope = objectMapper.readValue(message, new TypeReference<>() {});
                    applicationService.handleFundsHeld(envelope);
                    break;
                }
                case "FundsHoldRejected": {
                    EventEnvelope<FundsHoldRejected> envelope = objectMapper.readValue(message, new TypeReference<>() {});
                    applicationService.handleFundsHoldRejected(envelope);
                    break;
                }
                case "LedgerTransactionPosted": {
                    EventEnvelope<JsonNode> envelope = objectMapper.readValue(message, new TypeReference<>() {});
                    applicationService.handleLedgerTransactionPosted(envelope);
                    break;
                }
                case "LedgerPostRejected": {
                    EventEnvelope<LedgerPostRejected> envelope = objectMapper.readValue(message, new TypeReference<>() {});
                    applicationService.handleLedgerPostRejected(envelope);
                    break;
                }
                case "FundsSettled": {
                    EventEnvelope<FundsSettled> envelope = objectMapper.readValue(message, new TypeReference<>() {});
                    applicationService.handleFundsSettled(envelope);
                    break;
                }
                case "HoldCompensated": {
                    EventEnvelope<HoldCompensated> envelope = objectMapper.readValue(message, new TypeReference<>() {});
                    applicationService.handleHoldCompensated(envelope);
                    break;
                }
                case "AccountCreated": {
                    EventEnvelope<com.dftp.transaction.domain.event.AccountCreated> envelope = objectMapper.readValue(message, new TypeReference<>() {});
                    applicationService.handleAccountCreated(envelope);
                    break;
                }
                default:
                    // Ignore unknown events
                    log.debug("Ignored event type {}", eventType);
                    break;
            }

            acknowledgment.acknowledge();

        } catch (IllegalArgumentException | com.fasterxml.jackson.core.JsonProcessingException e) {
            log.error("Permanent non-retryable saga event error: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Failed to process saga event", e);
            throw e;
        } finally {
            TraceContextPropagator.clearMdc();
            MDC.clear();
        }
    }

    public void consume(String message, Acknowledgment acknowledgment) throws Exception {
        consume(new ConsumerRecord<>("saga-events", 0, 0L, null, message), acknowledgment);
    }
}
