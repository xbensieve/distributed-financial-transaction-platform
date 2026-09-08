package com.dftp.account.messaging;

import com.dftp.account.application.AccountApplicationService;
import com.dftp.account.domain.event.AccountCreated;
import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.FundsHoldRequested;
import com.dftp.common.event.payload.FundsSettlementRequested;
import com.dftp.common.event.payload.HoldCompensationRequested;
import com.dftp.common.inbox.InboxMessage;
import com.dftp.common.inbox.InboxMessageRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.dftp.common.observability.TraceContextPropagator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.slf4j.MDC;

import java.util.Optional;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final InboxMessageRepository inboxMessageRepository;
    private final AccountApplicationService applicationService;
    private final ObjectMapper objectMapper;
    private static final String CONSUMER_GROUP = "account-service-group";

    @KafkaListener(topics = {"account-events", "transaction-events"}, groupId = CONSUMER_GROUP)
    @Transactional
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) throws Exception {
        if (record != null && record.headers() != null) {
            TraceContextPropagator.extractFromKafkaHeaders(record.headers())
                    .ifPresent(TraceContextPropagator::populateMdc);
        }
        consume(record != null ? record.value() : null, acknowledgment);
    }

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

            int inserted = inboxMessageRepository.insertIfAbsent(eventId, CONSUMER_GROUP, eventType);
            if (inserted == 0) {
                log.info("Event {} already processed by {} (atomic deduplication), skipping.", eventId, CONSUMER_GROUP);
                acknowledgment.acknowledge();
                return;
            }

            switch (eventType) {
                case "AccountCreated": {
                    EventEnvelope<AccountCreated> envelope = objectMapper.readValue(message, new TypeReference<>() {});
                    log.info("Handling AccountCreated for accountId: {}, correlationId: {}", envelope.getPayload().getAccountId(), envelope.getCorrelationId());
                    break;
                }
                case "FundsHoldRequested": {
                    EventEnvelope<FundsHoldRequested> envelope = objectMapper.readValue(message, new TypeReference<>() {});
                    FundsHoldRequested payload = envelope.getPayload();
                    applicationService.holdFunds(payload.getSourceAccountId(), payload.getAmount(), envelope.getTransactionId(), envelope.getCorrelationId(), eventId.toString());
                    break;
                }
                case "FundsSettlementRequested": {
                    EventEnvelope<FundsSettlementRequested> envelope = objectMapper.readValue(message, new TypeReference<>() {});
                    FundsSettlementRequested payload = envelope.getPayload();
                    applicationService.settleFunds(payload.getSourceAccountId(), payload.getDestinationAccountId(), payload.getAmount(), envelope.getTransactionId(), envelope.getCorrelationId(), eventId.toString());
                    break;
                }
                case "HoldCompensationRequested": {
                    EventEnvelope<HoldCompensationRequested> envelope = objectMapper.readValue(message, new TypeReference<>() {});
                    HoldCompensationRequested payload = envelope.getPayload();
                    applicationService.compensateHold(payload.getSourceAccountId(), payload.getAmount(), envelope.getTransactionId(), envelope.getCorrelationId(), eventId.toString());
                    break;
                }
                default:
                    log.debug("Ignored event type {}", eventType);
                    break;
            }

            acknowledgment.acknowledge();

        } catch (IllegalArgumentException | com.fasterxml.jackson.core.JsonProcessingException e) {
            log.error("Permanent non-retryable event error: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Failed to process event", e);
            throw e;
        } finally {
            TraceContextPropagator.clearMdc();
            MDC.clear();
        }
    }
}
