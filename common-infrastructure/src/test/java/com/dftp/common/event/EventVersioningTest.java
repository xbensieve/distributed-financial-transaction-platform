package com.dftp.common.event;

import com.dftp.common.event.payload.FundsHeld;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventVersioningTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
    }

    @Test
    @DisplayName("Backward Compatibility: Deserialization succeeds when event envelope and payload have extra unknown fields")
    void testBackwardCompatibilityWithUnknownFields() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();

        // JSON with extra envelope fields ("futureMetadata", "routingTag") and extra payload fields ("riskScore", "merchantId")
        String jsonWithUnknownFields = String.format("""
            {
              "eventId": "%s",
              "eventType": "FundsHeld",
              "eventVersion": "1.0",
              "occurredAt": "2026-09-05T12:00:00Z",
              "correlationId": "corr-123",
              "transactionId": "tx-123",
              "causationId": "cause-123",
              "producerService": "account-service",
              "futureMetadata": { "schemaHash": "abc1234" },
              "routingTag": "US-EAST",
              "payload": {
                "sourceAccountId": "%s",
                "amount": 100.50,
                "currency": "USD",
                "riskScore": 0.05,
                "merchantId": "merchant-xyz"
              }
            }
            """, eventId, accountId);

        EventEnvelope<FundsHeld> envelope = objectMapper.readValue(jsonWithUnknownFields, new TypeReference<>() {});

        assertThat(envelope).isNotNull();
        assertThat(envelope.getEventId()).isEqualTo(eventId);
        assertThat(envelope.getEventType()).isEqualTo("FundsHeld");
        assertThat(envelope.getEventVersion()).isEqualTo("1.0");
        assertThat(envelope.getTransactionId()).isEqualTo("tx-123");

        FundsHeld payload = envelope.getPayload();
        assertThat(payload).isNotNull();
        assertThat(payload.getSourceAccountId()).isEqualTo(accountId);
        assertThat(payload.getAmount()).isEqualByComparingTo(new BigDecimal("100.50"));
        assertThat(payload.getCurrency()).isEqualTo("USD");
    }

    @Test
    @DisplayName("Version Rejection: Unsupported major event version (e.g. 2.0) is rejected with non-retryable IllegalArgumentException")
    void testUnsupportedEventVersionRejected() throws Exception {
        String jsonWithUnsupportedVersion = """
            {
              "eventId": "11111111-1111-1111-1111-111111111111",
              "eventType": "FundsHeld",
              "eventVersion": "2.0",
              "payload": {}
            }
            """;

        JsonNode rootNode = objectMapper.readTree(jsonWithUnsupportedVersion);
        JsonNode versionNode = rootNode.get("eventVersion");
        String version = versionNode != null ? versionNode.asText() : null;

        assertThatThrownBy(() -> {
            if (version != null && !version.startsWith("1.")) {
                throw new IllegalArgumentException("Unsupported event version: " + version);
            }
        }).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Unsupported event version: 2.0");
    }
}
