package com.dftp.common.kafka;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.FundsHeld;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 07: Kafka Serialization Regression & Round-Trip Test.
 *
 * Verifies end-to-end:
 * Java Object → EventEnvelope → ObjectMapper → JSON String → Kafka StringSerializer
 * → Kafka bytes → StringDeserializer → ObjectMapper → EventEnvelope.
 *
 * Proves that:
 * 1. StringSerializer produces clean unescaped JSON bytes (Single-serialization).
 * 2. Deserializer yields an ObjectNode where eventType, eventId, and payload are directly accessible.
 * 3. Regression test proves that double serialization (JsonSerializer over JSON String) causes
 *    TextNode result and missing eventType, matching the Phase 06.10 root-cause analysis.
 */
class SerializationRoundTripTest {

    private ObjectMapper objectMapper;
    private StringSerializer stringSerializer;
    private StringDeserializer stringDeserializer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        stringSerializer = new StringSerializer();
        stringDeserializer = new StringDeserializer();
    }

    @Test
    @DisplayName("P0: Clean Round-Trip — EventEnvelope serializes and deserializes without double-escaping")
    void testEventEnvelopeCleanRoundTrip() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID sourceAccountId = UUID.randomUUID();
        String correlationId = UUID.randomUUID().toString();
        String transactionId = "tx-roundtrip-123";
        String causationId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        FundsHeld payload = FundsHeld.builder()
                .sourceAccountId(sourceAccountId)
                .amount(new BigDecimal("250.00"))
                .currency("USD")
                .build();

        EventEnvelope<FundsHeld> originalEnvelope = EventEnvelope.<FundsHeld>builder()
                .eventId(eventId)
                .eventType("FundsHeld")
                .eventVersion("1.0")
                .occurredAt(now)
                .correlationId(correlationId)
                .transactionId(transactionId)
                .causationId(causationId)
                .producerService("account-service")
                .payload(payload)
                .build();

        // 1. Outbox stores JSON string
        String outboxPayloadJson = objectMapper.writeValueAsString(originalEnvelope);
        assertThat(outboxPayloadJson).startsWith("{");
        assertThat(outboxPayloadJson).doesNotStartWith("\"");

        // 2. OutboxRelay publishes via Kafka StringSerializer
        byte[] kafkaBytes = stringSerializer.serialize("account-events", outboxPayloadJson);
        assertThat(kafkaBytes).isNotNull();

        // 3. Kafka Consumer reads via StringDeserializer
        String receivedMessage = stringDeserializer.deserialize("account-events", kafkaBytes);
        assertThat(receivedMessage).isEqualTo(outboxPayloadJson);

        // 4. Consumer parses root JsonNode
        JsonNode rootNode = objectMapper.readTree(receivedMessage);
        assertThat(rootNode.isObject()).isTrue();
        assertThat(rootNode.isTextual()).isFalse();

        // 5. Mandatory Envelope headers are present and correctly typed
        assertThat(rootNode.has("eventType")).isTrue();
        assertThat(rootNode.get("eventType").asText()).isEqualTo("FundsHeld");

        assertThat(rootNode.has("eventId")).isTrue();
        assertThat(UUID.fromString(rootNode.get("eventId").asText())).isEqualTo(eventId);

        assertThat(rootNode.has("transactionId")).isTrue();
        assertThat(rootNode.get("transactionId").asText()).isEqualTo(transactionId);

        assertThat(rootNode.has("correlationId")).isTrue();
        assertThat(rootNode.get("correlationId").asText()).isEqualTo(correlationId);

        assertThat(rootNode.has("causationId")).isTrue();
        assertThat(rootNode.get("causationId").asText()).isEqualTo(causationId);

        // 6. Full EventEnvelope typed deserialization
        EventEnvelope<FundsHeld> deserializedEnvelope = objectMapper.readValue(
                receivedMessage, new TypeReference<EventEnvelope<FundsHeld>>() {}
        );

        assertThat(deserializedEnvelope.getEventId()).isEqualTo(eventId);
        assertThat(deserializedEnvelope.getEventType()).isEqualTo("FundsHeld");
        assertThat(deserializedEnvelope.getEventVersion()).isEqualTo("1.0");
        assertThat(deserializedEnvelope.getTransactionId()).isEqualTo(transactionId);
        assertThat(deserializedEnvelope.getCorrelationId()).isEqualTo(correlationId);
        assertThat(deserializedEnvelope.getCausationId()).isEqualTo(causationId);
        assertThat(deserializedEnvelope.getPayload().getSourceAccountId()).isEqualTo(sourceAccountId);
        assertThat(deserializedEnvelope.getPayload().getAmount()).isEqualByComparingTo("250.00");
        assertThat(deserializedEnvelope.getPayload().getCurrency()).isEqualTo("USD");
    }

    @Test
    @DisplayName("P0 Regression Proof: JsonSerializer on already-serialized String causes TextNode failure")
    void testDoubleSerializationRegressionReproduction() throws Exception {
        UUID eventId = UUID.randomUUID();
        EventEnvelope<String> envelope = EventEnvelope.<String>builder()
                .eventId(eventId)
                .eventType("TestEvent")
                .eventVersion("1.0")
                .payload("test-data")
                .build();

        // Outbox payload string
        String jsonString = objectMapper.writeValueAsString(envelope);

        // Simulate incorrect double-serialization via JsonSerializer
        try (JsonSerializer<Object> jsonSerializer = new JsonSerializer<>()) {
            byte[] doubleSerializedBytes = jsonSerializer.serialize("test-topic", jsonString);

            // Consumer reads via StringDeserializer
            String badMessage = stringDeserializer.deserialize("test-topic", doubleSerializedBytes);

            // Notice the string is quoted and escaped: "\"{\\\"eventId\\\":...}\""
            assertThat(badMessage).startsWith("\"{\\\"");

            // Parsing it produces a TextNode rather than ObjectNode!
            JsonNode rootNode = objectMapper.readTree(badMessage);
            assertThat(rootNode.isTextual()).isTrue();
            assertThat(rootNode.isObject()).isFalse();

            // This causes rootNode.get("eventType") to be null — exactly reproducing the Phase 06.6 bug!
            assertThat(rootNode.get("eventType")).isNull();
        }
    }
}
