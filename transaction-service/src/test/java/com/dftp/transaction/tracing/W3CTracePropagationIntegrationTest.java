package com.dftp.transaction.tracing;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.common.observability.TraceContextPropagator;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import com.dftp.transaction.api.dto.CreateTransactionRequest;
import com.dftp.transaction.api.dto.TransactionResponse;
import com.dftp.transaction.domain.AccountReference;
import com.dftp.transaction.domain.AccountReferenceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration Test for Phase 14 / ADR-014: W3C Distributed Tracing Propagation.
 *
 * Verifies end-to-end trace propagation across:
 * 1. HTTP inbound request (traceparent & tracestate headers).
 * 2. OutboxEvent persistence in PostgreSQL (traceparent & tracestate columns).
 * 3. Kafka ProducerRecord headers emitted by OutboxRelay.
 * 4. Preservation of 32-char hex Trace ID across boundaries.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "dftp.outbox.relay.enabled=true",
        "dftp.outbox.relay.poll-interval-ms=100",
        "dftp.shedlock.enabled=false"
})
public class W3CTracePropagationIntegrationTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountReferenceRepository accountReferenceRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private com.dftp.common.security.JwtTokenService jwtTokenService;

    @org.springframework.boot.test.web.server.LocalServerPort
    private int port;

    private KafkaConsumer<String, String> testConsumer;

    @BeforeEach
    void setUp() {
        accountReferenceRepository.deleteAll();

        // Kafka consumer to assert headers
        testConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "trace-test-group-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        ));
        testConsumer.subscribe(Collections.singletonList("transaction-events"));
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    @DisplayName("Inbound W3C traceparent header propagates to Outbox record and Kafka message header")
    void testEndToEndTracePropagation() throws Exception {
        // Setup source and destination accounts
        UUID sourceAccountId = UUID.randomUUID();
        UUID destinationAccountId = UUID.randomUUID();

        accountReferenceRepository.save(new AccountReference(sourceAccountId, java.time.Instant.now(), "trace-user"));
        accountReferenceRepository.save(new AccountReference(destinationAccountId, java.time.Instant.now(), "dest-user"));

        String token = jwtTokenService.generateToken("trace-user", List.of("ROLE_USER"));

        // Fixed W3C traceparent and tracestate
        String expectedTraceId = "4bf92f3577b34da6a3ce929d0e0e4736";
        String expectedParentSpan = "00f067aa0ba902b7";
        String clientTraceparent = "00-" + expectedTraceId + "-" + expectedParentSpan + "-01";
        String clientTracestate = "congo=t61rcWkgMzE,rojo=123";

        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        headers.set("traceparent", clientTraceparent);
        headers.set("tracestate", clientTracestate);
        headers.set("Idempotency-Key", UUID.randomUUID().toString());

        String clientTxId = "tx-trace-" + UUID.randomUUID();
        CreateTransactionRequest request = new CreateTransactionRequest(
                clientTxId,
                sourceAccountId,
                destinationAccountId,
                new BigDecimal("250.00"),
                "USD"
        );

        HttpEntity<CreateTransactionRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<TransactionResponse> response = restTemplate.exchange(
                "http://localhost:" + port + "/transactions",
                HttpMethod.POST,
                entity,
                TransactionResponse.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        String transactionId = response.getBody().getTransactionId();

        // 1. Verify HTTP Response contains traceparent header with matching Trace ID
        String responseTraceparent = response.getHeaders().getFirst(TraceContextPropagator.TRACEPARENT_HEADER);
        assertThat(responseTraceparent).isNotNull();
        assertThat(responseTraceparent).contains(expectedTraceId);

        // 2. Verify OutboxEvent stored in database has traceparent and tracestate
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<OutboxEvent> events = outboxEventRepository.findAll();
            OutboxEvent outboxEvent = events.stream()
                    .filter(e -> e.getPayload() != null && e.getPayload().contains(transactionId))
                    .findFirst()
                    .orElse(null);
            assertThat(outboxEvent).isNotNull();
            assertThat(outboxEvent.getTraceparent()).isNotNull();
            assertThat(outboxEvent.getTraceparent()).contains(expectedTraceId);
            assertThat(outboxEvent.getTracestate()).isEqualTo(clientTracestate);
        });

        // 3. Verify Kafka message consumed on 'transaction-events' has W3C trace headers
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, String> records = testConsumer.poll(Duration.ofMillis(200));
            boolean found = false;
            for (ConsumerRecord<String, String> record : records) {
                if (record.value().contains(transactionId.toString())) {
                    found = true;

                    // Assert traceparent header
                    Header traceHeader = record.headers().lastHeader(TraceContextPropagator.TRACEPARENT_HEADER);
                    assertThat(traceHeader).isNotNull();
                    String kafkaTraceparent = new String(traceHeader.value(), StandardCharsets.UTF_8);
                    assertThat(kafkaTraceparent).contains(expectedTraceId);

                    // Assert tracestate header
                    Header stateHeader = record.headers().lastHeader(TraceContextPropagator.TRACESTATE_HEADER);
                    assertThat(stateHeader).isNotNull();
                    String kafkaTracestate = new String(stateHeader.value(), StandardCharsets.UTF_8);
                    assertThat(kafkaTracestate).isEqualTo(clientTracestate);
                }
            }
            assertThat(found).isTrue();
        });
    }
}
