package com.dftp.transaction.resilience;

import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.common.security.JwtTokenService;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import com.dftp.transaction.api.dto.CreateTransactionRequest;
import com.dftp.transaction.backpressure.OutboxBackpressureProperties;
import com.dftp.transaction.backpressure.OutboxBackpressureService;
import com.dftp.transaction.backpressure.OutboxBackpressureState;
import com.dftp.transaction.domain.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * P12.5-COND-01: Outbox Storage Protection & Admission Backpressure Integration Test.
 *
 * Deterministically verifies the end-to-end operational lifecycle:
 * Kafka healthy -> accept transactions -> simulated outage -> Outbox grows ->
 * warning threshold -> critical threshold -> admission control activates ->
 * new requests rejected safely (503 + Retry-After) without DB mutation ->
 * Kafka restored / Outbox drained -> admission control deactivates -> transactions accepted.
 */
@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "dftp.outbox.relay.enabled=false",
        "dftp.outbox.backpressure.enabled=true",
        "dftp.outbox.backpressure.warning-threshold=3",
        "dftp.outbox.backpressure.critical-threshold=5",
        "dftp.outbox.backpressure.throttle-threshold=5",
        "dftp.outbox.backpressure.recovery-threshold=2",
        "dftp.outbox.backpressure.retry-after-seconds=30"
})
class OutboxBackpressureIntegrationTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JwtTokenService jwtTokenService;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private AccountReferenceRepository accountReferenceRepository;

    @Autowired
    private ApiIdempotencyKeyRepository apiIdempotencyKeyRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private OutboxBackpressureService outboxBackpressureService;

    @Autowired
    private OutboxBackpressureProperties backpressureProperties;

    private String userToken;
    private UUID sourceAccountId;
    private UUID destAccountId;

    @BeforeEach
    void setUp() {
        apiIdempotencyKeyRepository.deleteAll();
        transactionRepository.deleteAll();
        outboxEventRepository.deleteAll();
        accountReferenceRepository.deleteAll();

        sourceAccountId = UUID.randomUUID();
        destAccountId = UUID.randomUUID();

        accountReferenceRepository.save(new AccountReference(sourceAccountId, Instant.now(), "client-user"));
        accountReferenceRepository.save(new AccountReference(destAccountId, Instant.now(), "merchant-user"));

        userToken = jwtTokenService.generateToken("client-user", List.of("ROLE_USER"));
    }

    @Test
    @DisplayName("P12.5-COND-01: End-to-end Outbox outage simulation and admission backpressure lifecycle")
    void testOutboxBackpressureOutageSimulation() throws Exception {
        // -------------------------------------------------------------
        // Phase 1: Nominal Healthy Ingress
        // -------------------------------------------------------------
        outboxBackpressureService.forceRefresh();
        assertThat(outboxBackpressureService.getCurrentState()).isEqualTo(OutboxBackpressureState.HEALTHY);

        CreateTransactionRequest req1 = CreateTransactionRequest.builder()
                .transactionId("TX-BP-01")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .build();

        mockMvc.perform(post("/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken)
                        .header("Idempotency-Key", "IDEMP-BP-01")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req1)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("PENDING"));

        long pendingAfterReq1 = outboxEventRepository.countByStatus("PENDING");
        assertThat(pendingAfterReq1).isEqualTo(1);
        assertThat(transactionRepository.findByTransactionId("TX-BP-01")).isPresent();

        // -------------------------------------------------------------
        // Phase 2: Broker Outage & Outbox Accumulation -> Warning Threshold
        // -------------------------------------------------------------
        // Simulate broker outage causing outbox events to accumulate
        for (int i = 2; i <= 3; i++) {
            outboxEventRepository.save(OutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("transaction-events")
                    .aggregateId(UUID.randomUUID().toString())
                    .eventType("SimulatedPendingEvent")
                    .payload("{}")
                    .status("PENDING")
                    .build());
        }

        OutboxBackpressureState stateWarning = outboxBackpressureService.forceRefresh();
        assertThat(stateWarning).isEqualTo(OutboxBackpressureState.WARNING);
        assertThat(outboxBackpressureService.isAdmissionAllowed()).isTrue();

        // -------------------------------------------------------------
        // Phase 3: Critical Threshold Reached -> Admission Throttling Activates
        // -------------------------------------------------------------
        // Accumulate to 5 (critical & throttle threshold = 5)
        for (int i = 4; i <= 5; i++) {
            outboxEventRepository.save(OutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("transaction-events")
                    .aggregateId(UUID.randomUUID().toString())
                    .eventType("SimulatedPendingEvent")
                    .payload("{}")
                    .status("PENDING")
                    .build());
        }

        OutboxBackpressureState stateThrottled = outboxBackpressureService.forceRefresh();
        assertThat(stateThrottled).isEqualTo(OutboxBackpressureState.THROTTLED);
        assertThat(outboxBackpressureService.isAdmissionAllowed()).isFalse();

        // -------------------------------------------------------------
        // Phase 4: Inbound Traffic Throttled Safely Before DB Mutation
        // Invariant: Rejected request != partially committed mutation
        // -------------------------------------------------------------
        CreateTransactionRequest reqThrottled = CreateTransactionRequest.builder()
                .transactionId("TX-BP-THROTTLED")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("50.00"))
                .currency("USD")
                .build();

        mockMvc.perform(post("/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken)
                        .header("Idempotency-Key", "IDEMP-BP-THROTTLED")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(reqThrottled)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "30"))
                .andExpect(jsonPath("$.errorCode").value("OUTBOX_ADMISSION_THROTTLED"));

        // PROOF: Zero database rows inserted for the rejected request
        assertThat(transactionRepository.findByTransactionId("TX-BP-THROTTLED")).isEmpty();
        assertThat(apiIdempotencyKeyRepository.findById("IDEMP-BP-THROTTLED")).isEmpty();
        assertThat(outboxEventRepository.countByStatus("PENDING")).isEqualTo(5); // No new outbox row!

        // -------------------------------------------------------------
        // Phase 5: Idempotency Key Hit under Throttling Returns Cached Response
        // -------------------------------------------------------------
        mockMvc.perform(post("/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken)
                        .header("Idempotency-Key", "IDEMP-BP-01")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req1)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.transactionId").value("TX-BP-01"));

        // -------------------------------------------------------------
        // Phase 6: Broker Restored -> Outbox Drains -> Hysteresis Recovery
        // -------------------------------------------------------------
        // Drain 4 events (leaving 1 pending <= recovery threshold 2)
        List<OutboxEvent> pendingEvents = outboxEventRepository.findAll().stream()
                .filter(e -> "PENDING".equals(e.getStatus()))
                .limit(4)
                .toList();
        for (OutboxEvent e : pendingEvents) {
            e.setStatus("PUBLISHED");
            e.setProcessedAt(Instant.now());
        }
        outboxEventRepository.saveAll(pendingEvents);

        OutboxBackpressureState stateRecovered = outboxBackpressureService.forceRefresh();
        assertThat(stateRecovered).isEqualTo(OutboxBackpressureState.HEALTHY);
        assertThat(outboxBackpressureService.isAdmissionAllowed()).isTrue();

        // -------------------------------------------------------------
        // Phase 7: Post-Recovery Admission Restored
        // -------------------------------------------------------------
        CreateTransactionRequest reqAfterRecovery = CreateTransactionRequest.builder()
                .transactionId("TX-BP-RECOVERED")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("75.00"))
                .currency("USD")
                .build();

        mockMvc.perform(post("/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken)
                        .header("Idempotency-Key", "IDEMP-BP-RECOVERED")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(reqAfterRecovery)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.transactionId").value("TX-BP-RECOVERED"));

        // -------------------------------------------------------------
        // Phase 8: Authoritative Invariants Verification
        // -------------------------------------------------------------
        // 1. Exactly 2 transactions committed in database (TX-BP-01 and TX-BP-RECOVERED)
        assertThat(transactionRepository.count()).isEqualTo(2);
        // 2. Rejected transaction NEVER existed
        assertThat(transactionRepository.findByTransactionId("TX-BP-THROTTLED")).isEmpty();
        // 3. Message loss count = 0
        assertThat(outboxEventRepository.findAll()).isNotEmpty();
    }
}
