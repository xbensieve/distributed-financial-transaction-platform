package com.dftp.transaction.resilience;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.HoldCompensated;
import com.dftp.common.model.SnapshotResult;
import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.common.security.JwtTokenService;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import com.dftp.transaction.application.TransactionApplicationService;
import com.dftp.transaction.domain.AccountReference;
import com.dftp.transaction.domain.AccountReferenceRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import com.dftp.transaction.reconciliation.client.AccountServiceClient;
import com.dftp.transaction.reconciliation.client.LedgerServiceClient;
import com.dftp.transaction.reconciliation.client.dto.AccountSnapshotDto;
import com.dftp.transaction.reconciliation.client.dto.LedgerSnapshotDto;
import com.dftp.transaction.security.SecurityAuditLog;
import com.dftp.transaction.security.SecurityAuditLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * P12.5-COND-02: Stalled Saga Operational Recovery & Compensation Idempotency Test.
 *
 * Verifies:
 * 1. Operator diagnosis and verified eligible compensation path:
 *    SOURCE_HELD -> operator diagnosis -> verified compensation -> COMPENSATING -> FAILED
 * 2. Mandatory safety verifications (hold exists, ledger absent, no settlement, no prior compensation).
 * 3. Immutable audit trail on manual intervention.
 * 4. Crash-and-retry idempotency (safe no-op on duplicate operator calls).
 * 5. Rejection of unsafe compensation when ledger posting exists.
 */
@SpringBootTest
@AutoConfigureMockMvc
class StalledSagaOperationalRecoveryTest extends AbstractTransactionIntegrationTest {

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
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private SecurityAuditLogRepository securityAuditLogRepository;

    @Autowired
    private TransactionApplicationService transactionApplicationService;

    @MockBean
    private AccountServiceClient accountServiceClient;

    @MockBean
    private LedgerServiceClient ledgerServiceClient;

    private String adminToken;
    private UUID sourceAccountId;
    private UUID destAccountId;

    @BeforeEach
    void setUp() {
        securityAuditLogRepository.deleteAll();
        transactionRepository.deleteAll();
        outboxEventRepository.deleteAll();
        accountReferenceRepository.deleteAll();

        sourceAccountId = UUID.randomUUID();
        destAccountId = UUID.randomUUID();

        accountReferenceRepository.save(new AccountReference(sourceAccountId, Instant.now(), "client-user"));
        accountReferenceRepository.save(new AccountReference(destAccountId, Instant.now(), "merchant-user"));

        adminToken = jwtTokenService.generateToken("admin-operator", List.of("ROLE_ADMIN"));
    }

    @Test
    @DisplayName("P12.5-COND-02: Operator diagnoses and triggers verified compensation on stalled SOURCE_HELD saga")
    void testSuccessfulStalledSagaCompensation() throws Exception {
        String txId = "TX-STALLED-01";
        Instant stalledTime = Instant.now().minus(20, ChronoUnit.MINUTES); // > 15m threshold

        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("client-user")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("250.00"))
                .currency("USD")
                .status("SOURCE_HELD")
                .createdAt(stalledTime)
                .updatedAt(stalledTime)
                .build();
        tx = transactionRepository.save(tx);

        // Mock Account Service: Hold exists, no Settle, no Compensate
        AccountSnapshotDto.AccountOperationDto holdOp = AccountSnapshotDto.AccountOperationDto.builder()
                .id(UUID.randomUUID())
                .accountId(sourceAccountId)
                .operationType("HOLD")
                .createdAt(stalledTime)
                .build();
        when(accountServiceClient.getOperationsByTransaction(eq(txId)))
                .thenReturn(SnapshotResult.found(List.of(holdOp)));

        // Mock Ledger Service: Transaction NOT found (404)
        when(ledgerServiceClient.getLedgerSnapshot(eq(txId)))
                .thenReturn(SnapshotResult.notFound("404 Not Found"));

        // Trigger manual compensation via Administrative Endpoint
        mockMvc.perform(post("/admin/saga/stalled/" + txId + "/compensate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .param("reason", "Operator forensic diagnosis: ledger partitioned, hold orphaned > 15m")
                        .header("X-Correlation-Id", "CORR-STALLED-01"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transactionId").value(txId))
                .andExpect(jsonPath("$.previousStatus").value("SOURCE_HELD"))
                .andExpect(jsonPath("$.newStatus").value("COMPENSATING"))
                .andExpect(jsonPath("$.resolutionStatus").value("COMPENSATION_INITIATED"));

        // Verify DB State
        Transaction updatedTx = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(updatedTx.getStatus()).isEqualTo("COMPENSATING");

        // Verify Outbox Event Emitted for Downstream Account Service
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAll();
        assertThat(outboxEvents).hasSize(1);
        OutboxEvent event = outboxEvents.get(0);
        assertThat(event.getEventType()).isEqualTo("HoldCompensationRequested");
        assertThat(event.getAggregateType()).isEqualTo("transaction-events");
        assertThat(event.getPayload()).contains(sourceAccountId.toString());

        // Verify Immutable Security Audit Log Entry
        List<SecurityAuditLog> auditLogs = securityAuditLogRepository.findAll();
        assertThat(auditLogs).hasSize(1);
        SecurityAuditLog log = auditLogs.get(0);
        assertThat(log.getPrincipal()).isEqualTo("admin-operator");
        assertThat(log.getAction()).isEqualTo("MANUAL_SAGA_COMPENSATION");
        assertThat(log.getTargetResource()).isEqualTo(txId);
        assertThat(log.getStatus()).isEqualTo("SUCCESS");
        assertThat(log.getDetails()).contains("verifiedHold");
        assertThat(log.getDetails()).contains("verifiedLedgerAbsent");
    }

    @Test
    @DisplayName("P12.5-COND-02: Compensation idempotency under crash retry and terminal convergence")
    void testCompensationIdempotencyAndCrashRetry() throws Exception {
        String txId = "TX-STALLED-IDEMP";
        Instant stalledTime = Instant.now().minus(25, ChronoUnit.MINUTES);

        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("client-user")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("150.00"))
                .currency("USD")
                .status("SOURCE_HELD")
                .createdAt(stalledTime)
                .updatedAt(stalledTime)
                .build();
        transactionRepository.save(tx);

        AccountSnapshotDto.AccountOperationDto holdOp = AccountSnapshotDto.AccountOperationDto.builder()
                .id(UUID.randomUUID())
                .accountId(sourceAccountId)
                .operationType("HOLD")
                .createdAt(stalledTime)
                .build();
        when(accountServiceClient.getOperationsByTransaction(eq(txId)))
                .thenReturn(SnapshotResult.found(List.of(holdOp)));
        when(ledgerServiceClient.getLedgerSnapshot(eq(txId)))
                .thenReturn(SnapshotResult.notFound("404 Not Found"));

        // First Invocation: Transitions to COMPENSATING
        mockMvc.perform(post("/admin/saga/stalled/" + txId + "/compensate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .param("reason", "First attempt by operator"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.resolutionStatus").value("COMPENSATION_INITIATED"));

        assertThat(outboxEventRepository.count()).isEqualTo(1);

        // Crash & Retry Simulation: Operator retries while transaction is COMPENSATING
        mockMvc.perform(post("/admin/saga/stalled/" + txId + "/compensate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .param("reason", "Operator retrying after simulated process restart"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.resolutionStatus").value("SAFE_NOOP"))
                .andExpect(jsonPath("$.newStatus").value("COMPENSATING"));

        // PROOF: Zero duplicate outbox events created on retry
        assertThat(outboxEventRepository.count()).isEqualTo(1);

        // Downstream Execution: Account Service completes compensation and emits HoldCompensated
        EventEnvelope<HoldCompensated> compensatedEnvelope = EventEnvelope.<HoldCompensated>builder()
                .eventId(UUID.randomUUID())
                .eventType("HoldCompensated")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .correlationId("CORR-COMP-01")
                .producerService("account-service")
                .payload(HoldCompensated.builder()
                        .sourceAccountId(sourceAccountId)
                        .amount(new BigDecimal("150.00"))
                        .build())
                .build();

        transactionApplicationService.handleHoldCompensated(compensatedEnvelope);

        // Verify transaction reached FAILED terminal state
        Transaction finalTx = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(finalTx.getStatus()).isEqualTo("FAILED");

        // Third Invocation: Operator retries after transaction reached FAILED
        mockMvc.perform(post("/admin/saga/stalled/" + txId + "/compensate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .param("reason", "Operator re-checking failed transaction"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.resolutionStatus").value("SAFE_NOOP"))
                .andExpect(jsonPath("$.newStatus").value("FAILED"));

        // Final Invariant Proof: Outbox event count remains exactly 1; transaction remains FAILED
        assertThat(outboxEventRepository.count()).isEqualTo(1);
        assertThat(transactionRepository.findByTransactionId(txId).orElseThrow().getStatus()).isEqualTo("FAILED");
    }

    @Test
    @DisplayName("P12.5-COND-02: Rejects unsafe compensation when ledger posting exists concurrently")
    void testUnsafeCompensationRejectedWhenLedgerExists() throws Exception {
        String txId = "TX-UNSAFE-LEDGER";
        Instant stalledTime = Instant.now().minus(18, ChronoUnit.MINUTES);

        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("client-user")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("300.00"))
                .currency("USD")
                .status("SOURCE_HELD")
                .createdAt(stalledTime)
                .updatedAt(stalledTime)
                .build();
        transactionRepository.save(tx);

        AccountSnapshotDto.AccountOperationDto holdOp = AccountSnapshotDto.AccountOperationDto.builder()
                .id(UUID.randomUUID())
                .accountId(sourceAccountId)
                .operationType("HOLD")
                .createdAt(stalledTime)
                .build();
        when(accountServiceClient.getOperationsByTransaction(eq(txId)))
                .thenReturn(SnapshotResult.found(List.of(holdOp)));

        // Ledger transaction WAS actually posted in ledger-service
        LedgerSnapshotDto ledgerSnapshot = LedgerSnapshotDto.builder()
                .ledgerTransactionId("LTX-ACTIVE-001")
                .businessTransactionId(txId)
                .status("POSTED")
                .build();
        when(ledgerServiceClient.getLedgerSnapshot(eq(txId)))
                .thenReturn(SnapshotResult.found(ledgerSnapshot));

        // Attempting compensation must be REJECTED to preserve double-entry balance invariants
        mockMvc.perform(post("/admin/saga/stalled/" + txId + "/compensate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .param("reason", "Operator attempts unsafe compensation"))
                .andExpect(status().isConflict());

        // PROOF: Transaction was NOT modified
        Transaction untouched = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(untouched.getStatus()).isEqualTo("SOURCE_HELD");

        // PROOF: Zero compensation outbox events generated
        assertThat(outboxEventRepository.count()).isEqualTo(0);

        // PROOF: Security audit log recorded policy rejection
        List<SecurityAuditLog> auditLogs = securityAuditLogRepository.findAll();
        assertThat(auditLogs).hasSize(1);
        assertThat(auditLogs.get(0).getStatus()).isEqualTo("REJECTED_POLICY");
        assertThat(auditLogs.get(0).getAction()).isEqualTo("REJECTED_LEDGER_POSTED");
    }
}
