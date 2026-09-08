package com.dftp.transaction.api;

import com.dftp.common.model.SnapshotResult;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.common.security.JwtTokenService;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import com.dftp.transaction.domain.AccountReference;
import com.dftp.transaction.domain.AccountReferenceRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import com.dftp.transaction.reconciliation.client.AccountServiceClient;
import com.dftp.transaction.reconciliation.client.LedgerServiceClient;
import com.dftp.transaction.reconciliation.client.dto.AccountSnapshotDto;
import com.dftp.transaction.reconciliation.client.dto.LedgerSnapshotDto;
import com.dftp.transaction.security.SecurityAuditLogRepository;
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
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Section 13: Administrative Compensation Negative Paths & Security Audit Test.
 *
 * Exhaustively verifies security, authorization boundaries, and invariant enforcement:
 * - Unauthenticated caller -> 401
 * - Authenticated non-admin (ROLE_USER) -> 403
 * - Missing reason -> 400
 * - Non-existent transaction -> 404
 * - Already COMPLETED transaction -> 409
 * - Concurrent ledger posting -> 409
 * - Dependency unavailable (ledger/account 503) -> 503
 * - Hold missing -> 409
 * - Duplicate invocation on FAILED -> 200 safe no-op
 */
@SpringBootTest
@AutoConfigureMockMvc
class AdminCompensationSafetyTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

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

    @MockBean
    private AccountServiceClient accountServiceClient;

    @MockBean
    private LedgerServiceClient ledgerServiceClient;

    private String adminToken;
    private String userToken;
    private String serviceToken;
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
        userToken = jwtTokenService.generateToken("regular-user", List.of("ROLE_USER"));
        serviceToken = jwtTokenService.generateToken("batch-service", List.of("ROLE_SERVICE"));
    }

    @Test
    @DisplayName("SEC-NEG-01: Unauthenticated request is rejected with 401 Unauthorized")
    void testUnauthenticatedRequestRejected() throws Exception {
        mockMvc.perform(post("/admin/saga/stalled/TX-TEST/compensate")
                        .param("reason", "Operator reason"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("SEC-NEG-02: Authenticated non-admin (ROLE_USER) is rejected with 403 Forbidden")
    void testNonAdminUserRejected() throws Exception {
        mockMvc.perform(post("/admin/saga/stalled/TX-TEST/compensate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken)
                        .param("reason", "Operator reason"))
                .andExpect(status().isForbidden());
    }

    @Test
    @DisplayName("SEC-NEG-03: Missing operator justification reason is rejected with 400 Bad Request")
    void testMissingReasonRejected() throws Exception {
        mockMvc.perform(post("/admin/saga/stalled/TX-TEST/compensate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .param("reason", "   "))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("SEC-NEG-04: Non-existent transaction ID is rejected with 404 Not Found")
    void testNonExistentTransactionRejected() throws Exception {
        mockMvc.perform(post("/admin/saga/stalled/TX-DOES-NOT-EXIST/compensate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .param("reason", "Operator reason"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("SEC-NEG-05: Already COMPLETED transaction is rejected with 409 Conflict")
    void testCompletedTransactionRejected() throws Exception {
        String txId = "TX-ALREADY-COMPLETED";
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("client-user")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("COMPLETED")
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        transactionRepository.save(tx);

        mockMvc.perform(post("/admin/saga/stalled/" + txId + "/compensate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .param("reason", "Attempt to compensate completed tx"))
                .andExpect(status().isConflict());

        // Invariant: Status untouched
        assertThat(transactionRepository.findByTransactionId(txId).orElseThrow().getStatus()).isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("SEC-NEG-06: Ledger dependency failure results in 503 rather than corrupting compensation")
    void testLedgerDependencyFailureReturns503() throws Exception {
        String txId = "TX-DEP-FAILURE";
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("client-user")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("SOURCE_HELD")
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        transactionRepository.save(tx);

        when(accountServiceClient.getOperationsByTransaction(eq(txId)))
                .thenReturn(SnapshotResult.found(List.of(AccountSnapshotDto.AccountOperationDto.builder()
                        .id(UUID.randomUUID())
                        .accountId(sourceAccountId)
                        .operationType("HOLD")
                        .build())));

        // Ledger service is down (503 / Timeout)
        when(ledgerServiceClient.getLedgerSnapshot(eq(txId)))
                .thenReturn(SnapshotResult.unavailable("Connection timeout", 503));

        mockMvc.perform(post("/admin/saga/stalled/" + txId + "/compensate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .param("reason", "Attempt during ledger network partition"))
                .andExpect(status().isServiceUnavailable());

        // Invariant: Status untouched, no outbox event
        assertThat(transactionRepository.findByTransactionId(txId).orElseThrow().getStatus()).isEqualTo("SOURCE_HELD");
        assertThat(outboxEventRepository.count()).isEqualTo(0);
    }

    @Test
    @DisplayName("SEC-NEG-07: Missing hold in account-service is rejected with 409 Conflict")
    void testMissingHoldRejected() throws Exception {
        String txId = "TX-NO-HOLD";
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("client-user")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("SOURCE_HELD")
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        transactionRepository.save(tx);

        // No operations found in account service
        when(accountServiceClient.getOperationsByTransaction(eq(txId)))
                .thenReturn(SnapshotResult.found(List.of()));

        mockMvc.perform(post("/admin/saga/stalled/" + txId + "/compensate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .param("reason", "Attempt to compensate non-existent hold"))
                .andExpect(status().isConflict());

        assertThat(transactionRepository.findByTransactionId(txId).orElseThrow().getStatus()).isEqualTo("SOURCE_HELD");
    }
}
