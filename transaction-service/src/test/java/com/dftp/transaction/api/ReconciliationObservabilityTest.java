package com.dftp.transaction.api;

import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.common.security.JwtTokenService;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@org.springframework.test.context.TestPropertySource(properties = "dftp.outbox.relay.enabled=false")
class ReconciliationObservabilityTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JwtTokenService jwtTokenService;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void setUp() {
        transactionRepository.deleteAll();
        outboxEventRepository.deleteAll();
    }

    @Test
    @DisplayName("RECON-01: Anonymous access to reconciliation report is rejected with 401")
    void testAnonymousAccessRejected() throws Exception {
        mockMvc.perform(get("/admin/reconciliation/report"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("RECON-02: ROLE_USER access to reconciliation report is rejected with 403 Forbidden")
    void testUserAccessForbidden() throws Exception {
        String userToken = jwtTokenService.generateToken("regular-user", List.of("ROLE_USER"));
        mockMvc.perform(get("/admin/reconciliation/report")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken))
                .andExpect(status().isForbidden());
    }

    @Test
    @DisplayName("RECON-03: ROLE_ADMIN accesses report, accurately detects held-without-ledger and stalled outbox, without mutating state")
    void testAdminReconciliationScanning() throws Exception {
        // Seed an anomalous transaction stuck in SOURCE_HELD 10 minutes ago
        Instant tenMinutesAgo = Instant.now().minusSeconds(600);
        Transaction stuckTx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId("tx-stuck-001")
                .sourceAccountId(UUID.randomUUID())
                .destinationAccountId(UUID.randomUUID())
                .amount(new BigDecimal("250.00"))
                .currency("USD")
                .status("SOURCE_HELD")
                .ownerId("user-1")
                .createdAt(tenMinutesAgo)
                .updatedAt(tenMinutesAgo)
                .build();
        transactionRepository.save(stuckTx);

        // Seed a stalled outbox event 5 minutes ago
        OutboxEvent stalledOutbox = OutboxEvent.builder()
                .id(UUID.randomUUID())
                .aggregateType("transaction-events")
                .aggregateId(stuckTx.getId().toString())
                .eventType("FundsHoldRequested")
                .payload("{}")
                .status("PENDING")
                .createdAt(Instant.now().minusSeconds(300))
                .build();
        outboxEventRepository.save(stalledOutbox);

        String adminToken = jwtTokenService.generateToken("admin-ops", List.of("ROLE_ADMIN"));

        mockMvc.perform(get("/admin/reconciliation/report")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .param("sagaThresholdSeconds", "180")
                        .param("outboxThresholdSeconds", "60"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalAnomaliesDetected").value(2))
                .andExpect(jsonPath("$.fundsHeldLedgerAbsentCount").value(1))
                .andExpect(jsonPath("$.stalledOutboxCount").value(1))
                .andExpect(jsonPath("$.anomalySummaries[0]").value(org.hamcrest.Matchers.containsString("tx-stuck-001")));

        // Critical invariant: state is NOT mutated by observability
        Transaction persistedTx = transactionRepository.findById(stuckTx.getId()).orElseThrow();
        assertThat(persistedTx.getStatus()).isEqualTo("SOURCE_HELD");
        assertThat(persistedTx.getAmount()).isEqualByComparingTo("250.00");
    }
}
