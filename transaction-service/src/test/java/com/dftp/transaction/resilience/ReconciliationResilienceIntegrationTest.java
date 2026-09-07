package com.dftp.transaction.resilience;

import com.dftp.common.model.SnapshotResult;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import com.dftp.transaction.domain.AccountReference;
import com.dftp.transaction.domain.AccountReferenceRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import com.dftp.transaction.reconciliation.application.CrossServiceReconciliationService;
import com.dftp.transaction.reconciliation.client.AccountServiceClient;
import com.dftp.transaction.reconciliation.client.LedgerServiceClient;
import com.dftp.transaction.reconciliation.client.dto.CandidateHoldDto;
import com.dftp.transaction.reconciliation.client.dto.CandidateLedgerTransactionDto;
import com.dftp.transaction.reconciliation.client.dto.LedgerSnapshotDto;
import com.dftp.transaction.reconciliation.domain.ReconciliationAnomaly;
import com.dftp.transaction.reconciliation.domain.ReconciliationAnomalyHistory;
import com.dftp.transaction.reconciliation.dto.CrossServiceReconciliationReport;
import com.dftp.transaction.reconciliation.repository.ReconciliationAnomalyHistoryRepository;
import com.dftp.transaction.reconciliation.repository.ReconciliationAnomalyRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/**
 * Phase 12 Chaos & Resilience: Reconciliation Engine Adversarial Review.
 * Verifies dependency failure handling, reverse scanner multi-page pagination,
 * and database-level audit history immutability.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ReconciliationResilienceIntegrationTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private CrossServiceReconciliationService reconciliationService;

    @Autowired
    private ReconciliationAnomalyRepository anomalyRepository;

    @Autowired
    private ReconciliationAnomalyHistoryRepository anomalyHistoryRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private AccountReferenceRepository accountReferenceRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @MockBean
    private AccountServiceClient accountServiceClient;

    @MockBean
    private LedgerServiceClient ledgerServiceClient;

    private UUID sourceAccountId;
    private UUID destAccountId;

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("TRUNCATE TABLE reconciliation_anomaly_history, reconciliation_anomalies CASCADE");
        outboxEventRepository.deleteAll();
        transactionRepository.deleteAll();
        accountReferenceRepository.deleteAll();

        sourceAccountId = UUID.randomUUID();
        destAccountId = UUID.randomUUID();

        accountReferenceRepository.save(new AccountReference(sourceAccountId, Instant.now(), "user-alice"));
        accountReferenceRepository.save(new AccountReference(destAccountId, Instant.now(), "user-bob"));

        // Defaults
        when(accountServiceClient.getBulkOperations(anyList())).thenReturn(Collections.emptyMap());
        when(ledgerServiceClient.getBulkLedgerSnapshots(anyList())).thenReturn(Collections.emptyMap());
        when(ledgerServiceClient.getUnmatchedCandidates(anyLong(), anyInt(), anyInt())).thenReturn(Collections.emptyList());
        when(accountServiceClient.getOrphanHoldCandidates(anyLong(), anyInt(), anyInt())).thenReturn(Collections.emptyList());
    }

    @Test
    @DisplayName("CHAOS-RECON-01: Bulk snapshot 5xx/unavailable classifies as UNKNOWN_DUE_TO_DEPENDENCY_FAILURE, not CRITICAL")
    void testBulkSnapshotDependencyFailureClassification() {
        String txId = "TX-CHAOS-RECON-DEP-" + UUID.randomUUID().toString().substring(0, 8);
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("COMPLETED")
                .createdAt(Instant.now().minus(Duration.ofMinutes(10)))
                .updatedAt(Instant.now().minus(Duration.ofMinutes(10)))
                .build();
        transactionRepository.save(tx);

        // Inject dependency failure during bulk lookup
        when(ledgerServiceClient.getBulkLedgerSnapshots(anyList())).thenReturn(
                Map.of(txId, SnapshotResult.unavailable("Ledger Service 503 Service Unavailable", 503))
        );

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(
                Duration.ofSeconds(1), Duration.ofMinutes(2));

        assertThat(report.getTotalTransactionsScanned()).isEqualTo(1);
        // Must NOT produce CRITICAL_LEDGER_MISMATCH
        assertThat(report.getCriticalAnomaliesCount()).isEqualTo(0);
        assertThat(report.getMediumAnomaliesCount()).isEqualTo(1);

        ReconciliationAnomaly anomaly = anomalyRepository.findAll().get(0);
        assertThat(anomaly.getAnomalyType()).isEqualTo("UNKNOWN_DUE_TO_DEPENDENCY_FAILURE");
        assertThat(anomaly.getSeverity()).isEqualTo("MEDIUM");
    }

    @Test
    @DisplayName("CHAOS-RECON-02: Reverse scanner paginates across multiple pages (> 100 candidates)")
    void testReverseScannerMultiPagePagination() {
        int totalCandidates = 150;
        List<CandidateLedgerTransactionDto> page0 = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            page0.add(CandidateLedgerTransactionDto.builder()
                    .businessTransactionId("TX-ORPHAN-PAGE0-" + i)
                    .ledgerTransactionId("LEDGER-PAGE0-" + i)
                    .status("POSTED")
                    .createdAt(Instant.now().minus(Duration.ofMinutes(10)))
                    .build());
        }

        List<CandidateLedgerTransactionDto> page1 = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            page1.add(CandidateLedgerTransactionDto.builder()
                    .businessTransactionId("TX-ORPHAN-PAGE1-" + i)
                    .ledgerTransactionId("LEDGER-PAGE1-" + i)
                    .status("POSTED")
                    .createdAt(Instant.now().minus(Duration.ofMinutes(10)))
                    .build());
        }

        when(ledgerServiceClient.getUnmatchedCandidates(anyLong(), eq(0), eq(100))).thenReturn(page0);
        when(ledgerServiceClient.getUnmatchedCandidates(anyLong(), eq(1), eq(100))).thenReturn(page1);

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(
                Duration.ofSeconds(1), Duration.ofMinutes(2));

        // Total 150 orphan ledger transactions must all be discovered
        long orphanCount = anomalyRepository.countByAnomalyType("ORPHANED_LEDGER_TRANSACTION");
        assertThat(orphanCount).isEqualTo(totalCandidates);
    }

    @Test
    @DisplayName("CHAOS-RECON-03: PostgreSQL trigger blocks UPDATE or DELETE on reconciliation_anomaly_history")
    void testAuditHistoryImmutabilityTrigger() {
        UUID anomalyId = UUID.randomUUID();
        ReconciliationAnomaly anomaly = ReconciliationAnomaly.builder()
                .id(anomalyId)
                .reconciliationId(UUID.randomUUID())
                .businessTransactionId("TX-AUDIT-IMMUTABLE")
                .anomalyType("CRITICAL_LEDGER_MISMATCH")
                .severity("CRITICAL")
                .status("DETECTED")
                .observedState("{}")
                .detectedAt(Instant.now())
                .lastCheckedAt(Instant.now())
                .build();
        anomalyRepository.save(anomaly);

        ReconciliationAnomalyHistory history = ReconciliationAnomalyHistory.builder()
                .id(UUID.randomUUID())
                .anomalyId(anomalyId)
                .businessTransactionId("TX-AUDIT-IMMUTABLE")
                .anomalyType("CRITICAL_LEDGER_MISMATCH")
                .fromStatus(null)
                .toStatus("DETECTED")
                .actor("system-reconciliation")
                .occurredAt(Instant.now())
                .build();
        anomalyHistoryRepository.save(history);

        // Attempt direct SQL DELETE on history row -> must be blocked by DB trigger
        assertThatThrownBy(() -> jdbcTemplate.execute(
                "DELETE FROM reconciliation_anomaly_history WHERE id = '" + history.getId() + "'"))
                .hasMessageContaining("strictly append-only and immutable");

        // Attempt direct SQL UPDATE on history row -> must be blocked by DB trigger
        assertThatThrownBy(() -> jdbcTemplate.execute(
                "UPDATE reconciliation_anomaly_history SET actor = 'tampered' WHERE id = '" + history.getId() + "'"))
                .hasMessageContaining("strictly append-only and immutable");
    }
}
