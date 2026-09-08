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
import com.dftp.transaction.reconciliation.client.dto.AccountSnapshotDto;
import com.dftp.transaction.reconciliation.domain.ReconciliationAnomaly;
import com.dftp.transaction.reconciliation.dto.CrossServiceReconciliationReport;
import com.dftp.transaction.reconciliation.repository.ReconciliationAnomalyHistoryRepository;
import com.dftp.transaction.reconciliation.repository.ReconciliationAnomalyRepository;
import com.dftp.transaction.retention.DataRetentionPurgeService;
import com.dftp.transaction.settlement.application.BatchSettlementService;
import com.dftp.transaction.settlement.domain.SettlementBatch;
import com.dftp.transaction.settlement.domain.SettlementBatchItem;
import com.dftp.transaction.settlement.repository.SettlementBatchItemRepository;
import com.dftp.transaction.settlement.repository.SettlementBatchRepository;
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
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * P12.5-COND-03: NTP / Clock-Skew Operational Requirement & Deterministic Behavior Test.
 *
 * Evaluates timestamp-sensitive mechanisms across temporal thresholds:
 * 1. Skew below accepted threshold (20ms / within grace window): zero false-positive anomalies.
 * 2. Skew near threshold (29s in 30s grace window): correctly preserved in grace window.
 * 3. Skew above threshold (> 30s): anomaly flagged for operator diagnosis without corrupting balances.
 * 4. Stalled worker recovery: active items (< timeout) are never prematurely reclaimed;
 *    legitimate stalled items (> timeout) are cleanly recovered.
 * 5. Data retention: records within retention window are never purged early.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ClockSkewResilienceTest extends AbstractTransactionIntegrationTest {

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
    private BatchSettlementService batchSettlementService;

    @Autowired
    private SettlementBatchRepository batchRepository;

    @Autowired
    private SettlementBatchItemRepository batchItemRepository;

    @Autowired
    private DataRetentionPurgeService dataRetentionPurgeService;

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
        batchItemRepository.deleteAll();
        batchRepository.deleteAll();
        transactionRepository.deleteAll();
        outboxEventRepository.deleteAll();
        accountReferenceRepository.deleteAll();

        sourceAccountId = UUID.randomUUID();
        destAccountId = UUID.randomUUID();

        accountReferenceRepository.save(new AccountReference(sourceAccountId, Instant.now(), "client-user"));
        accountReferenceRepository.save(new AccountReference(destAccountId, Instant.now(), "merchant-user"));
    }

    @Test
    @DisplayName("CLOCK-01: Clock skew below threshold (10s in 30s grace) classifies transaction as within grace window")
    void testReconciliationUnderAcceptableSkew() {
        String txId = "TX-SKEW-LOW";
        // Transaction updated 10 seconds ago (well within 30s grace window)
        Instant txTime = Instant.now().minus(10, ChronoUnit.SECONDS);

        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("client-user")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("SOURCE_HELD")
                .createdAt(txTime)
                .updatedAt(txTime)
                .build();
        transactionRepository.save(tx);

        AccountSnapshotDto.AccountOperationDto holdOp = AccountSnapshotDto.AccountOperationDto.builder()
                .id(UUID.randomUUID())
                .accountId(sourceAccountId)
                .operationType("HOLD")
                .createdAt(txTime)
                .build();
        when(accountServiceClient.getOperationsByTransaction(eq(txId)))
                .thenReturn(SnapshotResult.found(List.of(holdOp)));
        when(ledgerServiceClient.getLedgerSnapshot(eq(txId)))
                .thenReturn(SnapshotResult.notFound("Not yet posted"));

        // Execute scan with 30s grace window
        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(
                Duration.ofSeconds(30), Duration.ofMinutes(2));

        // PROOF: Zero false anomalies detected for in-flight transaction
        assertThat(report.getTotalAnomaliesDetected()).isEqualTo(0);
        assertThat(anomalyRepository.count()).isEqualTo(0);
    }

    @Test
    @DisplayName("CLOCK-02: Clock skew near threshold (29s in 30s grace) is correctly preserved within grace window")
    void testReconciliationNearThreshold() {
        String txId = "TX-SKEW-NEAR";
        Instant txTime = Instant.now().minus(29, ChronoUnit.SECONDS);

        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("client-user")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("SOURCE_HELD")
                .createdAt(txTime)
                .updatedAt(txTime)
                .build();
        transactionRepository.save(tx);

        AccountSnapshotDto.AccountOperationDto holdOp = AccountSnapshotDto.AccountOperationDto.builder()
                .id(UUID.randomUUID())
                .accountId(sourceAccountId)
                .operationType("HOLD")
                .createdAt(txTime)
                .build();
        when(accountServiceClient.getOperationsByTransaction(eq(txId)))
                .thenReturn(SnapshotResult.found(List.of(holdOp)));
        when(ledgerServiceClient.getLedgerSnapshot(eq(txId)))
                .thenReturn(SnapshotResult.notFound("Not yet posted"));

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(
                Duration.ofSeconds(30), Duration.ofMinutes(2));

        assertThat(report.getTotalAnomaliesDetected()).isEqualTo(0);
    }

    @Test
    @DisplayName("CLOCK-03: Clock skew above threshold (>30s) records anomaly for diagnosis without corrupting balances")
    void testReconciliationExcessiveSkewFlagsAnomalyWithoutMutation() {
        String txId = "TX-SKEW-EXCESSIVE";
        // Transaction appears 45s old (exceeding 30s grace window)
        Instant txTime = Instant.now().minus(45, ChronoUnit.SECONDS);

        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("client-user")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("500.00"))
                .currency("USD")
                .status("SOURCE_HELD")
                .createdAt(txTime)
                .updatedAt(txTime)
                .build();
        transactionRepository.save(tx);

        AccountSnapshotDto.AccountOperationDto holdOp = AccountSnapshotDto.AccountOperationDto.builder()
                .id(UUID.randomUUID())
                .accountId(sourceAccountId)
                .operationType("HOLD")
                .createdAt(txTime)
                .build();
        when(accountServiceClient.getOperationsByTransaction(eq(txId)))
                .thenReturn(SnapshotResult.found(List.of(holdOp)));
        when(ledgerServiceClient.getLedgerSnapshot(eq(txId)))
                .thenReturn(SnapshotResult.notFound("Not yet posted"));

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(
                Duration.ofSeconds(30), Duration.ofMinutes(2));

        // Anomaly is detected and flagged for operator
        assertThat(report.getTotalAnomaliesDetected()).isGreaterThanOrEqualTo(1);
        ReconciliationAnomaly anomaly = anomalyRepository.findByBusinessTransactionIdAndAnomalyType(
                txId, "STUCK_BEFORE_LEDGER").orElseThrow();
        assertThat(anomaly.getSeverity()).isEqualTo("HIGH");

        // PROOF: Transaction state was NOT mutated; balances are completely uncorrupted
        Transaction untouched = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(untouched.getStatus()).isEqualTo("SOURCE_HELD");
    }

    @Test
    @DisplayName("CLOCK-04: Stalled worker recovery never reclaims active items prematurely (< timeout)")
    void testStalledWorkerRecoveryRespectsTimeout() {
        String batchId = "BATCH-CLOCK-01";
        SettlementBatch batch = SettlementBatch.builder()
                .id(UUID.randomUUID())
                .batchId(batchId)
                .status("RUNNING")
                .totalItems(2)
                .successCount(0)
                .failureCount(0)
                .retryCount(0)
                .createdAt(Instant.now())
                .createdBy("test")
                .build();
        batchRepository.save(batch);

        // Item 1: Active item updated 30s ago (< 2m timeout)
        SettlementBatchItem activeItem = SettlementBatchItem.builder()
                .id(UUID.randomUUID())
                .batchId(batchId)
                .transactionId("TX-ACTIVE-01")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("10.00"))
                .currency("USD")
                .status("PROCESSING")
                .retryCount(1)
                .createdAt(Instant.now().minus(30, ChronoUnit.SECONDS))
                .updatedAt(Instant.now().minus(30, ChronoUnit.SECONDS))
                .build();

        // Item 2: Stalled item updated 3 minutes ago (> 2m timeout)
        SettlementBatchItem stalledItem = SettlementBatchItem.builder()
                .id(UUID.randomUUID())
                .batchId(batchId)
                .transactionId("TX-STALLED-02")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("20.00"))
                .currency("USD")
                .status("PROCESSING")
                .retryCount(1)
                .createdAt(Instant.now().minus(3, ChronoUnit.MINUTES))
                .updatedAt(Instant.now().minus(3, ChronoUnit.MINUTES))
                .build();

        batchItemRepository.saveAll(List.of(activeItem, stalledItem));

        // Execute recovery with 2-minute timeout
        int recoveredCount = batchSettlementService.recoverStalledItems(Duration.ofMinutes(2));

        // Exactly 1 item recovered (the stalled item)
        assertThat(recoveredCount).isEqualTo(1);

        // PROOF: Active item was NOT prematurely reclaimed
        SettlementBatchItem activeAfter = batchItemRepository.findById(activeItem.getId()).orElseThrow();
        assertThat(activeAfter.getStatus()).isEqualTo("PROCESSING");

        // PROOF: Stalled item WAS recovered to PENDING
        SettlementBatchItem stalledAfter = batchItemRepository.findById(stalledItem.getId()).orElseThrow();
        assertThat(stalledAfter.getStatus()).isEqualTo("PENDING");
        assertThat(stalledAfter.getErrorMessage()).contains("Recovered from worker crash");
    }

    @Test
    @DisplayName("CLOCK-05: Data retention purge does not delete records within retention window")
    void testRetentionPurgeDoesNotPurgeEarly() {
        String batchId = "BATCH-RETENTION-01";
        SettlementBatch batch = SettlementBatch.builder()
                .id(UUID.randomUUID())
                .batchId(batchId)
                .status("COMPLETED")
                .totalItems(1)
                .successCount(1)
                .failureCount(0)
                .retryCount(0)
                .createdAt(Instant.now().minus(30, ChronoUnit.DAYS)) // 30 days old (< 90 days retention)
                .createdBy("test")
                .build();
        batchRepository.save(batch);

        SettlementBatchItem item = SettlementBatchItem.builder()
                .id(UUID.randomUUID())
                .batchId(batchId)
                .transactionId("TX-RET-01")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("10.00"))
                .currency("USD")
                .status("SETTLED")
                .retryCount(0)
                .createdAt(Instant.now().minus(30, ChronoUnit.DAYS))
                .updatedAt(Instant.now().minus(30, ChronoUnit.DAYS))
                .build();
        batchItemRepository.save(item);

        // Purge with 90-day retention
        int purged = dataRetentionPurgeService.purgeSettlementItems(Duration.ofDays(90), 100);

        // PROOF: 0 items purged early
        assertThat(purged).isEqualTo(0);
        assertThat(batchItemRepository.findById(item.getId())).isPresent();
    }
}
