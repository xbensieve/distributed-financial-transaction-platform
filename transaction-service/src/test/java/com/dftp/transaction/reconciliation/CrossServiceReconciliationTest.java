package com.dftp.transaction.reconciliation;

import com.dftp.common.model.SnapshotResult;
import com.dftp.common.outbox.OutboxEvent;
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
import com.dftp.transaction.reconciliation.client.dto.CandidateHoldDto;
import com.dftp.transaction.reconciliation.client.dto.CandidateLedgerTransactionDto;
import com.dftp.transaction.reconciliation.client.dto.LedgerSnapshotDto;
import com.dftp.transaction.reconciliation.domain.ReconciliationAnomaly;
import com.dftp.transaction.reconciliation.domain.ReconciliationAnomalyHistory;
import com.dftp.transaction.reconciliation.dto.CrossServiceReconciliationReport;
import com.dftp.transaction.reconciliation.repository.ReconciliationAnomalyHistoryRepository;
import com.dftp.transaction.reconciliation.repository.ReconciliationAnomalyRepository;
import com.dftp.transaction.retention.DataRetentionPurgeService;
import com.dftp.transaction.security.SecurityAuditLog;
import com.dftp.transaction.security.SecurityAuditLogRepository;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CrossServiceReconciliationTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private CrossServiceReconciliationService reconciliationService;

    @Autowired
    private ReconciliationAnomalyRepository anomalyRepository;

    @Autowired
    private ReconciliationAnomalyHistoryRepository anomalyHistoryRepository;

    @Autowired
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private AccountReferenceRepository accountReferenceRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private SettlementBatchRepository batchRepository;

    @Autowired
    private SettlementBatchItemRepository batchItemRepository;

    @Autowired
    private SecurityAuditLogRepository securityAuditLogRepository;

    @Autowired
    private DataRetentionPurgeService dataRetentionPurgeService;

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
        batchItemRepository.deleteAll();
        batchRepository.deleteAll();
        securityAuditLogRepository.deleteAll();

        sourceAccountId = UUID.randomUUID();
        destAccountId = UUID.randomUUID();

        accountReferenceRepository.save(new AccountReference(sourceAccountId, Instant.now(), "user-alice"));
        accountReferenceRepository.save(new AccountReference(destAccountId, Instant.now(), "user-bob"));

        // Default bulk mock routing
        when(ledgerServiceClient.getBulkLedgerSnapshots(any())).thenAnswer(invocation -> {
            List<String> ids = invocation.getArgument(0);
            Map<String, SnapshotResult<LedgerSnapshotDto>> map = new HashMap<>();
            for (String id : ids) {
                SnapshotResult<LedgerSnapshotDto> res = ledgerServiceClient.getLedgerSnapshot(id);
                map.put(id, res != null ? res : SnapshotResult.notFound("not found"));
            }
            return map;
        });

        when(accountServiceClient.getBulkOperations(any())).thenAnswer(invocation -> {
            List<String> ids = invocation.getArgument(0);
            Map<String, SnapshotResult<List<AccountSnapshotDto.AccountOperationDto>>> map = new HashMap<>();
            for (String id : ids) {
                SnapshotResult<List<AccountSnapshotDto.AccountOperationDto>> res =
                        accountServiceClient.getOperationsByTransaction(id);
                map.put(id, res != null ? res : SnapshotResult.found(Collections.emptyList()));
            }
            return map;
        });

        when(ledgerServiceClient.getUnmatchedCandidates(anyLong(), anyInt(), anyInt())).thenReturn(Collections.emptyList());
        when(accountServiceClient.getOrphanHoldCandidates(anyLong(), anyInt(), anyInt())).thenReturn(Collections.emptyList());
    }

    @Test
    @DisplayName("CASE-A: Detects STUCK_BEFORE_LEDGER when funds held in account-service but no ledger post exists")
    void testCaseA_StuckBeforeLedger() {
        String txId = "TX-CASE-A-01";
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("SOURCE_HELD")
                .createdAt(Instant.now().minusSeconds(120))
                .updatedAt(Instant.now().minusSeconds(120))
                .build();
        transactionRepository.save(tx);

        when(ledgerServiceClient.getLedgerSnapshot(txId)).thenReturn(SnapshotResult.notFound("Not found (404)"));
        when(accountServiceClient.getOperationsByTransaction(txId)).thenReturn(SnapshotResult.found(List.of(
                AccountSnapshotDto.AccountOperationDto.builder()
                        .accountId(sourceAccountId)
                        .transactionId(txId)
                        .operationType("HOLD")
                        .createdAt(Instant.now().minusSeconds(120))
                        .build()
        )));

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(Duration.ofSeconds(30), Duration.ofMinutes(2));

        assertThat(report.getTotalAnomaliesDetected()).isGreaterThanOrEqualTo(1);
        ReconciliationAnomaly anomaly = anomalyRepository.findByBusinessTransactionIdAndAnomalyType(txId, "STUCK_BEFORE_LEDGER").orElseThrow();
        assertThat(anomaly.getSeverity()).isEqualTo("HIGH");
        assertThat(anomaly.getStatus()).isEqualTo("DETECTED");
    }

    @Test
    @DisplayName("CASE-B: Detects LEDGER_POSTED_SETTLEMENT_PENDING when ledger transaction is posted but settlement is missing")
    void testCaseB_LedgerPostedSettlementPending() {
        String txId = "TX-CASE-B-01";
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("200.00"))
                .currency("USD")
                .status("LEDGER_POSTED")
                .createdAt(Instant.now().minusSeconds(120))
                .updatedAt(Instant.now().minusSeconds(120))
                .build();
        transactionRepository.save(tx);

        when(ledgerServiceClient.getLedgerSnapshot(txId)).thenReturn(SnapshotResult.found(
                LedgerSnapshotDto.builder()
                        .ledgerTransactionId("LT-001")
                        .businessTransactionId(txId)
                        .status("POSTED")
                        .postings(List.of(
                                LedgerSnapshotDto.PostingDto.builder().accountId(sourceAccountId).amount(new BigDecimal("200.00")).currency("USD").postingType("DEBIT").build(),
                                LedgerSnapshotDto.PostingDto.builder().accountId(destAccountId).amount(new BigDecimal("200.00")).currency("USD").postingType("CREDIT").build()
                        ))
                        .build()
        ));
        when(accountServiceClient.getOperationsByTransaction(txId)).thenReturn(SnapshotResult.found(List.of(
                AccountSnapshotDto.AccountOperationDto.builder().accountId(sourceAccountId).transactionId(txId).operationType("HOLD").build()
        )));

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(Duration.ofSeconds(30), Duration.ofMinutes(2));

        ReconciliationAnomaly anomaly = anomalyRepository.findByBusinessTransactionIdAndAnomalyType(txId, "LEDGER_POSTED_SETTLEMENT_PENDING").orElseThrow();
        assertThat(anomaly.getSeverity()).isEqualTo("MEDIUM");
    }

    @Test
    @DisplayName("CASE-C: Detects CRITICAL_LEDGER_MISMATCH when transaction is marked COMPLETED but ledger entry is confirmed NOT_FOUND (404)")
    void testCaseC_CriticalLedgerMismatch() {
        String txId = "TX-CASE-C-01";
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("300.00"))
                .currency("USD")
                .status("COMPLETED")
                .createdAt(Instant.now().minusSeconds(120))
                .updatedAt(Instant.now().minusSeconds(120))
                .build();
        transactionRepository.save(tx);

        when(ledgerServiceClient.getLedgerSnapshot(txId)).thenReturn(SnapshotResult.notFound("Not found (404)"));

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(Duration.ofSeconds(30), Duration.ofMinutes(2));

        ReconciliationAnomaly anomaly = anomalyRepository.findByBusinessTransactionIdAndAnomalyType(txId, "CRITICAL_LEDGER_MISMATCH").orElseThrow();
        assertThat(anomaly.getSeverity()).isEqualTo("CRITICAL");
    }

    @Test
    @DisplayName("P11-REM-01: Service unavailable (503/timeout) maps to UNKNOWN_DUE_TO_DEPENDENCY_FAILURE and NEVER critical drift")
    void testCaseC_DependencyTimeoutNeverCritical() {
        String txId = "TX-TIMEOUT-01";
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("300.00"))
                .currency("USD")
                .status("COMPLETED")
                .createdAt(Instant.now().minusSeconds(120))
                .updatedAt(Instant.now().minusSeconds(120))
                .build();
        transactionRepository.save(tx);

        // Inject 503 / timeout failure from ledger-service
        when(ledgerServiceClient.getLedgerSnapshot(txId)).thenReturn(SnapshotResult.unavailable("Connection timeout to ledger-service", 503));

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(Duration.ofSeconds(30), Duration.ofMinutes(2));

        // Must NOT flag CRITICAL_LEDGER_MISMATCH!
        assertThat(anomalyRepository.findByBusinessTransactionIdAndAnomalyType(txId, "CRITICAL_LEDGER_MISMATCH")).isEmpty();

        // Must flag UNKNOWN_DUE_TO_DEPENDENCY_FAILURE with MEDIUM severity
        ReconciliationAnomaly anomaly = anomalyRepository.findByBusinessTransactionIdAndAnomalyType(
                txId, "UNKNOWN_DUE_TO_DEPENDENCY_FAILURE").orElseThrow();
        assertThat(anomaly.getSeverity()).isEqualTo("MEDIUM");
        assertThat(anomaly.getObservedState()).contains("Connection timeout to ledger-service");
    }

    @Test
    @DisplayName("CASE-D: Detects ORPHANED_HOLD when transaction FAILED but account still has uncompensated hold")
    void testCaseD_OrphanedHold() {
        String txId = "TX-CASE-D-01";
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("400.00"))
                .currency("USD")
                .status("FAILED")
                .createdAt(Instant.now().minusSeconds(120))
                .updatedAt(Instant.now().minusSeconds(120))
                .build();
        transactionRepository.save(tx);

        when(accountServiceClient.getOperationsByTransaction(txId)).thenReturn(SnapshotResult.found(List.of(
                AccountSnapshotDto.AccountOperationDto.builder().accountId(sourceAccountId).transactionId(txId).operationType("HOLD").build()
        )));
        when(ledgerServiceClient.getLedgerSnapshot(txId)).thenReturn(SnapshotResult.notFound("Not found (404)"));

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(Duration.ofSeconds(30), Duration.ofMinutes(2));

        ReconciliationAnomaly anomaly = anomalyRepository.findByBusinessTransactionIdAndAnomalyType(txId, "ORPHANED_HOLD").orElseThrow();
        assertThat(anomaly.getSeverity()).isEqualTo("HIGH");
    }

    @Test
    @DisplayName("P11-REM-02: Reverse Reconciliation autonomously discovers ORPHANED_LEDGER_TRANSACTION without parent tx row")
    void testReverseReconciliation_AutonomousOrphanLedger() {
        String orphanedTxId = "TX-REVERSE-ORPHAN-01";
        // Do NOT save any Transaction in transactionRepository!

        // Inject candidate ledger transaction from ledger-service
        CandidateLedgerTransactionDto cand = CandidateLedgerTransactionDto.builder()
                .ledgerTransactionId("LT-REVERSE-01")
                .businessTransactionId(orphanedTxId)
                .status("POSTED")
                .createdAt(Instant.now().minusSeconds(120))
                .build();

        when(ledgerServiceClient.getUnmatchedCandidates(anyLong(), anyInt(), anyInt())).thenReturn(List.of(cand));

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(Duration.ofSeconds(30), Duration.ofMinutes(2));

        // Autonomous discovery via reverse scan!
        ReconciliationAnomaly anomaly = anomalyRepository.findByBusinessTransactionIdAndAnomalyType(
                orphanedTxId, "ORPHANED_LEDGER_TRANSACTION").orElseThrow();
        assertThat(anomaly.getSeverity()).isEqualTo("CRITICAL");
        assertThat(anomaly.getLedgerTransactionId()).isEqualTo("LT-REVERSE-01");
    }

    @Test
    @DisplayName("P11-REM-02: Reverse Reconciliation autonomously discovers ORPHANED_HOLD without parent tx row")
    void testReverseReconciliation_AutonomousOrphanHold() {
        String holdTxId = "TX-REVERSE-HOLD-01";
        // Do NOT save any Transaction in transactionRepository!

        CandidateHoldDto holdCand = CandidateHoldDto.builder()
                .transactionId(holdTxId)
                .accountId(sourceAccountId)
                .createdAt(Instant.now().minusSeconds(120))
                .build();

        when(accountServiceClient.getOrphanHoldCandidates(anyLong(), anyInt(), anyInt())).thenReturn(List.of(holdCand));

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(Duration.ofSeconds(30), Duration.ofMinutes(2));

        ReconciliationAnomaly anomaly = anomalyRepository.findByBusinessTransactionIdAndAnomalyType(
                holdTxId, "ORPHANED_HOLD").orElseThrow();
        assertThat(anomaly.getSeverity()).isEqualTo("HIGH");
        assertThat(anomaly.getAccountId()).isEqualTo(sourceAccountId);
    }

    @Test
    @DisplayName("P11-REM-04: Anomaly Audit History records DETECTED -> RESOLVED -> DETECTED (reopen) without losing history")
    void testAnomalyAuditHistory_PreservesTransitionsAcrossReopen() {
        String txId = "TX-HISTORY-01";
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("COMPLETED")
                .createdAt(Instant.now().minusSeconds(120))
                .updatedAt(Instant.now().minusSeconds(120))
                .build();
        transactionRepository.save(tx);

        when(ledgerServiceClient.getLedgerSnapshot(txId)).thenReturn(SnapshotResult.notFound("Not found (404)"));

        // 1. Initial Scan: Anomaly Detected
        reconciliationService.runReconciliation(Duration.ofSeconds(30), Duration.ofMinutes(2));
        ReconciliationAnomaly anomaly = anomalyRepository.findByBusinessTransactionIdAndAnomalyType(txId, "CRITICAL_LEDGER_MISMATCH").orElseThrow();
        assertThat(anomaly.getStatus()).isEqualTo("DETECTED");

        // 2. Operator Resolves Anomaly
        reconciliationService.resolveAnomaly(anomaly.getId(), "Verified ledger manually via cold storage", "auditor-charlie");
        anomaly = anomalyRepository.findById(anomaly.getId()).orElseThrow();
        assertThat(anomaly.getStatus()).isEqualTo("RESOLVED");

        // 3. Second Scan (Discrepancy persists): Anomaly Re-opened to DETECTED
        reconciliationService.runReconciliation(Duration.ofSeconds(30), Duration.ofMinutes(2));
        anomaly = anomalyRepository.findById(anomaly.getId()).orElseThrow();
        assertThat(anomaly.getStatus()).isEqualTo("DETECTED");

        // 4. Verify Immutable Audit History Trail
        List<ReconciliationAnomalyHistory> history = anomalyHistoryRepository.findByAnomalyIdOrderByOccurredAtAsc(anomaly.getId());
        assertThat(history).hasSize(3);

        // Transition 1: null -> DETECTED
        assertThat(history.get(0).getFromStatus()).isNull();
        assertThat(history.get(0).getToStatus()).isEqualTo("DETECTED");

        // Transition 2: DETECTED -> RESOLVED
        assertThat(history.get(1).getFromStatus()).isEqualTo("DETECTED");
        assertThat(history.get(1).getToStatus()).isEqualTo("RESOLVED");
        assertThat(history.get(1).getActor()).isEqualTo("auditor-charlie");
        assertThat(history.get(1).getResolution()).isEqualTo("Verified ledger manually via cold storage");

        // Transition 3: RESOLVED -> DETECTED (Re-opened)
        assertThat(history.get(2).getFromStatus()).isEqualTo("RESOLVED");
        assertThat(history.get(2).getToStatus()).isEqualTo("DETECTED");
        assertThat(history.get(2).getNotes()).contains("re-opened");
    }

    @Test
    @DisplayName("P11-REM-05: Scheduled Data Purge safely purges old batch items and audit logs while retaining new ones")
    void testDataRetentionPurge_OldRowsPurgedNewRetained() {
        String batchIdOld = "BATCH-OLD-95D";
        String batchIdNew = "BATCH-NEW-1D";

        SettlementBatch batchOld = SettlementBatch.builder()
                .id(UUID.randomUUID())
                .batchId(batchIdOld)
                .status("COMPLETED")
                .createdAt(Instant.now().minus(Duration.ofDays(95)))
                .build();
        batchRepository.save(batchOld);

        SettlementBatch batchNew = SettlementBatch.builder()
                .id(UUID.randomUUID())
                .batchId(batchIdNew)
                .status("COMPLETED")
                .createdAt(Instant.now().minus(Duration.ofDays(1)))
                .build();
        batchRepository.save(batchNew);

        SettlementBatchItem oldItem = SettlementBatchItem.builder()
                .id(UUID.randomUUID())
                .batchId(batchIdOld)
                .transactionId("tx-old-item")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("10.00"))
                .currency("USD")
                .status("SETTLED")
                .createdAt(Instant.now().minus(Duration.ofDays(95)))
                .build();
        batchItemRepository.save(oldItem);

        SettlementBatchItem newItem = SettlementBatchItem.builder()
                .id(UUID.randomUUID())
                .batchId(batchIdNew)
                .transactionId("tx-new-item")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("20.00"))
                .currency("USD")
                .status("SETTLED")
                .createdAt(Instant.now().minus(Duration.ofDays(1)))
                .build();
        batchItemRepository.save(newItem);

        SecurityAuditLog oldLog = SecurityAuditLog.builder()
                .id(UUID.randomUUID())
                .principal("old-actor")
                .action("OLD_ACTION")
                .targetResource("RESOURCE")
                .status("SUCCESS")
                .createdAt(Instant.now().minus(Duration.ofDays(370)))
                .build();
        securityAuditLogRepository.save(oldLog);

        SecurityAuditLog newLog = SecurityAuditLog.builder()
                .id(UUID.randomUUID())
                .principal("new-actor")
                .action("NEW_ACTION")
                .targetResource("RESOURCE")
                .status("SUCCESS")
                .createdAt(Instant.now().minus(Duration.ofDays(10)))
                .build();
        securityAuditLogRepository.save(newLog);

        // Execute Purge
        int deletedItems = dataRetentionPurgeService.purgeSettlementItems(Duration.ofDays(90), 500);
        int deletedBatches = dataRetentionPurgeService.purgeSettlementBatches(Duration.ofDays(90), 500);
        int deletedLogs = dataRetentionPurgeService.purgeSecurityAuditLogs(Duration.ofDays(365), 500);

        assertThat(deletedItems).isEqualTo(1);
        assertThat(deletedBatches).isEqualTo(1);
        assertThat(deletedLogs).isEqualTo(1);

        // Verify Old rows are gone
        assertThat(batchItemRepository.findById(oldItem.getId())).isEmpty();
        assertThat(batchRepository.findByBatchId(batchIdOld)).isEmpty();
        assertThat(securityAuditLogRepository.findById(oldLog.getId())).isEmpty();

        // Verify New rows are retained
        assertThat(batchItemRepository.findById(newItem.getId())).isPresent();
        assertThat(batchRepository.findByBatchId(batchIdNew)).isPresent();
        assertThat(securityAuditLogRepository.findById(newLog.getId())).isPresent();
    }

    @Test
    @DisplayName("CASE-G: Detects EVENT_DISPATCH_LAG when outbox events remain in PENDING past the threshold")
    void testCaseG_EventDispatchLag() {
        OutboxEvent stalledEvent = OutboxEvent.builder()
                .id(UUID.randomUUID())
                .aggregateType("transaction-events")
                .aggregateId("tx-123")
                .eventType("FundsHoldRequested")
                .payload("{\"test\": true}")
                .status("PENDING")
                .createdAt(Instant.now().minusSeconds(300))
                .updatedAt(Instant.now().minusSeconds(300))
                .build();
        outboxEventRepository.save(stalledEvent);

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(Duration.ofSeconds(30), Duration.ofMinutes(2));

        ReconciliationAnomaly anomaly = anomalyRepository.findByBusinessTransactionIdAndAnomalyType("OUTBOX-GLOBAL", "EVENT_DISPATCH_LAG").orElseThrow();
        assertThat(anomaly.getSeverity()).isEqualTo("MEDIUM");
    }

    @Test
    @DisplayName("INV-AMOUNT & CURRENCY: Detects AMOUNT_MISMATCH and CURRENCY_MISMATCH in ledger postings")
    void testAmountAndCurrencyMismatch() {
        String txId = "TX-AMOUNT-01";
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("LEDGER_POSTED")
                .createdAt(Instant.now().minusSeconds(120))
                .updatedAt(Instant.now().minusSeconds(120))
                .build();
        transactionRepository.save(tx);

        when(ledgerServiceClient.getLedgerSnapshot(txId)).thenReturn(SnapshotResult.found(
                LedgerSnapshotDto.builder()
                        .ledgerTransactionId("LT-MISMATCH-01")
                        .businessTransactionId(txId)
                        .status("POSTED")
                        .postings(List.of(
                                LedgerSnapshotDto.PostingDto.builder().accountId(sourceAccountId).amount(new BigDecimal("50.00")).currency("EUR").postingType("DEBIT").build(),
                                LedgerSnapshotDto.PostingDto.builder().accountId(destAccountId).amount(new BigDecimal("50.00")).currency("EUR").postingType("CREDIT").build()
                        ))
                        .build()
        ));
        when(accountServiceClient.getOperationsByTransaction(txId)).thenReturn(SnapshotResult.found(List.of()));

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(Duration.ofSeconds(30), Duration.ofMinutes(2));

        assertThat(anomalyRepository.findByBusinessTransactionIdAndAnomalyType(txId, "AMOUNT_MISMATCH")).isPresent();
        assertThat(anomalyRepository.findByBusinessTransactionIdAndAnomalyType(txId, "CURRENCY_MISMATCH")).isPresent();
    }

    @Test
    @DisplayName("SNAPSHOT-CONSISTENCY: Recent updates within grace window are classified as EXPECTED_EVENTUAL_CONSISTENCY (no anomaly)")
    void testSnapshotGraceWindowHandling() {
        String txId = "TX-RECENT-01";
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("SOURCE_HELD")
                .createdAt(Instant.now().minusSeconds(10))
                .updatedAt(Instant.now().minusSeconds(10))
                .build();
        transactionRepository.save(tx);

        when(ledgerServiceClient.getLedgerSnapshot(txId)).thenReturn(SnapshotResult.notFound("Not found"));
        when(accountServiceClient.getOperationsByTransaction(txId)).thenReturn(SnapshotResult.found(List.of(
                AccountSnapshotDto.AccountOperationDto.builder().accountId(sourceAccountId).transactionId(txId).operationType("HOLD").build()
        )));

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(Duration.ofSeconds(30), Duration.ofMinutes(2));

        assertThat(anomalyRepository.findByBusinessTransactionIdAndAnomalyType(txId, "STUCK_BEFORE_LEDGER")).isEmpty();
    }

    @Test
    @DisplayName("REMEDIATION-POLICY: Safe actions permitted; financial mutations strictly rejected per non-negotiable rules")
    void testAutomatedRemediationPolicy() {
        // Safe Action: Outbox requeue
        OutboxEvent stalledEvent = OutboxEvent.builder()
                .id(UUID.randomUUID())
                .aggregateType("transaction-events")
                .aggregateId("tx-lag")
                .eventType("FundsHoldRequested")
                .payload("{\"test\": true}")
                .status("FAILED")
                .createdAt(Instant.now().minusSeconds(300))
                .updatedAt(Instant.now().minusSeconds(300))
                .build();
        outboxEventRepository.save(stalledEvent);

        ReconciliationAnomaly outboxAnomaly = reconciliationService.recordOrUpdateAnomaly(
                UUID.randomUUID(), "OUTBOX-GLOBAL", null, null, "EVENT_DISPATCH_LAG", "MEDIUM", "{}");

        String outboxResult = reconciliationService.remediateAnomaly(outboxAnomaly.getId(), "test-operator");
        assertThat(outboxResult).startsWith("SUCCESS");

        // Dangerous Action: Critical ledger mismatch
        ReconciliationAnomaly criticalAnomaly = reconciliationService.recordOrUpdateAnomaly(
                UUID.randomUUID(), "TX-CRIT-01", sourceAccountId, null, "CRITICAL_LEDGER_MISMATCH", "CRITICAL", "{}");

        String mutationResult = reconciliationService.remediateAnomaly(criticalAnomaly.getId(), "test-operator");
        assertThat(mutationResult).startsWith("REJECTED");
        assertThat(mutationResult).contains("strictly prohibited by DFTP financial rules");
    }
}
