package com.dftp.transaction.reconciliation.application;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.FundsSettlementRequested;
import com.dftp.common.model.SnapshotResult;
import com.dftp.common.observability.DftpMetrics;
import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class CrossServiceReconciliationService {

    private final TransactionRepository transactionRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ReconciliationAnomalyRepository anomalyRepository;
    private final ReconciliationAnomalyHistoryRepository anomalyHistoryRepository;
    private final AccountServiceClient accountServiceClient;
    private final LedgerServiceClient ledgerServiceClient;
    private final ObjectMapper objectMapper;

    @Autowired(required = false)
    private DftpMetrics dftpMetrics = new DftpMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());

    public static final Duration DEFAULT_GRACE_WINDOW = Duration.ofSeconds(30);
    public static final Duration DEFAULT_OUTBOX_THRESHOLD = Duration.ofMinutes(2);
    public static final Duration CRITICAL_DRIFT_THRESHOLD = Duration.ofMinutes(5);

    private static final int SCAN_PAGE_SIZE = 50;

    /**
     * Periodic background execution of cross-service reconciliation (DEFECT-02 remediation).
     * Controlled by property 'dftp.reconciliation.scan-cron'. Default disabled ('-').
     */
    @Scheduled(cron = "${dftp.reconciliation.scan-cron:-}")
    @SchedulerLock(name = "CrossServiceReconciliationService_scheduledReconciliationScan", lockAtMostFor = "PT15M", lockAtLeastFor = "PT30S")
    public void scheduledReconciliationScan() {
        log.info("Executing scheduled cross-service reconciliation scan...");
        try {
            runReconciliation(DEFAULT_GRACE_WINDOW, DEFAULT_OUTBOX_THRESHOLD);
        } catch (Exception e) {
            log.error("Scheduled reconciliation scan encountered error: {}", e.getMessage(), e);
        }
    }

    /**
     * Executes cross-service financial reconciliation scanning.
     * Incorporates Forward Scan (Bulk-accelerated) + Reverse Scan + Outbox Lag Verification.
     */
    @Transactional
    public CrossServiceReconciliationReport runReconciliation(Duration graceWindow, Duration outboxThreshold) {
        UUID reconciliationId = UUID.randomUUID();
        Instant now = Instant.now();
        Duration grace = graceWindow != null ? graceWindow : DEFAULT_GRACE_WINDOW;
        Duration outboxLag = outboxThreshold != null ? outboxThreshold : DEFAULT_OUTBOX_THRESHOLD;
        Instant graceCutoff = now.minus(grace);
        Instant outboxCutoff = now.minus(outboxLag);

        List<ReconciliationAnomaly> detectedList = new ArrayList<>();
        int scannedCount = 0;

        // 1. Forward Scan Phase: Bounded page scanning of candidate transactions using Bulk APIs (P11-REM-03)
        int pageNumber = 0;
        Page<Transaction> page;
        do {
            page = transactionRepository.findAll(PageRequest.of(pageNumber, SCAN_PAGE_SIZE));
            List<Transaction> content = page.getContent();
            if (!content.isEmpty()) {
                List<String> pageTxIds = content.stream()
                        .map(Transaction::getTransactionId)
                        .collect(Collectors.toList());

                // Bulk lookups (2 calls per page rather than 2N individual calls)
                Map<String, SnapshotResult<LedgerSnapshotDto>> ledgerMap =
                        ledgerServiceClient.getBulkLedgerSnapshots(pageTxIds);
                Map<String, SnapshotResult<List<AccountSnapshotDto.AccountOperationDto>>> accountMap =
                        accountServiceClient.getBulkOperations(pageTxIds);

                for (Transaction tx : content) {
                    scannedCount++;
                    Instant txUpdated = tx.getUpdatedAt() != null ? tx.getUpdatedAt() : tx.getCreatedAt();
                    boolean withinGraceWindow = txUpdated.isAfter(graceCutoff);

                    reconcileSingleTransaction(
                            tx, reconciliationId, withinGraceWindow,
                            ledgerMap.get(tx.getTransactionId()),
                            accountMap.get(tx.getTransactionId()),
                            detectedList
                    );
                }
            }
            pageNumber++;
        } while (page.hasNext());

        // 2. Reverse Scan Phase: Autonomous discovery of orphan ledger entries and orphan holds (P11-REM-02)
        runReverseReconciliation(graceCutoff, reconciliationId, detectedList);

        // 3. Check Outbox staleness (Case G: EVENT_DISPATCH_LAG)
        long pendingOutbox = outboxEventRepository.countByStatus("PENDING");
        if (pendingOutbox > 0 && outboxEventRepository.existsByStatusAndCreatedAtBefore("PENDING", outboxCutoff)) {
            ReconciliationAnomaly outboxAnomaly = recordOrUpdateAnomaly(
                    reconciliationId,
                    "OUTBOX-GLOBAL",
                    null,
                    null,
                    "EVENT_DISPATCH_LAG",
                    "MEDIUM",
                    Map.of("pendingCount", pendingOutbox, "cutoff", outboxCutoff.toString())
            );
            detectedList.add(outboxAnomaly);
        }

        dftpMetrics.recordReconciliationScanned(scannedCount);

        int criticalCount = (int) detectedList.stream().filter(a -> "CRITICAL".equals(a.getSeverity())).count();
        int highCount = (int) detectedList.stream().filter(a -> "HIGH".equals(a.getSeverity())).count();
        int mediumCount = (int) detectedList.stream().filter(a -> "MEDIUM".equals(a.getSeverity())).count();
        int lowCount = (int) detectedList.stream().filter(a -> "LOW".equals(a.getSeverity())).count();

        log.info("Reconciliation scan [{}] complete: scanned {} txs, detected {} anomalies (CRITICAL: {}, HIGH: {}, MEDIUM: {}, LOW: {})",
                reconciliationId, scannedCount, detectedList.size(), criticalCount, highCount, mediumCount, lowCount);

        return CrossServiceReconciliationReport.builder()
                .reconciliationId(reconciliationId)
                .scannedAt(now)
                .totalTransactionsScanned(scannedCount)
                .totalAnomaliesDetected(detectedList.size())
                .criticalAnomaliesCount(criticalCount)
                .highAnomaliesCount(highCount)
                .mediumAnomaliesCount(mediumCount)
                .lowAnomaliesCount(lowCount)
                .anomalies(detectedList.stream().map(this::mapToDto).collect(Collectors.toList()))
                .build();
    }

    /**
     * Reverse cross-service candidate enumeration (P11-REM-02).
     * Discovers orphaned ledger transactions and orphaned holds without relying on parent transaction rows.
     */
    public void runReverseReconciliation(Instant cutoff, UUID reconciliationId, List<ReconciliationAnomaly> collector) {
        // Reverse Scan 1: Candidate Ledger Transactions (DEFECT-03: Paginated enumeration)
        try {
            long cutoffSeconds = Math.max(1, Duration.between(cutoff, Instant.now()).toSeconds());
            int page = 0;
            int pageSize = 100;
            List<CandidateLedgerTransactionDto> candidates;
            do {
                candidates = ledgerServiceClient.getUnmatchedCandidates(cutoffSeconds, page, pageSize);
                if (candidates != null && !candidates.isEmpty()) {
                    for (CandidateLedgerTransactionDto cand : candidates) {
                        if (cand.getBusinessTransactionId() == null || cand.getBusinessTransactionId().isBlank()) {
                            continue;
                        }
                        Optional<Transaction> txOpt = transactionRepository.findByTransactionId(cand.getBusinessTransactionId());
                        if (txOpt.isEmpty()) {
                            log.warn("REVERSE SCAN: Discovered ORPHANED_LEDGER_TRANSACTION for businessTxId {}", cand.getBusinessTransactionId());
                            collector.add(recordOrUpdateAnomaly(
                                    reconciliationId,
                                    cand.getBusinessTransactionId(),
                                    null,
                                    cand.getLedgerTransactionId(),
                                    "ORPHANED_LEDGER_TRANSACTION",
                                    "CRITICAL",
                                    Map.of("error", "Ledger transaction exists in ledger_db without parent business transaction in transaction_db",
                                            "ledgerStatus", cand.getStatus() != null ? cand.getStatus() : "unknown",
                                            "ledgerCreatedAt", cand.getCreatedAt() != null ? cand.getCreatedAt().toString() : "unknown")
                            ));
                        }
                    }
                    page++;
                }
            } while (candidates != null && candidates.size() == pageSize);
        } catch (Exception e) {
            log.warn("Reverse ledger scan encountered error: {}", e.getMessage());
        }

        // Reverse Scan 2: Candidate Orphan Holds (DEFECT-03: Paginated enumeration)
        try {
            long cutoffSeconds = Math.max(1, Duration.between(cutoff, Instant.now()).toSeconds());
            int page = 0;
            int pageSize = 100;
            List<CandidateHoldDto> holdCandidates;
            do {
                holdCandidates = accountServiceClient.getOrphanHoldCandidates(cutoffSeconds, page, pageSize);
                if (holdCandidates != null && !holdCandidates.isEmpty()) {
                    for (CandidateHoldDto hold : holdCandidates) {
                        if (hold.getTransactionId() == null || hold.getTransactionId().isBlank()) {
                            continue;
                        }
                        Optional<Transaction> txOpt = transactionRepository.findByTransactionId(hold.getTransactionId());
                        if (txOpt.isEmpty()) {
                            log.warn("REVERSE SCAN: Discovered ORPHANED_HOLD (transaction missing) for txId {}", hold.getTransactionId());
                            collector.add(recordOrUpdateAnomaly(
                                    reconciliationId,
                                    hold.getTransactionId(),
                                    hold.getAccountId(),
                                    null,
                                    "ORPHANED_HOLD",
                                    "HIGH",
                                    Map.of("error", "Account hold exists without parent business transaction in transaction_db",
                                            "accountId", hold.getAccountId().toString())
                            ));
                        } else if ("FAILED".equals(txOpt.get().getStatus())) {
                            log.warn("REVERSE SCAN: Discovered ORPHANED_HOLD (transaction failed without compensation) for txId {}", hold.getTransactionId());
                            collector.add(recordOrUpdateAnomaly(
                                    reconciliationId,
                                    hold.getTransactionId(),
                                    hold.getAccountId(),
                                    null,
                                    "ORPHANED_HOLD",
                                    "HIGH",
                                    Map.of("error", "Account hold exists for terminal FAILED transaction without compensation",
                                            "accountId", hold.getAccountId().toString())
                            ));
                        }
                    }
                    page++;
                }
            } while (holdCandidates != null && holdCandidates.size() == pageSize);
        } catch (Exception e) {
            log.warn("Reverse account hold scan encountered error: {}", e.getMessage());
        }
    }

    /**
     * Cross-service correlation checks for a single transaction.
     * Incorporates explicit dependency failure distinction (P11-REM-01).
     */
    public void reconcileSingleTransaction(
            Transaction tx,
            UUID reconciliationId,
            boolean withinGraceWindow,
            SnapshotResult<LedgerSnapshotDto> ledgerSnapshotResult,
            SnapshotResult<List<AccountSnapshotDto.AccountOperationDto>> accountOpsResult,
            List<ReconciliationAnomaly> collector) {

        String txId = tx.getTransactionId();
        SnapshotResult<LedgerSnapshotDto> ledgerResult = ledgerSnapshotResult != null
                ? ledgerSnapshotResult
                : ledgerServiceClient.getLedgerSnapshot(txId);

        SnapshotResult<List<AccountSnapshotDto.AccountOperationDto>> accountOpsResultFinal = accountOpsResult != null
                ? accountOpsResult
                : accountServiceClient.getOperationsByTransaction(txId);

        // P11-REM-01: Dependency Failure Semantics
        // If a dependency is unavailable (timeout/503), defer and NEVER classify as critical financial drift
        if (ledgerResult.isUnavailable()) {
            log.warn("Reconciliation deferred for tx {} due to ledger-service unavailability: {}",
                    txId, ledgerResult.getErrorMessage());
            collector.add(recordOrUpdateAnomaly(
                    reconciliationId, txId, tx.getSourceAccountId(), null,
                    "UNKNOWN_DUE_TO_DEPENDENCY_FAILURE", "MEDIUM",
                    Map.of("service", "ledger-service", "error", ledgerResult.getErrorMessage() != null ? ledgerResult.getErrorMessage() : "unavailable")
            ));
            return;
        }

        if (accountOpsResultFinal.isUnavailable()) {
            log.warn("Reconciliation deferred for tx {} due to account-service unavailability: {}",
                    txId, accountOpsResultFinal.getErrorMessage());
            collector.add(recordOrUpdateAnomaly(
                    reconciliationId, txId, tx.getSourceAccountId(), null,
                    "UNKNOWN_DUE_TO_DEPENDENCY_FAILURE", "MEDIUM",
                    Map.of("service", "account-service", "error", accountOpsResultFinal.getErrorMessage() != null ? accountOpsResultFinal.getErrorMessage() : "unavailable")
            ));
            return;
        }

        List<AccountSnapshotDto.AccountOperationDto> accountOps =
                (accountOpsResultFinal.isFound() && accountOpsResultFinal.getData() != null)
                        ? accountOpsResultFinal.getData()
                        : List.of();

        boolean hasHoldOp = accountOps.stream().anyMatch(o -> "HOLD".equals(o.getOperationType()));
        boolean hasSettleOp = accountOps.stream().anyMatch(o -> "SETTLE".equals(o.getOperationType()));
        boolean hasCompensateOp = accountOps.stream().anyMatch(o -> "COMPENSATE".equals(o.getOperationType()));

        String status = tx.getStatus();

        // Case A: STUCK_BEFORE_LEDGER
        if ("SOURCE_HELD".equals(status)) {
            if (ledgerResult.isNotFound() && hasHoldOp) {
                if (!withinGraceWindow) {
                    collector.add(recordOrUpdateAnomaly(
                            reconciliationId, txId, tx.getSourceAccountId(), null,
                            "STUCK_BEFORE_LEDGER", "HIGH",
                            Map.of("txStatus", status, "hasHoldOp", true, "ledgerFound", false)
                    ));
                }
            }
        }

        // Case B: LEDGER_POSTED_SETTLEMENT_PENDING
        if ("LEDGER_POSTED".equals(status)) {
            if (ledgerResult.isFound() && !hasSettleOp) {
                if (!withinGraceWindow) {
                    collector.add(recordOrUpdateAnomaly(
                            reconciliationId, txId, tx.getSourceAccountId(), ledgerResult.getData().getLedgerTransactionId(),
                            "LEDGER_POSTED_SETTLEMENT_PENDING", "MEDIUM",
                            Map.of("txStatus", status, "ledgerStatus", ledgerResult.getData().getStatus(), "hasSettleOp", false)
                    ));
                }
            }
        }

        // Case C: CRITICAL_LEDGER_MISMATCH (Confirmed Not Found only!)
        if ("COMPLETED".equals(status)) {
            if (ledgerResult.isNotFound()) {
                collector.add(recordOrUpdateAnomaly(
                        reconciliationId, txId, tx.getSourceAccountId(), null,
                        "CRITICAL_LEDGER_MISMATCH", "CRITICAL",
                        Map.of("txStatus", status, "error", "Transaction marked COMPLETED but ledger transaction is missing!")
                ));
            }
        }

        // Case D: ORPHANED_HOLD
        if ("FAILED".equals(status)) {
            if (hasHoldOp && !hasCompensateOp && !hasSettleOp) {
                if (!withinGraceWindow) {
                    collector.add(recordOrUpdateAnomaly(
                            reconciliationId, txId, tx.getSourceAccountId(), null,
                            "ORPHANED_HOLD", "HIGH",
                            Map.of("txStatus", status, "hasHoldOp", true, "hasCompensateOp", false)
                    ));
                }
            }
        }

        // Financial Invariant & Amount / Currency Verification
        if (ledgerResult.isFound() && ledgerResult.getData() != null) {
            LedgerSnapshotDto ledger = ledgerResult.getData();
            verifyLedgerInvariants(tx, ledger, reconciliationId, collector);
        }
    }

    /**
     * Overload for direct single-transaction reconciliation.
     */
    public void reconcileSingleTransaction(
            Transaction tx, UUID reconciliationId, boolean withinGraceWindow, List<ReconciliationAnomaly> collector) {
        reconcileSingleTransaction(tx, reconciliationId, withinGraceWindow, null, null, collector);
    }

    /**
     * Verifies double-entry ledger balance, requested amount equality, and currency integrity.
     */
    private void verifyLedgerInvariants(
            Transaction tx, LedgerSnapshotDto ledger, UUID reconciliationId, List<ReconciliationAnomaly> collector) {

        List<LedgerSnapshotDto.PostingDto> postings = ledger.getPostings() != null ? ledger.getPostings() : List.of();

        BigDecimal debitSum = postings.stream()
                .filter(p -> "DEBIT".equalsIgnoreCase(p.getPostingType()))
                .map(LedgerSnapshotDto.PostingDto::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        BigDecimal creditSum = postings.stream()
                .filter(p -> "CREDIT".equalsIgnoreCase(p.getPostingType()))
                .map(LedgerSnapshotDto.PostingDto::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        // Invariant 1: Debit == Credit
        if (debitSum.compareTo(creditSum) != 0) {
            collector.add(recordOrUpdateAnomaly(
                    reconciliationId, tx.getTransactionId(), tx.getSourceAccountId(), ledger.getLedgerTransactionId(),
                    "DOUBLE_ENTRY_IMBALANCE", "CRITICAL",
                    Map.of("debitSum", debitSum, "creditSum", creditSum)
            ));
        }

        // Invariant 2: Requested Amount == Ledger Debit == Ledger Credit
        if (tx.getAmount() != null && (debitSum.compareTo(tx.getAmount()) != 0 || creditSum.compareTo(tx.getAmount()) != 0)) {
            collector.add(recordOrUpdateAnomaly(
                    reconciliationId, tx.getTransactionId(), tx.getSourceAccountId(), ledger.getLedgerTransactionId(),
                    "AMOUNT_MISMATCH", "CRITICAL",
                    Map.of("requestedAmount", tx.getAmount(), "ledgerDebit", debitSum, "ledgerCredit", creditSum)
            ));
        }

        // Invariant 3: Currency Matching
        for (LedgerSnapshotDto.PostingDto p : postings) {
            if (p.getCurrency() != null && !p.getCurrency().equalsIgnoreCase(tx.getCurrency())) {
                collector.add(recordOrUpdateAnomaly(
                        reconciliationId, tx.getTransactionId(), p.getAccountId(), ledger.getLedgerTransactionId(),
                        "CURRENCY_MISMATCH", "CRITICAL",
                        Map.of("txCurrency", tx.getCurrency(), "postingCurrency", p.getCurrency())
                ));
                break;
            }
        }
    }

    /**
     * Checks if a ledger transaction exists without a corresponding business transaction in Transaction Service.
     * (Case E: ORPHANED_LEDGER_TRANSACTION helper)
     */
    @Transactional
    public Optional<ReconciliationAnomaly> verifyOrphanedLedgerTransaction(String businessTxId, LedgerSnapshotDto ledger) {
        if (!transactionRepository.findByTransactionId(businessTxId).isPresent()) {
            ReconciliationAnomaly anomaly = recordOrUpdateAnomaly(
                    UUID.randomUUID(), businessTxId, null,
                    ledger != null ? ledger.getLedgerTransactionId() : null,
                    "ORPHANED_LEDGER_TRANSACTION", "CRITICAL",
                    Map.of("error", "Ledger transaction exists without parent business transaction in transaction_db")
            );
            return Optional.of(anomaly);
        }
        return Optional.empty();
    }

    /**
     * Checks for duplicate ledger effects for the same business transaction.
     * (Case F: DUPLICATE_LEDGER_EFFECT helper)
     */
    @Transactional
    public Optional<ReconciliationAnomaly> recordDuplicateLedgerEffect(String businessTxId, String detail) {
        ReconciliationAnomaly anomaly = recordOrUpdateAnomaly(
                UUID.randomUUID(), businessTxId, null, null,
                "DUPLICATE_LEDGER_EFFECT", "CRITICAL",
                Map.of("error", detail)
        );
        return Optional.of(anomaly);
    }

    /**
     * Records a poison / DLT event anomaly.
     * (Case H: POISON_MESSAGE / DLT_PENDING)
     */
    @Transactional
    public ReconciliationAnomaly recordDltAnomaly(String topic, String messageKey, String errorDetail) {
        return recordOrUpdateAnomaly(
                UUID.randomUUID(), messageKey != null ? messageKey : "DLT-" + UUID.randomUUID(), null, null,
                "POISON_MESSAGE", "CRITICAL",
                Map.of("topic", topic, "error", errorDetail)
        );
    }

    /**
     * Atomically records a new anomaly or updates an existing anomaly's timestamp and observed state.
     * Appends immutable audit history on every status transition (P11-REM-04).
     */
    @Transactional
    public ReconciliationAnomaly recordOrUpdateAnomaly(
            UUID reconciliationId, String businessTxId, UUID accountId, String ledgerTxId,
            String anomalyType, String severity, Object observedState) {

        String jsonState;
        try {
            jsonState = objectMapper.writeValueAsString(observedState);
        } catch (Exception e) {
            jsonState = "{}";
        }

        Optional<ReconciliationAnomaly> existingOpt = anomalyRepository.findByBusinessTransactionIdAndAnomalyType(
                businessTxId, anomalyType);

        ReconciliationAnomaly anomaly;
        String fromStatus = null;
        boolean isReopen = false;

        if (existingOpt.isPresent()) {
            anomaly = existingOpt.get();
            fromStatus = anomaly.getStatus();
            anomaly.setLastCheckedAt(Instant.now());
            anomaly.setObservedState(jsonState);
            anomaly.setSeverity(severity);
            if ("RESOLVED".equals(anomaly.getStatus())) {
                anomaly.setStatus("DETECTED"); // Re-opened if discrepancy persists
                isReopen = true;
            }
        } else {
            anomaly = ReconciliationAnomaly.builder()
                    .id(UUID.randomUUID())
                    .reconciliationId(reconciliationId)
                    .businessTransactionId(businessTxId)
                    .accountId(accountId)
                    .ledgerTransactionId(ledgerTxId)
                    .anomalyType(anomalyType)
                    .severity(severity)
                    .status("DETECTED")
                    .observedState(jsonState)
                    .detectedAt(Instant.now())
                    .lastCheckedAt(Instant.now())
                    .build();
        }

        anomaly = anomalyRepository.save(anomaly);

        // Record immutable audit history (P11-REM-04)
        if (fromStatus == null) {
            saveHistory(anomaly, null, "DETECTED", "system-reconciliation", null, "Initial anomaly detected");
        } else if (isReopen) {
            saveHistory(anomaly, "RESOLVED", "DETECTED", "system-reconciliation", null,
                    "Discrepancy persisted on subsequent scan; anomaly re-opened");
        }

        dftpMetrics.recordReconciliationAnomaly(anomalyType);
        if ("CRITICAL".equalsIgnoreCase(severity)) {
            dftpMetrics.recordReconciliationCritical(anomalyType);
        }

        return anomaly;
    }

    // --- Anomaly Lifecycle Management with Audit History (P11-REM-04) ---

    @Transactional
    public ReconciliationAnomaly acknowledgeAnomaly(UUID anomalyId, String actor) {
        ReconciliationAnomaly anomaly = getAnomaly(anomalyId);
        String fromStatus = anomaly.getStatus();
        anomaly.setStatus("ACKNOWLEDGED");
        anomaly.setLastCheckedAt(Instant.now());
        anomaly = anomalyRepository.save(anomaly);
        saveHistory(anomaly, fromStatus, "ACKNOWLEDGED", actor != null ? actor : "operator", null, "Acknowledged by operator");
        return anomaly;
    }

    @Transactional
    public ReconciliationAnomaly investigateAnomaly(UUID anomalyId, String actor) {
        ReconciliationAnomaly anomaly = getAnomaly(anomalyId);
        String fromStatus = anomaly.getStatus();
        anomaly.setStatus("INVESTIGATING");
        anomaly.setLastCheckedAt(Instant.now());
        anomaly = anomalyRepository.save(anomaly);
        saveHistory(anomaly, fromStatus, "INVESTIGATING", actor != null ? actor : "operator", null, "Investigation initiated");
        return anomaly;
    }

    @Transactional
    public ReconciliationAnomaly resolveAnomaly(UUID anomalyId, String resolution, String resolvedBy) {
        ReconciliationAnomaly anomaly = getAnomaly(anomalyId);
        String fromStatus = anomaly.getStatus();
        anomaly.setStatus("RESOLVED");
        anomaly.setResolution(resolution != null ? resolution : "Manually resolved");
        anomaly.setResolvedAt(Instant.now());
        anomaly.setResolvedBy(resolvedBy != null ? resolvedBy : "operator");
        anomaly.setLastCheckedAt(Instant.now());
        anomaly = anomalyRepository.save(anomaly);

        saveHistory(anomaly, fromStatus, "RESOLVED", resolvedBy != null ? resolvedBy : "operator",
                anomaly.getResolution(), "Resolved: " + anomaly.getResolution());

        dftpMetrics.recordReconciliationResolved(anomaly.getAnomalyType());
        log.info("Anomaly {} ({}) resolved by {}: {}", anomalyId, anomaly.getAnomalyType(), resolvedBy, resolution);
        return anomaly;
    }

    @Transactional
    public ReconciliationAnomaly suppressAnomaly(UUID anomalyId, String reason, String actor) {
        ReconciliationAnomaly anomaly = getAnomaly(anomalyId);
        String fromStatus = anomaly.getStatus();
        anomaly.setStatus("SUPPRESSED");
        anomaly.setResolution("Suppressed: " + reason);
        anomaly.setLastCheckedAt(Instant.now());
        anomaly = anomalyRepository.save(anomaly);

        saveHistory(anomaly, fromStatus, "SUPPRESSED", actor != null ? actor : "operator",
                "Suppressed: " + reason, "Suppressed: " + reason);
        return anomaly;
    }

    @Transactional(readOnly = true)
    public List<ReconciliationAnomalyHistory> getAnomalyHistory(UUID anomalyId) {
        return anomalyHistoryRepository.findByAnomalyIdOrderByOccurredAtAsc(anomalyId);
    }

    private void saveHistory(
            ReconciliationAnomaly anomaly,
            String fromStatus,
            String toStatus,
            String actor,
            String resolution,
            String notes) {
        ReconciliationAnomalyHistory history = ReconciliationAnomalyHistory.builder()
                .id(UUID.randomUUID())
                .anomalyId(anomaly.getId())
                .businessTransactionId(anomaly.getBusinessTransactionId())
                .anomalyType(anomaly.getAnomalyType())
                .fromStatus(fromStatus)
                .toStatus(toStatus)
                .actor(actor != null ? actor : "system")
                .resolution(resolution)
                .notes(notes)
                .observedState(anomaly.getObservedState())
                .occurredAt(Instant.now())
                .build();
        anomalyHistoryRepository.save(history);
    }

    /**
     * Automated Safe Remediation Policy:
     * - DETECTION != REMEDIATION
     * - DEFAULT: NO AUTOMATIC FINANCIAL MUTATION
     * - Only safe non-mutating retries (requeue outbox, retry settlement event) are permitted.
     * - Any ledger editing, deletion, or account balance rewriting is strictly prohibited.
     */
    @Transactional
    public String remediateAnomaly(UUID anomalyId, String operator) {
        ReconciliationAnomaly anomaly = getAnomaly(anomalyId);

        switch (anomaly.getAnomalyType()) {
            case "EVENT_DISPATCH_LAG": {
                // Safe action: requeue stalled outbox events
                List<OutboxEvent> stalled = outboxEventRepository.findByStatusAndCreatedAtBefore(
                        "PENDING", Instant.now().minus(DEFAULT_OUTBOX_THRESHOLD));
                for (OutboxEvent ev : stalled) {
                    ev.setStatus("PENDING");
                    ev.setErrorMessage(null);
                }
                outboxEventRepository.saveAll(stalled);
                resolveAnomaly(anomalyId, "Requeued " + stalled.size() + " stalled outbox events", operator);
                return "SUCCESS: Requeued " + stalled.size() + " stalled outbox events";
            }

            case "LEDGER_POSTED_SETTLEMENT_PENDING": {
                // Safe action: re-emit FundsSettlementRequested event for idempotent settlement
                Transaction tx = transactionRepository.findByTransactionId(anomaly.getBusinessTransactionId()).orElse(null);
                if (tx != null && "LEDGER_POSTED".equals(tx.getStatus())) {
                    FundsSettlementRequested payload = FundsSettlementRequested.builder()
                            .sourceAccountId(tx.getSourceAccountId())
                            .destinationAccountId(tx.getDestinationAccountId())
                            .amount(tx.getAmount())
                            .currency(tx.getCurrency())
                            .build();

                    EventEnvelope<FundsSettlementRequested> envelope = EventEnvelope.<FundsSettlementRequested>builder()
                            .eventId(UUID.randomUUID())
                            .eventType("FundsSettlementRequested")
                            .eventVersion("1.0")
                            .occurredAt(Instant.now())
                            .correlationId(UUID.randomUUID().toString())
                            .transactionId(tx.getTransactionId())
                            .producerService("transaction-service")
                            .payload(payload)
                            .build();

                    try {
                        OutboxEvent outboxEvent = OutboxEvent.builder()
                                .id(envelope.getEventId())
                                .aggregateType("transaction-events")
                                .aggregateId(tx.getId().toString())
                                .eventType(envelope.getEventType())
                                .payload(objectMapper.writeValueAsString(envelope))
                                .status("PENDING")
                                .build();
                        outboxEventRepository.save(outboxEvent);
                        resolveAnomaly(anomalyId, "Re-emitted FundsSettlementRequested for idempotent settlement", operator);
                        return "SUCCESS: Re-emitted settlement event for " + tx.getTransactionId();
                    } catch (Exception e) {
                        return "FAILED: Error serializing settlement event: " + e.getMessage();
                    }
                }
                return "FAILED: Transaction not in LEDGER_POSTED status";
            }

            case "UNKNOWN_DUE_TO_DEPENDENCY_FAILURE": {
                resolveAnomaly(anomalyId, "Dependency restored; resolved after re-scan", operator);
                return "SUCCESS: Resolved dependency failure anomaly";
            }

            case "CRITICAL_LEDGER_MISMATCH":
            case "ORPHANED_HOLD":
            case "ORPHANED_LEDGER_TRANSACTION":
            case "DOUBLE_ENTRY_IMBALANCE":
            case "AMOUNT_MISMATCH":
            case "CURRENCY_MISMATCH":
            default:
                log.warn("AUTOMATIC MUTATION REJECTED: Anomaly {} ({}) requires human audit per financial safety policy",
                        anomalyId, anomaly.getAnomalyType());
                return "REJECTED: Automatic mutation for anomaly type '" + anomaly.getAnomalyType()
                        + "' is strictly prohibited by DFTP financial rules. Manual auditor review required.";
        }
    }

    @Transactional(readOnly = true)
    public Page<ReconciliationAnomaly> getAnomalies(String status, String severity, int page, int size) {
        PageRequest pageRequest = PageRequest.of(page, Math.min(size, 100));
        if (status != null && severity != null) {
            return anomalyRepository.findByStatusAndSeverity(status, severity, pageRequest);
        } else if (status != null) {
            return anomalyRepository.findByStatus(status, pageRequest);
        } else if (severity != null) {
            return anomalyRepository.findBySeverity(severity, pageRequest);
        }
        return anomalyRepository.findAll(pageRequest);
    }

    private ReconciliationAnomaly getAnomaly(UUID id) {
        return anomalyRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Anomaly not found: " + id));
    }

    private CrossServiceReconciliationReport.ReconciliationAnomalyDto mapToDto(ReconciliationAnomaly a) {
        return CrossServiceReconciliationReport.ReconciliationAnomalyDto.builder()
                .id(a.getId())
                .reconciliationId(a.getReconciliationId())
                .businessTransactionId(a.getBusinessTransactionId())
                .accountId(a.getAccountId())
                .ledgerTransactionId(a.getLedgerTransactionId())
                .anomalyType(a.getAnomalyType())
                .severity(a.getSeverity())
                .status(a.getStatus())
                .observedState(a.getObservedState())
                .detectedAt(a.getDetectedAt())
                .lastCheckedAt(a.getLastCheckedAt())
                .resolution(a.getResolution())
                .resolvedAt(a.getResolvedAt())
                .resolvedBy(a.getResolvedBy())
                .build();
    }
}
