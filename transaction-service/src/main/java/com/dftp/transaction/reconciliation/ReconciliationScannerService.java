package com.dftp.transaction.reconciliation;

import com.dftp.common.observability.DftpMetrics;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Read-only Reconciliation Observability Scanner.
 * Detects distributed Saga and ledger state divergences without mutating financial state.
 * Emits Prometheus reconciliation anomaly metrics and produces operational audit reports.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReconciliationScannerService {

    private final TransactionRepository transactionRepository;
    private final OutboxEventRepository outboxEventRepository;

    @Autowired(required = false)
    private DftpMetrics dftpMetrics = new DftpMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());

    public static final Duration SAGA_STUCK_THRESHOLD = Duration.ofMinutes(5);
    public static final Duration OUTBOX_STALLED_THRESHOLD = Duration.ofMinutes(2);

    @Getter
    @Builder
    public static class ReconciliationReport {
        private Instant scanTimestamp;
        private int totalAnomaliesDetected;
        private int fundsHeldLedgerAbsentCount;
        private int ledgerPostedIncompleteCount;
        private int transactionStuckPendingCount;
        private int stalledOutboxCount;
        private List<String> anomalySummaries;
    }

    @Scheduled(cron = "${dftp.reconciliation.anomaly-scan-cron:-}")
    @SchedulerLock(name = "ReconciliationScannerService_scheduledAnomalyScan", lockAtMostFor = "PT15M", lockAtLeastFor = "PT30S")
    public void scheduledAnomalyScan() {
        log.info("Executing scheduled read-only reconciliation anomaly scan...");
        try {
            scanForAnomalies(SAGA_STUCK_THRESHOLD, OUTBOX_STALLED_THRESHOLD);
        } catch (Exception e) {
            log.error("Scheduled read-only reconciliation anomaly scan failed: {}", e.getMessage(), e);
        }
    }

    @Transactional(readOnly = true)
    public ReconciliationReport scanForAnomalies(Duration sagaThreshold, Duration outboxThreshold) {
        Instant now = Instant.now();
        Instant sagaCutoff = now.minus(sagaThreshold != null ? sagaThreshold : SAGA_STUCK_THRESHOLD);
        Instant outboxCutoff = now.minus(outboxThreshold != null ? outboxThreshold : OUTBOX_STALLED_THRESHOLD);

        List<String> summaries = new ArrayList<>();
        int heldAbsentCount = 0;
        int ledgerIncompleteCount = 0;
        int pendingStuckCount = 0;

        int pageNumber = 0;
        int pageSize = 100;
        List<String> targetStatuses = List.of("PENDING", "SOURCE_HELD", "LEDGER_POSTED");
        org.springframework.data.domain.Page<Transaction> page;
        do {
            page = transactionRepository.findAnomalousTransactions(
                    targetStatuses, sagaCutoff, org.springframework.data.domain.PageRequest.of(pageNumber, pageSize));
            for (Transaction tx : page.getContent()) {
                Instant updatedAt = tx.getUpdatedAt() != null ? tx.getUpdatedAt() : tx.getCreatedAt();
                if ("SOURCE_HELD".equals(tx.getStatus())) {
                    heldAbsentCount++;
                    dftpMetrics.recordReconciliationAnomaly("FUNDS_HELD_LEDGER_ABSENT");
                    summaries.add(String.format("ANOMALY: Funds held without ledger confirmation for tx '%s' (since %s)",
                            tx.getTransactionId(), updatedAt));
                } else if ("LEDGER_POSTED".equals(tx.getStatus())) {
                    ledgerIncompleteCount++;
                    dftpMetrics.recordReconciliationAnomaly("LEDGER_POSTED_INCOMPLETE");
                    summaries.add(String.format("ANOMALY: Ledger posted but tx '%s' not finalized (since %s)",
                            tx.getTransactionId(), updatedAt));
                } else if ("PENDING".equals(tx.getStatus())) {
                    pendingStuckCount++;
                    dftpMetrics.recordReconciliationAnomaly("TRANSACTION_STUCK_PENDING");
                    summaries.add(String.format("ANOMALY: Transaction '%s' stuck in PENDING (since %s)",
                            tx.getTransactionId(), updatedAt));
                }
            }
            pageNumber++;
        } while (page.hasNext());

        // Check Outbox staleness using indexed bounded check without loading unbounded rows into memory
        long pendingOutbox = outboxEventRepository.countByStatus("PENDING");
        int stalledOutbox = 0;
        if (pendingOutbox > 0 && outboxEventRepository.existsByStatusAndCreatedAtBefore("PENDING", outboxCutoff)) {
            stalledOutbox = (int) pendingOutbox;
            dftpMetrics.recordReconciliationAnomaly("STALLED_OUTBOX");
            summaries.add(String.format("ANOMALY: Outbox relay stalled with %d pending events older than threshold", pendingOutbox));
        }

        int total = heldAbsentCount + ledgerIncompleteCount + pendingStuckCount + stalledOutbox;
        log.info("Reconciliation scan complete: {} anomalies detected across distributed contexts", total);

        return ReconciliationReport.builder()
                .scanTimestamp(now)
                .totalAnomaliesDetected(total)
                .fundsHeldLedgerAbsentCount(heldAbsentCount)
                .ledgerPostedIncompleteCount(ledgerIncompleteCount)
                .transactionStuckPendingCount(pendingStuckCount)
                .stalledOutboxCount(stalledOutbox)
                .anomalySummaries(summaries)
                .build();
    }
}
