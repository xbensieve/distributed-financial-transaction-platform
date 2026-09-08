package com.dftp.transaction.retention;

import com.dftp.transaction.security.SecurityAuditLogRepository;
import com.dftp.transaction.settlement.repository.SettlementBatchItemRepository;
import com.dftp.transaction.settlement.repository.SettlementBatchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;

/**
 * Scheduled Data Purge Service (P11-REM-05).
 * Enforces automated, bounded chunk-based data retention policies:
 * - Settlement metadata & items: 90 days
 * - Security audit logs: 365 days
 *
 * Implements bounded chunk loops with commits per chunk to prevent long-held database locks.
 * Never purges financial ledger entries or reconciliation anomaly history.
 */
@Slf4j
@Service
public class DataRetentionPurgeService {

    private final SettlementBatchRepository batchRepository;
    private final SettlementBatchItemRepository itemRepository;
    private final SecurityAuditLogRepository securityAuditLogRepository;
    private final TransactionTemplate transactionTemplate;

    @Value("${dftp.retention.settlement-days:90}")
    private long settlementRetentionDays;

    @Value("${dftp.retention.security-audit-days:365}")
    private long securityAuditRetentionDays;

    private static final int DEFAULT_CHUNK_SIZE = 500;

    public DataRetentionPurgeService(
            SettlementBatchRepository batchRepository,
            SettlementBatchItemRepository itemRepository,
            SecurityAuditLogRepository securityAuditLogRepository,
            PlatformTransactionManager transactionManager) {
        this.batchRepository = batchRepository;
        this.itemRepository = itemRepository;
        this.securityAuditLogRepository = securityAuditLogRepository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /**
     * Daily scheduled purge job (runs at 02:00 UTC by default).
     */
    @Scheduled(cron = "${dftp.retention.purge-cron:0 0 2 * * ?}")
    @SchedulerLock(name = "DataRetentionPurgeService_runScheduledPurge", lockAtMostFor = "PT30M", lockAtLeastFor = "PT1M")
    public void runScheduledPurge() {
        log.info("Starting scheduled data retention purge job...");
        int purgedItems = purgeSettlementItems(Duration.ofDays(settlementRetentionDays), DEFAULT_CHUNK_SIZE);
        int purgedBatches = purgeSettlementBatches(Duration.ofDays(settlementRetentionDays), DEFAULT_CHUNK_SIZE);
        int purgedLogs = purgeSecurityAuditLogs(Duration.ofDays(securityAuditRetentionDays), DEFAULT_CHUNK_SIZE);
        log.info("Scheduled data retention purge complete: {} batch items, {} batches, {} security audit logs purged.",
                purgedItems, purgedBatches, purgedLogs);
    }

    /**
     * Bounded chunk-based purging of completed/settled batch items older than cutoff.
     */
    public int purgeSettlementItems(Duration retention, int chunkSize) {
        Instant cutoff = Instant.now().minus(retention != null ? retention : Duration.ofDays(settlementRetentionDays));
        int limit = chunkSize > 0 ? chunkSize : DEFAULT_CHUNK_SIZE;
        int totalDeleted = 0;
        int deletedInChunk;

        do {
            final int chunkLimit = limit;
            deletedInChunk = transactionTemplate.execute(status ->
                    itemRepository.deleteOldItemsChunk(cutoff, chunkLimit)
            );
            totalDeleted += deletedInChunk;
            if (deletedInChunk > 0) {
                log.debug("Purged chunk of {} settlement batch items older than {}", deletedInChunk, cutoff);
            }
        } while (deletedInChunk >= limit);

        return totalDeleted;
    }

    /**
     * Bounded chunk-based purging of empty completed/failed batches older than cutoff.
     */
    public int purgeSettlementBatches(Duration retention, int chunkSize) {
        Instant cutoff = Instant.now().minus(retention != null ? retention : Duration.ofDays(settlementRetentionDays));
        int limit = chunkSize > 0 ? chunkSize : DEFAULT_CHUNK_SIZE;
        int totalDeleted = 0;
        int deletedInChunk;

        do {
            final int chunkLimit = limit;
            deletedInChunk = transactionTemplate.execute(status ->
                    batchRepository.deleteOldBatchesChunk(cutoff, chunkLimit)
            );
            totalDeleted += deletedInChunk;
            if (deletedInChunk > 0) {
                log.debug("Purged chunk of {} settlement batches older than {}", deletedInChunk, cutoff);
            }
        } while (deletedInChunk >= limit);

        return totalDeleted;
    }

    /**
     * Bounded chunk-based purging of security audit logs older than cutoff.
     */
    public int purgeSecurityAuditLogs(Duration retention, int chunkSize) {
        Instant cutoff = Instant.now().minus(retention != null ? retention : Duration.ofDays(securityAuditRetentionDays));
        int limit = chunkSize > 0 ? chunkSize : DEFAULT_CHUNK_SIZE;
        int totalDeleted = 0;
        int deletedInChunk;

        do {
            final int chunkLimit = limit;
            deletedInChunk = transactionTemplate.execute(status ->
                    securityAuditLogRepository.deleteOldLogsChunk(cutoff, chunkLimit)
            );
            totalDeleted += deletedInChunk;
            if (deletedInChunk > 0) {
                log.debug("Purged chunk of {} security audit logs older than {}", deletedInChunk, cutoff);
            }
        } while (deletedInChunk >= limit);

        return totalDeleted;
    }
}
