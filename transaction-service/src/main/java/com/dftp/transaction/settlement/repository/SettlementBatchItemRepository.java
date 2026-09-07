package com.dftp.transaction.settlement.repository;

import com.dftp.transaction.settlement.domain.SettlementBatchItem;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface SettlementBatchItemRepository extends JpaRepository<SettlementBatchItem, UUID> {

    Optional<SettlementBatchItem> findByBatchIdAndTransactionId(String batchId, String transactionId);

    List<SettlementBatchItem> findByBatchId(String batchId);

    Page<SettlementBatchItem> findByBatchId(String batchId, Pageable pageable);

    Page<SettlementBatchItem> findByBatchIdAndStatus(String batchId, String status, Pageable pageable);

    long countByBatchId(String batchId);

    long countByBatchIdAndStatus(String batchId, String status);

    /**
     * Atomically claims bounded batch items using PostgreSQL row locking with SKIP LOCKED.
     * Prevents concurrent workers or replicas from processing the same items.
     */
    @Query(value = "SELECT * FROM settlement_batch_items " +
                   "WHERE batch_id = :batchId AND status IN ('PENDING', 'FAILED') " +
                   "ORDER BY created_at ASC " +
                   "LIMIT :limit " +
                   "FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<SettlementBatchItem> claimNextItemsForProcessing(@Param("batchId") String batchId, @Param("limit") int limit);

    /**
     * Identifies items stuck in PROCESSING past the recovery cutoff (worker crash window).
     */
    @Query(value = "SELECT * FROM settlement_batch_items " +
                   "WHERE status = 'PROCESSING' AND updated_at < :cutoff " +
                   "LIMIT :limit " +
                   "FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<SettlementBatchItem> claimStalledProcessingItems(@Param("cutoff") Instant cutoff, @Param("limit") int limit);

    @org.springframework.data.jpa.repository.Modifying
    @Query(value = "WITH to_delete AS (" +
                   "  SELECT id FROM settlement_batch_items " +
                   "  WHERE status IN ('SETTLED', 'SKIPPED', 'FAILED') AND created_at < :cutoff " +
                   "  LIMIT :limit" +
                   ") " +
                   "DELETE FROM settlement_batch_items " +
                   "WHERE id IN (SELECT id FROM to_delete)", nativeQuery = true)
    int deleteOldItemsChunk(@Param("cutoff") Instant cutoff, @Param("limit") int limit);
}
