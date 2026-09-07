package com.dftp.transaction.settlement.repository;

import com.dftp.transaction.settlement.domain.SettlementBatch;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface SettlementBatchRepository extends JpaRepository<SettlementBatch, UUID> {
    Optional<SettlementBatch> findByBatchId(String batchId);
    boolean existsByBatchId(String batchId);
    Page<SettlementBatch> findByStatus(String status, Pageable pageable);
    List<SettlementBatch> findByStatusAndStartedAtBefore(String status, Instant cutoff);

    @org.springframework.data.jpa.repository.Modifying
    @org.springframework.data.jpa.repository.Query(value = "WITH to_delete AS (" +
                   "  SELECT b.id FROM settlement_batches b " +
                   "  WHERE b.status IN ('COMPLETED', 'FAILED') AND b.created_at < :cutoff " +
                   "    AND NOT EXISTS (SELECT 1 FROM settlement_batch_items i WHERE i.batch_id = b.batch_id) " +
                   "  LIMIT :limit" +
                   ") " +
                   "DELETE FROM settlement_batches " +
                   "WHERE id IN (SELECT id FROM to_delete)", nativeQuery = true)
    int deleteOldBatchesChunk(@org.springframework.data.repository.query.Param("cutoff") Instant cutoff,
                              @org.springframework.data.repository.query.Param("limit") int limit);
}
