package com.dftp.transaction.reconciliation.repository;

import com.dftp.transaction.reconciliation.domain.ReconciliationAnomalyHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ReconciliationAnomalyHistoryRepository extends JpaRepository<ReconciliationAnomalyHistory, UUID> {
    List<ReconciliationAnomalyHistory> findByAnomalyIdOrderByOccurredAtAsc(UUID anomalyId);
    List<ReconciliationAnomalyHistory> findByBusinessTransactionIdOrderByOccurredAtAsc(String businessTransactionId);
}
