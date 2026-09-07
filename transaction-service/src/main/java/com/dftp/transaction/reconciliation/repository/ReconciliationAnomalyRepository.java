package com.dftp.transaction.reconciliation.repository;

import com.dftp.transaction.reconciliation.domain.ReconciliationAnomaly;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ReconciliationAnomalyRepository extends JpaRepository<ReconciliationAnomaly, UUID> {

    Optional<ReconciliationAnomaly> findByBusinessTransactionIdAndAnomalyType(String businessTransactionId, String anomalyType);

    List<ReconciliationAnomaly> findByStatus(String status);

    Page<ReconciliationAnomaly> findByStatus(String status, Pageable pageable);

    Page<ReconciliationAnomaly> findBySeverity(String severity, Pageable pageable);

    Page<ReconciliationAnomaly> findByAnomalyType(String anomalyType, Pageable pageable);

    Page<ReconciliationAnomaly> findByStatusAndSeverity(String status, String severity, Pageable pageable);

    long countByStatus(String status);

    long countBySeverity(String severity);

    long countByAnomalyType(String anomalyType);
}
