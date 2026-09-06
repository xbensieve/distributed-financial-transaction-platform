package com.dftp.transaction.domain;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface TransactionRepository extends JpaRepository<Transaction, UUID> {
    Optional<Transaction> findByTransactionId(String transactionId);

    @Query("SELECT t FROM Transaction t WHERE t.status IN :statuses AND (t.updatedAt < :cutoff OR (t.updatedAt IS NULL AND t.createdAt < :cutoff))")
    Page<Transaction> findAnomalousTransactions(
            @Param("statuses") Collection<String> statuses,
            @Param("cutoff") Instant cutoff,
            Pageable pageable);
}
