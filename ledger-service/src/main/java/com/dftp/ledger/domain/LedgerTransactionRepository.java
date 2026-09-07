package com.dftp.ledger.domain;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface LedgerTransactionRepository extends JpaRepository<LedgerTransaction, UUID> {
    Optional<LedgerTransaction> findByBusinessTransactionId(String businessTransactionId);
    Optional<LedgerTransaction> findByLedgerTransactionId(String ledgerTransactionId);

    Page<LedgerTransaction> findByCreatedAtBefore(Instant cutoff, Pageable pageable);

    List<LedgerTransaction> findByBusinessTransactionIdIn(Collection<String> businessTransactionIds);
}

