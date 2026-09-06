package com.dftp.ledger.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface LedgerTransactionRepository extends JpaRepository<LedgerTransaction, UUID> {
    Optional<LedgerTransaction> findByBusinessTransactionId(String businessTransactionId);
    Optional<LedgerTransaction> findByLedgerTransactionId(String ledgerTransactionId);
}
