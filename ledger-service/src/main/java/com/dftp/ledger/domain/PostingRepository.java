package com.dftp.ledger.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Repository
public interface PostingRepository extends JpaRepository<Posting, UUID> {
    List<Posting> findByLedgerTransactionId(UUID ledgerTransactionId);

    List<Posting> findByLedgerTransaction_BusinessTransactionId(String businessTransactionId);

    List<Posting> findByLedgerTransaction_BusinessTransactionIdIn(Collection<String> businessTransactionIds);
}
