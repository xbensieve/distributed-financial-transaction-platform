package com.dftp.ledger.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "ledger_transactions")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LedgerTransaction {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "ledger_transaction_id", nullable = false, unique = true, updatable = false)
    private String ledgerTransactionId;

    @Column(name = "business_transaction_id", nullable = false, unique = true, updatable = false)
    private String businessTransactionId;

    @Column(name = "status", nullable = false)
    @Builder.Default
    private String status = "POSTED";

    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();
}
