package com.dftp.account.domain;

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
@Table(name = "accounts")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Account {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "transaction_id", nullable = false, unique = true)
    private String transactionId;

    @Column(name = "owner_id", nullable = false)
    @Builder.Default
    private String ownerId = "system-unassigned";

    @Column(name = "status", nullable = false)
    @Builder.Default
    private String status = "ACTIVE";

    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    @Column(name = "settled_balance", nullable = false)
    @Builder.Default
    private java.math.BigDecimal settledBalance = java.math.BigDecimal.ZERO;

    @Column(name = "held_funds", nullable = false)
    @Builder.Default
    private java.math.BigDecimal heldFunds = java.math.BigDecimal.ZERO;

    @jakarta.persistence.Version
    @Column(name = "version", nullable = false)
    private Long version;
}
