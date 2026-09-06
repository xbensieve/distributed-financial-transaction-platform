package com.dftp.transaction.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "transactions")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "transaction_id", nullable = false, unique = true)
    private String transactionId;

    @Column(name = "owner_id", nullable = false)
    @Builder.Default
    private String ownerId = "system-unassigned";

    @Column(name = "source_account_id", nullable = false)
    private UUID sourceAccountId;

    @Column(name = "destination_account_id", nullable = false)
    private UUID destinationAccountId;

    @Column(name = "amount", nullable = false)
    private BigDecimal amount;

    @Column(name = "currency", nullable = false)
    private String currency;

    @Column(name = "status", nullable = false)
    @Builder.Default
    private String status = "PENDING"; // PENDING, CONFIRMED, REJECTED

    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    @Builder.Default
    private Instant updatedAt = Instant.now();

    @jakarta.persistence.Version
    @Column(name = "version", nullable = false)
    @Builder.Default
    private Long version = 0L;

    @jakarta.persistence.PrePersist
    protected void onPrePersist() {
        if (this.ownerId == null || this.ownerId.isBlank()) {
            this.ownerId = "system-unassigned";
        }
    }

    @jakarta.persistence.PreUpdate
    protected void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
