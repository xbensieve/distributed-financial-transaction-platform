package com.dftp.transaction.domain;

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
@Table(name = "account_references")
@Data
@Builder
@NoArgsConstructor
public class AccountReference {

    @Id
    @Column(name = "account_id")
    private UUID accountId;

    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    @Column(name = "owner_id", nullable = false)
    @Builder.Default
    private String ownerId = "system-unassigned";

    public AccountReference(UUID accountId, Instant createdAt) {
        this(accountId, createdAt, "system-unassigned");
    }

    public AccountReference(UUID accountId, Instant createdAt, String ownerId) {
        this.accountId = accountId;
        this.createdAt = createdAt != null ? createdAt : Instant.now();
        this.ownerId = (ownerId != null && !ownerId.isBlank()) ? ownerId : "system-unassigned";
    }

    @jakarta.persistence.PrePersist
    public void prePersist() {
        if (this.ownerId == null || this.ownerId.isBlank()) {
            this.ownerId = "system-unassigned";
        }
    }
}
