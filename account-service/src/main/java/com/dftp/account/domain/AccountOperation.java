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
@Table(name = "account_operations")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AccountOperation {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "account_id", nullable = false)
    private UUID accountId;

    @Column(name = "transaction_id", nullable = false)
    private String transactionId;

    @Column(name = "operation_type", nullable = false)
    private String operationType;

    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();
}
