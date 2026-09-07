package com.dftp.transaction.reconciliation.domain;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "reconciliation_anomalies")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReconciliationAnomaly {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "reconciliation_id", nullable = false)
    private UUID reconciliationId;

    @Column(name = "business_transaction_id", nullable = false)
    private String businessTransactionId;

    @Column(name = "account_id")
    private UUID accountId;

    @Column(name = "ledger_transaction_id")
    private String ledgerTransactionId;

    @Column(name = "anomaly_type", nullable = false)
    private String anomalyType;

    @Column(name = "severity", nullable = false)
    private String severity; // LOW, MEDIUM, HIGH, CRITICAL

    @Column(name = "status", nullable = false)
    @Builder.Default
    private String status = "DETECTED"; // DETECTED, ACKNOWLEDGED, INVESTIGATING, RESOLVED, SUPPRESSED

    @Column(name = "observed_state", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String observedState;

    @Column(name = "detected_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant detectedAt = Instant.now();

    @Column(name = "last_checked_at", nullable = false)
    @Builder.Default
    private Instant lastCheckedAt = Instant.now();

    @Column(name = "resolution")
    private String resolution;

    @Column(name = "resolved_at")
    private Instant resolvedAt;

    @Column(name = "resolved_by")
    private String resolvedBy;
}
