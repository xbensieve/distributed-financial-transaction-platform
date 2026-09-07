package com.dftp.transaction.reconciliation.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "reconciliation_anomaly_history")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReconciliationAnomalyHistory {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "anomaly_id", nullable = false)
    private UUID anomalyId;

    @Column(name = "business_transaction_id", nullable = false)
    private String businessTransactionId;

    @Column(name = "anomaly_type", nullable = false)
    private String anomalyType;

    @Column(name = "from_status")
    private String fromStatus;

    @Column(name = "to_status", nullable = false)
    private String toStatus;

    @Column(name = "actor", nullable = false)
    private String actor;

    @Column(name = "resolution")
    private String resolution;

    @Column(name = "notes")
    private String notes;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "observed_state", columnDefinition = "jsonb")
    private String observedState;

    @Column(name = "occurred_at", nullable = false)
    @Builder.Default
    private Instant occurredAt = Instant.now();
}
