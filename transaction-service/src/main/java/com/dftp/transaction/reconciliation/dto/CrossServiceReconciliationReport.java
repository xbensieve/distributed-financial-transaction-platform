package com.dftp.transaction.reconciliation.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CrossServiceReconciliationReport {
    private UUID reconciliationId;
    private Instant scannedAt;
    private int totalTransactionsScanned;
    private int totalAnomaliesDetected;
    private int criticalAnomaliesCount;
    private int highAnomaliesCount;
    private int mediumAnomaliesCount;
    private int lowAnomaliesCount;
    private List<ReconciliationAnomalyDto> anomalies;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReconciliationAnomalyDto {
        private UUID id;
        private UUID reconciliationId;
        private String businessTransactionId;
        private UUID accountId;
        private String ledgerTransactionId;
        private String anomalyType;
        private String severity;
        private String status;
        private String observedState;
        private Instant detectedAt;
        private Instant lastCheckedAt;
        private String resolution;
        private Instant resolvedAt;
        private String resolvedBy;
    }
}
