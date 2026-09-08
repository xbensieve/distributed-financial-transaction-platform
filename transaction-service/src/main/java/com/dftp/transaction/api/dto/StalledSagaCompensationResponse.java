package com.dftp.transaction.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StalledSagaCompensationResponse {
    private String transactionId;
    private String previousStatus;
    private String newStatus;
    private String resolutionStatus; // e.g., "COMPENSATION_INITIATED", "SAFE_NOOP"
    private String reason;
    private String initiatedBy;
    private Instant occurredAt;
    private String message;
}
