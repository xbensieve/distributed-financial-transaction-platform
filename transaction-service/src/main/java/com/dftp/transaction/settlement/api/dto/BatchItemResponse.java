package com.dftp.transaction.settlement.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BatchItemResponse {
    private UUID id;
    private String batchId;
    private String transactionId;
    private UUID sourceAccountId;
    private UUID destinationAccountId;
    private BigDecimal amount;
    private String currency;
    private String status;
    private int retryCount;
    private String errorMessage;
    private Instant processedAt;
    private Instant createdAt;
}
