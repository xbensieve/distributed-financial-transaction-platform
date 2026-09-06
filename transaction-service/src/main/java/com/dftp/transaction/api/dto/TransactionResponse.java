package com.dftp.transaction.api.dto;

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
public class TransactionResponse {
    private UUID id;
    private String transactionId;
    private UUID sourceAccountId;
    private UUID destinationAccountId;
    private BigDecimal amount;
    private String currency;
    private String status;
    private Instant createdAt;
    private Instant updatedAt;
    private String ownerId;
}
