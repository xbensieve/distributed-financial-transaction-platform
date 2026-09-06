package com.dftp.account.api.dto;

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
public class AccountResponse {
    private UUID accountId;
    private String transactionId;
    private String status;
    private Instant createdAt;
    private BigDecimal settledBalance;
    private BigDecimal heldFunds;
    private String ownerId;
}
