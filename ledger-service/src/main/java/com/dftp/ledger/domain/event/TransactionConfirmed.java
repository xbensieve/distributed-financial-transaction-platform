package com.dftp.ledger.domain.event;

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
public class TransactionConfirmed {
    private UUID transactionId;
    private UUID accountId;
    private BigDecimal amount;
    private String currency;
    private String status;
    private Instant confirmedAt;
}
