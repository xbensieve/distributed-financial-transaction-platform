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
public class LedgerTransactionPosted {
    private String ledgerTransactionId;
    private String businessTransactionId;
    private BigDecimal amount;
    private String currency;
    private Instant postedAt;
}
