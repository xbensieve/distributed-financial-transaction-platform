package com.dftp.ledger.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CandidateLedgerTransactionDto {
    private String ledgerTransactionId;
    private String businessTransactionId;
    private String status;
    private Instant createdAt;
}
