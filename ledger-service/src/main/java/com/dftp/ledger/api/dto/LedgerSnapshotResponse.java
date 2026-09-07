package com.dftp.ledger.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LedgerSnapshotResponse {
    private String ledgerTransactionId;
    private String businessTransactionId;
    private String status;
    private Instant createdAt;
    private List<PostingDto> postings;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PostingDto {
        private UUID id;
        private UUID accountId;
        private BigDecimal amount;
        private String currency;
        private String postingType;
        private Instant createdAt;
    }
}
