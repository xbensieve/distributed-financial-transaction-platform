package com.dftp.transaction.reconciliation.client.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
@JsonIgnoreProperties(ignoreUnknown = true)
public class LedgerSnapshotDto {
    private String ledgerTransactionId;
    private String businessTransactionId;
    private String status;
    private Instant createdAt;
    private List<PostingDto> postings;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PostingDto {
        private UUID id;
        private UUID accountId;
        private BigDecimal amount;
        private String currency;
        private String postingType; // DEBIT, CREDIT
        private Instant createdAt;
    }
}
