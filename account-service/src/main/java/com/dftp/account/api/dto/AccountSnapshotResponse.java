package com.dftp.account.api.dto;

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
public class AccountSnapshotResponse {
    private UUID accountId;
    private String transactionId;
    private String status;
    private Instant createdAt;
    private BigDecimal settledBalance;
    private BigDecimal heldFunds;
    private String ownerId;
    private List<AccountOperationDto> operations;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AccountOperationDto {
        private UUID id;
        private UUID accountId;
        private String transactionId;
        private String operationType;
        private Instant createdAt;
    }
}
