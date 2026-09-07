package com.dftp.transaction.settlement.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BatchResponse {
    private String batchId;
    private String status;
    private int totalItems;
    private int successCount;
    private int failureCount;
    private int retryCount;
    private Instant createdAt;
    private Instant startedAt;
    private Instant completedAt;
    private String createdBy;
    private List<BatchItemResponse> items;
}
