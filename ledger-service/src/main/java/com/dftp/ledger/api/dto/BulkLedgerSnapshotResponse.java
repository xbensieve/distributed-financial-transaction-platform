package com.dftp.ledger.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BulkLedgerSnapshotResponse {

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Item {
        private String businessTransactionId;
        private String status; // "FOUND" or "NOT_FOUND"
        private LedgerSnapshotResponse snapshot;
    }

    private List<Item> results;
}
