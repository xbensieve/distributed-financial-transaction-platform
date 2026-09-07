package com.dftp.ledger.api.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BulkLedgerSnapshotRequest {

    @NotNull
    @Size(min = 1, max = 100, message = "Bulk snapshot request must contain between 1 and 100 business transaction IDs")
    private List<String> businessTransactionIds;
}
