package com.dftp.account.api.dto;

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
public class BulkAccountOperationsRequest {

    @NotNull
    @Size(min = 1, max = 100, message = "Bulk operations request must contain between 1 and 100 transaction IDs")
    private List<String> transactionIds;
}
