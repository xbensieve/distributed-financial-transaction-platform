package com.dftp.account.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BulkAccountOperationsResponse {
    private Map<String, List<AccountSnapshotResponse.AccountOperationDto>> results;
}
