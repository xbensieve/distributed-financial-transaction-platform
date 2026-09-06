package com.dftp.ledger.domain.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AccountCreated {
    private UUID accountId;
    private String customerId;
    private String type;
    private String status;
    private Instant createdAt;
}
