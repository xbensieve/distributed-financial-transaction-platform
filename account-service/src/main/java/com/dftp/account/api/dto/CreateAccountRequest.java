package com.dftp.account.api.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateAccountRequest {
    
    @NotBlank(message = "transactionId is required for idempotency")
    private String transactionId;
    
    private String ownerId;

    public CreateAccountRequest(String transactionId) {
        this.transactionId = transactionId;
    }
}
