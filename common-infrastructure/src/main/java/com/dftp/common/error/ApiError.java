package com.dftp.common.error;

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
public class ApiError {
    private String errorCode;
    private String message;
    private String path;
    
    @Builder.Default
    private Instant timestamp = Instant.now();
    
    private String correlationId;
    private List<ValidationError> validationErrors;

    @Data
    @AllArgsConstructor
    public static class ValidationError {
        private String field;
        private String error;
    }
}
