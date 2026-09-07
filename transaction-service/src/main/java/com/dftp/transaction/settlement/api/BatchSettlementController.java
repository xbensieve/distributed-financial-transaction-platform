package com.dftp.transaction.settlement.api;

import com.dftp.transaction.security.SecurityAuditService;
import com.dftp.transaction.settlement.api.dto.BatchResponse;
import com.dftp.transaction.settlement.api.dto.CreateBatchRequest;
import com.dftp.transaction.settlement.application.BatchSettlementService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/admin/batches")
@RequiredArgsConstructor
public class BatchSettlementController {

    private final BatchSettlementService batchSettlementService;
    private final SecurityAuditService securityAuditService;

    @PostMapping
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<BatchResponse> createBatch(
            @RequestBody(required = false) CreateBatchRequest request,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            HttpServletRequest httpRequest) {

        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }
        if (request == null) {
            request = new CreateBatchRequest();
        }

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String principal = (auth != null) ? auth.getName() : "ANONYMOUS";
        String roles = getRoles(auth);
        String clientIp = httpRequest.getRemoteAddr();

        BatchResponse response = batchSettlementService.createBatch(request, principal);

        securityAuditService.recordEvent(
                principal, roles, "CREATE_BATCH", response.getBatchId(),
                String.format("{\"totalItems\": %d, \"status\": \"%s\"}", response.getTotalItems(), response.getStatus()),
                "SUCCESS", clientIp, correlationId
        );

        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PostMapping("/{batchId}/execute")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<BatchResponse> executeBatch(
            @PathVariable String batchId,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            HttpServletRequest httpRequest) {

        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String principal = (auth != null) ? auth.getName() : "ANONYMOUS";
        String roles = getRoles(auth);
        String clientIp = httpRequest.getRemoteAddr();

        BatchResponse response = batchSettlementService.executeBatch(batchId);

        securityAuditService.recordEvent(
                principal, roles, "EXECUTE_BATCH", batchId,
                String.format("{\"status\": \"%s\", \"successCount\": %d, \"failureCount\": %d}",
                        response.getStatus(), response.getSuccessCount(), response.getFailureCount()),
                "SUCCESS", clientIp, correlationId
        );

        return ResponseEntity.ok(response);
    }

    @GetMapping("/{batchId}")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<BatchResponse> getBatch(@PathVariable String batchId) {
        return ResponseEntity.ok(batchSettlementService.getBatch(batchId));
    }

    @PostMapping("/{batchId}/retry")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<BatchResponse> retryBatch(
            @PathVariable String batchId,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            HttpServletRequest httpRequest) {

        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String principal = (auth != null) ? auth.getName() : "ANONYMOUS";
        String roles = getRoles(auth);
        String clientIp = httpRequest.getRemoteAddr();

        BatchResponse response = batchSettlementService.retryBatch(batchId);

        securityAuditService.recordEvent(
                principal, roles, "RETRY_BATCH", batchId,
                String.format("{\"status\": \"%s\", \"retryCount\": %d}", response.getStatus(), response.getRetryCount()),
                "SUCCESS", clientIp, correlationId
        );

        return ResponseEntity.ok(response);
    }

    @PostMapping("/recover-stalled")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<String> recoverStalled(
            @RequestParam(defaultValue = "120") long timeoutSeconds,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            HttpServletRequest httpRequest) {

        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String principal = (auth != null) ? auth.getName() : "ANONYMOUS";
        String roles = getRoles(auth);
        String clientIp = httpRequest.getRemoteAddr();

        int recovered = batchSettlementService.recoverStalledItems(Duration.ofSeconds(timeoutSeconds));

        securityAuditService.recordEvent(
                principal, roles, "RECOVER_STALLED_BATCH_ITEMS", "BATCH_SYSTEM",
                String.format("{\"recoveredItems\": %d, \"timeoutSeconds\": %d}", recovered, timeoutSeconds),
                "SUCCESS", clientIp, correlationId
        );

        return ResponseEntity.ok(String.format("Recovered %d stalled batch items", recovered));
    }

    private String getRoles(Authentication auth) {
        if (auth == null || auth.getAuthorities() == null) return "";
        return auth.getAuthorities().stream()
                .map(GrantedAuthority::getAuthority)
                .collect(Collectors.joining(","));
    }
}
