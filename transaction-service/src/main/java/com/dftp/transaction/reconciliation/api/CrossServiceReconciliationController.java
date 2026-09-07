package com.dftp.transaction.reconciliation.api;

import com.dftp.transaction.reconciliation.application.CrossServiceReconciliationService;
import com.dftp.transaction.reconciliation.domain.ReconciliationAnomaly;
import com.dftp.transaction.reconciliation.domain.ReconciliationAnomalyHistory;
import com.dftp.transaction.reconciliation.dto.CrossServiceReconciliationReport;
import com.dftp.transaction.retention.DataRetentionPurgeService;
import com.dftp.transaction.security.SecurityAuditService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/admin/reconciliation")
@RequiredArgsConstructor
public class CrossServiceReconciliationController {

    private final CrossServiceReconciliationService reconciliationService;
    private final SecurityAuditService securityAuditService;
    private final DataRetentionPurgeService dataRetentionPurgeService;

    @PostMapping("/cross-service/run")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<CrossServiceReconciliationReport> runReconciliation(
            @RequestParam(defaultValue = "30") long graceSeconds,
            @RequestParam(defaultValue = "120") long outboxSeconds,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            HttpServletRequest request) {

        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String principal = (auth != null) ? auth.getName() : "ANONYMOUS";
        String roles = getRoles(auth);
        String clientIp = request.getRemoteAddr();

        CrossServiceReconciliationReport report = reconciliationService.runReconciliation(
                Duration.ofSeconds(graceSeconds), Duration.ofSeconds(outboxSeconds));

        securityAuditService.recordEvent(
                principal, roles, "RUN_CROSS_SERVICE_RECONCILIATION", report.getReconciliationId().toString(),
                String.format("{\"scanned\": %d, \"anomalies\": %d, \"critical\": %d}",
                        report.getTotalTransactionsScanned(), report.getTotalAnomaliesDetected(), report.getCriticalAnomaliesCount()),
                "SUCCESS", clientIp, correlationId
        );

        return ResponseEntity.ok(report);
    }

    @GetMapping("/anomalies")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<Page<ReconciliationAnomaly>> getAnomalies(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String severity,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {
        return ResponseEntity.ok(reconciliationService.getAnomalies(status, severity, page, size));
    }

    @GetMapping("/anomalies/{anomalyId}/history")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<List<ReconciliationAnomalyHistory>> getAnomalyHistory(
            @PathVariable UUID anomalyId) {
        return ResponseEntity.ok(reconciliationService.getAnomalyHistory(anomalyId));
    }

    @PostMapping("/anomalies/{anomalyId}/acknowledge")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<ReconciliationAnomaly> acknowledgeAnomaly(
            @PathVariable UUID anomalyId,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            HttpServletRequest request) {

        if (correlationId == null) correlationId = UUID.randomUUID().toString();
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String principal = (auth != null) ? auth.getName() : "ANONYMOUS";

        ReconciliationAnomaly anomaly = reconciliationService.acknowledgeAnomaly(anomalyId, principal);
        securityAuditService.recordEvent(
                principal, getRoles(auth), "ACKNOWLEDGE_ANOMALY", anomalyId.toString(),
                String.format("{\"status\": \"%s\"}", anomaly.getStatus()),
                "SUCCESS", request.getRemoteAddr(), correlationId
        );
        return ResponseEntity.ok(anomaly);
    }

    @PostMapping("/anomalies/{anomalyId}/investigate")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<ReconciliationAnomaly> investigateAnomaly(
            @PathVariable UUID anomalyId,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            HttpServletRequest request) {

        if (correlationId == null) correlationId = UUID.randomUUID().toString();
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String principal = (auth != null) ? auth.getName() : "ANONYMOUS";

        ReconciliationAnomaly anomaly = reconciliationService.investigateAnomaly(anomalyId, principal);
        securityAuditService.recordEvent(
                principal, getRoles(auth), "INVESTIGATE_ANOMALY", anomalyId.toString(),
                String.format("{\"status\": \"%s\"}", anomaly.getStatus()),
                "SUCCESS", request.getRemoteAddr(), correlationId
        );
        return ResponseEntity.ok(anomaly);
    }

    @PostMapping("/anomalies/{anomalyId}/resolve")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<ReconciliationAnomaly> resolveAnomaly(
            @PathVariable UUID anomalyId,
            @RequestParam(required = false) String resolution,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            HttpServletRequest request) {

        if (correlationId == null) correlationId = UUID.randomUUID().toString();
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String principal = (auth != null) ? auth.getName() : "ANONYMOUS";

        ReconciliationAnomaly anomaly = reconciliationService.resolveAnomaly(anomalyId, resolution, principal);
        securityAuditService.recordEvent(
                principal, getRoles(auth), "RESOLVE_ANOMALY", anomalyId.toString(),
                String.format("{\"resolution\": \"%s\"}", anomaly.getResolution()),
                "SUCCESS", request.getRemoteAddr(), correlationId
        );
        return ResponseEntity.ok(anomaly);
    }

    @PostMapping("/anomalies/{anomalyId}/suppress")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<ReconciliationAnomaly> suppressAnomaly(
            @PathVariable UUID anomalyId,
            @RequestParam String reason,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            HttpServletRequest request) {

        if (correlationId == null) correlationId = UUID.randomUUID().toString();
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String principal = (auth != null) ? auth.getName() : "ANONYMOUS";

        ReconciliationAnomaly anomaly = reconciliationService.suppressAnomaly(anomalyId, reason, principal);
        securityAuditService.recordEvent(
                principal, getRoles(auth), "SUPPRESS_ANOMALY", anomalyId.toString(),
                String.format("{\"reason\": \"%s\"}", reason),
                "SUCCESS", request.getRemoteAddr(), correlationId
        );
        return ResponseEntity.ok(anomaly);
    }

    @PostMapping("/anomalies/{anomalyId}/remediate")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<String> remediateAnomaly(
            @PathVariable UUID anomalyId,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            HttpServletRequest request) {

        if (correlationId == null) correlationId = UUID.randomUUID().toString();
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String principal = (auth != null) ? auth.getName() : "ANONYMOUS";

        String result = reconciliationService.remediateAnomaly(anomalyId, principal);
        securityAuditService.recordEvent(
                principal, getRoles(auth), "REMEDIATE_ANOMALY", anomalyId.toString(),
                String.format("{\"result\": \"%s\"}", result),
                result.startsWith("SUCCESS") ? "SUCCESS" : "REJECTED_POLICY", request.getRemoteAddr(), correlationId
        );
        return ResponseEntity.ok(result);
    }

    /**
     * Administrative trigger for scheduled data retention purge (P11-REM-05).
     */
    @PostMapping("/retention/purge")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<Map<String, Object>> triggerDataRetentionPurge(
            @RequestParam(defaultValue = "90") int settlementDays,
            @RequestParam(defaultValue = "365") int auditLogDays,
            @RequestParam(defaultValue = "500") int chunkSize,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            HttpServletRequest request) {

        if (correlationId == null) correlationId = UUID.randomUUID().toString();
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String principal = (auth != null) ? auth.getName() : "ANONYMOUS";

        int purgedItems = dataRetentionPurgeService.purgeSettlementItems(Duration.ofDays(settlementDays), chunkSize);
        int purgedBatches = dataRetentionPurgeService.purgeSettlementBatches(Duration.ofDays(settlementDays), chunkSize);
        int purgedLogs = dataRetentionPurgeService.purgeSecurityAuditLogs(Duration.ofDays(auditLogDays), chunkSize);

        securityAuditService.recordEvent(
                principal, getRoles(auth), "TRIGGER_RETENTION_PURGE", "DATA_RETENTION",
                String.format("{\"purgedItems\": %d, \"purgedBatches\": %d, \"purgedLogs\": %d}",
                        purgedItems, purgedBatches, purgedLogs),
                "SUCCESS", request.getRemoteAddr(), correlationId
        );

        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "purgedSettlementItems", purgedItems,
                "purgedSettlementBatches", purgedBatches,
                "purgedSecurityAuditLogs", purgedLogs
        ));
    }

    private String getRoles(Authentication auth) {
        if (auth == null || auth.getAuthorities() == null) return "";
        return auth.getAuthorities().stream()
                .map(GrantedAuthority::getAuthority)
                .collect(Collectors.joining(","));
    }
}
