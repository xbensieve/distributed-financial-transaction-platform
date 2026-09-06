package com.dftp.transaction.api;

import com.dftp.transaction.application.DltReplayService;
import com.dftp.transaction.security.SecurityAuditService;
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
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/admin")
@RequiredArgsConstructor
public class AdminController {

    private final DltReplayService dltReplayService;
    private final SecurityAuditService securityAuditService;
    private final com.dftp.transaction.security.DistributedDltCooldownService distributedDltCooldownService;
    private final com.dftp.transaction.reconciliation.ReconciliationScannerService reconciliationScannerService;

    private static final Set<String> ALLOWED_TOPICS = Set.of(
            "transaction-events",
            "account-events",
            "ledger-events"
    );

    private static final int MAX_BATCH_SIZE = 100;

    @PostMapping("/dlt/replay/{topic}")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<String> replayDlt(
            @PathVariable String topic,
            @RequestParam(name = "maxRecords", defaultValue = "50") int maxRecords,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            HttpServletRequest request) {

        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String principal = (auth != null) ? auth.getName() : "ANONYMOUS";
        String roles = (auth != null) ? auth.getAuthorities().stream()
                .map(GrantedAuthority::getAuthority)
                .collect(Collectors.joining(",")) : "";
        String clientIp = request.getRemoteAddr();

        // 1. Validate Topic Allowlist
        if (!ALLOWED_TOPICS.contains(topic)) {
            log.warn("SECURITY ALERT: Attempted DLT replay on unallowed topic '{}' by principal '{}'", topic, principal);
            securityAuditService.recordEvent(
                    principal, roles, "DLT_REPLAY", topic,
                    String.format("{\"error\": \"Topic not allowlisted\", \"requestedTopic\": \"%s\"}", topic),
                    "REJECTED_INVALID_TOPIC", clientIp, correlationId
            );
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Topic is not in the DLT allowlist: " + topic);
        }

        // 2. Validate Bounded Batch Size
        if (maxRecords <= 0 || maxRecords > MAX_BATCH_SIZE) {
            log.warn("SECURITY ALERT: Invalid batch size {} requested for topic '{}' by principal '{}'", maxRecords, topic, principal);
            securityAuditService.recordEvent(
                    principal, roles, "DLT_REPLAY", topic,
                    String.format("{\"error\": \"Invalid batch size\", \"maxRecords\": %d}", maxRecords),
                    "REJECTED_INVALID_BATCH_SIZE", clientIp, correlationId
            );
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "maxRecords must be between 1 and " + MAX_BATCH_SIZE);
        }

        // 3. Distributed Replay Abuse & Cooldown Guard (PostgreSQL-backed)
        String instanceId = System.getProperty("dftp.instance.id", "replica-" + UUID.randomUUID().toString().substring(0, 8));
        boolean slotAcquired = distributedDltCooldownService.acquireReplaySlot(topic, instanceId);
        if (!slotAcquired) {
            log.warn("SECURITY ALERT: Distributed replay cooldown active for topic '{}' (requested by '{}')", topic, principal);
            securityAuditService.recordEvent(
                    principal, roles, "DLT_REPLAY", topic,
                    String.format("{\"error\": \"Distributed cooldown active\", \"topic\": \"%s\"}", topic),
                    "REJECTED_COOLDOWN_ACTIVE", clientIp, correlationId
            );
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Replay cooldown active for topic: " + topic);
        }

        // 4. Execute Replay
        String dltTopic = topic + ".DLT";
        log.info("Authorized DLT replay started for {} -> {} with batch limit {} by principal '{}'",
                dltTopic, topic, maxRecords, principal);

        int replayedCount = dltReplayService.replayDlt(dltTopic, topic, maxRecords);

        // 5. Persistent Audit Log
        securityAuditService.recordEvent(
                principal, roles, "DLT_REPLAY", topic,
                String.format("{\"sourceTopic\": \"%s\", \"targetTopic\": \"%s\", \"replayedCount\": %d, \"maxRecords\": %d}",
                        dltTopic, topic, replayedCount, maxRecords),
                "SUCCESS", clientIp, correlationId
        );

        return ResponseEntity.ok(String.format("Successfully replayed %d messages from %s to %s",
                replayedCount, dltTopic, topic));
    }

    @GetMapping("/reconciliation/report")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<com.dftp.transaction.reconciliation.ReconciliationScannerService.ReconciliationReport> getReconciliationReport(
            @RequestParam(required = false, defaultValue = "300") long sagaThresholdSeconds,
            @RequestParam(required = false, defaultValue = "120") long outboxThresholdSeconds) {

        var report = reconciliationScannerService.scanForAnomalies(
                java.time.Duration.ofSeconds(sagaThresholdSeconds),
                java.time.Duration.ofSeconds(outboxThresholdSeconds)
        );
        return ResponseEntity.ok(report);
    }
}
