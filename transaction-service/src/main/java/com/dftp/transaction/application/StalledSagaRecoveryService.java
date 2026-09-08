package com.dftp.transaction.application;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.HoldCompensationRequested;
import com.dftp.common.model.SnapshotResult;
import com.dftp.common.observability.DftpMetrics;
import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.transaction.api.dto.StalledSagaCompensationResponse;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import com.dftp.transaction.reconciliation.client.AccountServiceClient;
import com.dftp.transaction.reconciliation.client.LedgerServiceClient;
import com.dftp.transaction.reconciliation.client.dto.AccountSnapshotDto;
import com.dftp.common.observability.TraceContextPropagator;
import com.dftp.transaction.reconciliation.client.dto.LedgerSnapshotDto;
import com.dftp.transaction.security.SecurityAuditService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Operational Recovery Service for Stalled Sagas (P12.5-COND-02).
 *
 * Implements authoritative 8-step verification path:
 * SOURCE_HELD -> operator diagnosis -> verified eligible compensation -> COMPENSATING -> FAILED
 *
 * Prevents arbitrary bypass of financial invariants:
 * - Requires verified absence of ledger transaction in ledger-service
 * - Requires verified existence of source hold in account-service
 * - Requires verified absence of terminal settlement
 * - Guaranteed idempotent: duplicate interventions are safe no-ops
 * - Produces immutable audit log entry for every manual intervention
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StalledSagaRecoveryService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final LedgerServiceClient ledgerServiceClient;
    private final OutboxEventRepository outboxEventRepository;
    private final SecurityAuditService securityAuditService;
    private final ObjectMapper objectMapper;

    @Autowired(required = false)
    private DftpMetrics dftpMetrics = new DftpMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());

    @Scheduled(cron = "${dftp.saga.stalled-recovery-cron:-}")
    @SchedulerLock(name = "StalledSagaRecoveryService_sweepStalledSagas", lockAtMostFor = "PT10M", lockAtLeastFor = "PT30S")
    public void sweepStalledSagas() {
        log.info("Executing scheduled sweep for stalled sagas...");
        Instant cutoff = Instant.now().minus(Duration.ofMinutes(15));
        org.springframework.data.domain.Page<Transaction> stalled =
                transactionRepository.findAnomalousTransactions(
                        List.of("SOURCE_HELD"), cutoff, org.springframework.data.domain.PageRequest.of(0, 50));
        if (!stalled.isEmpty()) {
            log.warn("Discovered {} potentially stalled sagas in SOURCE_HELD older than 15 minutes", stalled.getNumberOfElements());
            for (Transaction tx : stalled.getContent()) {
                dftpMetrics.recordReconciliationAnomaly("STALLED_SAGA_SOURCE_HELD");
            }
        }
    }

    @Transactional
    public StalledSagaCompensationResponse compensateStalledSaga(
            String transactionId,
            String reason,
            String principal,
            String roles,
            String clientIp,
            String correlationId) {

        if (reason == null || reason.trim().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "A valid operator justification reason is mandatory for manual compensation");
        }

        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }

        // 1. Transaction Identity Check
        Transaction tx = transactionRepository.findByTransactionId(transactionId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Transaction not found: " + transactionId));

        String currentStatus = tx.getStatus();

        // 2. State Invariant & Idempotency Checks
        if ("FAILED".equals(currentStatus)) {
            log.info("Transaction {} already in FAILED status; duplicate intervention is safe no-op.", transactionId);
            recordAudit(principal, roles, transactionId, "DUPLICATE_INVOCATION_NOOP",
                    Map.of("status", "FAILED", "reason", reason), "SUCCESS", clientIp, correlationId);
            return StalledSagaCompensationResponse.builder()
                    .transactionId(transactionId)
                    .previousStatus("FAILED")
                    .newStatus("FAILED")
                    .resolutionStatus("SAFE_NOOP")
                    .reason(reason)
                    .initiatedBy(principal)
                    .occurredAt(Instant.now())
                    .message("Transaction is already in terminal FAILED status. Duplicate compensation is a safe no-op.")
                    .build();
        }

        if ("COMPENSATING".equals(currentStatus)) {
            log.info("Transaction {} already in COMPENSATING status; compensation workflow is active.", transactionId);
            recordAudit(principal, roles, transactionId, "DUPLICATE_INVOCATION_NOOP",
                    Map.of("status", "COMPENSATING", "reason", reason), "SUCCESS", clientIp, correlationId);
            return StalledSagaCompensationResponse.builder()
                    .transactionId(transactionId)
                    .previousStatus("COMPENSATING")
                    .newStatus("COMPENSATING")
                    .resolutionStatus("SAFE_NOOP")
                    .reason(reason)
                    .initiatedBy(principal)
                    .occurredAt(Instant.now())
                    .message("Transaction is already in COMPENSATING status. Compensation workflow already active.")
                    .build();
        }

        if ("COMPLETED".equals(currentStatus)) {
            log.warn("SECURITY ALERT: Principal '{}' attempted compensation on COMPLETED transaction '{}'", principal, transactionId);
            recordAudit(principal, roles, transactionId, "REJECTED_ALREADY_COMPLETED",
                    Map.of("status", "COMPLETED", "reason", reason), "REJECTED_POLICY", clientIp, correlationId);
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Cannot compensate transaction " + transactionId + ": transaction already COMPLETED with posted ledger.");
        }

        if (!"SOURCE_HELD".equals(currentStatus)) {
            log.warn("Compensation rejected: transaction '{}' is in status '{}', expected 'SOURCE_HELD'", transactionId, currentStatus);
            recordAudit(principal, roles, transactionId, "REJECTED_INVALID_STATUS",
                    Map.of("status", currentStatus, "reason", reason), "REJECTED_POLICY", clientIp, correlationId);
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Transaction " + transactionId + " is in status '" + currentStatus + "'. Only SOURCE_HELD transactions qualify for stalled saga recovery.");
        }

        // 3. Verification of Source Hold in account-service
        SnapshotResult<List<AccountSnapshotDto.AccountOperationDto>> opsResult =
                accountServiceClient.getOperationsByTransaction(transactionId);
        if (opsResult.isUnavailable()) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Cannot verify hold status: account-service is unavailable (" + opsResult.getErrorMessage() + "). Stalled compensation requires verified hold.");
        }
        List<AccountSnapshotDto.AccountOperationDto> ops =
                (opsResult.isFound() && opsResult.getData() != null) ? opsResult.getData() : List.of();

        boolean hasHold = ops.stream().anyMatch(o -> "HOLD".equalsIgnoreCase(o.getOperationType()));
        if (!hasHold) {
            recordAudit(principal, roles, transactionId, "REJECTED_HOLD_MISSING",
                    Map.of("reason", reason, "error", "No HOLD operation found in account-service"), "REJECTED_POLICY", clientIp, correlationId);
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Safety check failed: No verified HOLD operation exists in account-service for transaction " + transactionId);
        }

        // 4. Verification of ABSENCE of Ledger Posting in ledger-service
        SnapshotResult<LedgerSnapshotDto> ledgerResult = ledgerServiceClient.getLedgerSnapshot(transactionId);
        if (ledgerResult.isFound()) {
            log.error("CRITICAL SAFETY VIOLATION PREVENTED: Principal '{}' attempted compensation on tx '{}' which HAS an active ledger posting!",
                    principal, transactionId);
            recordAudit(principal, roles, transactionId, "REJECTED_LEDGER_POSTED",
                    Map.of("reason", reason, "error", "Ledger transaction exists in ledger-service"), "REJECTED_POLICY", clientIp, correlationId);
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "UNSAFE COMPENSATION REJECTED: Ledger transaction exists in ledger-service for " + transactionId + ". Compensation would corrupt double-entry financial invariants.");
        }
        if (ledgerResult.isUnavailable()) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Cannot verify ledger status: ledger-service is unavailable (" + ledgerResult.getErrorMessage() + "). Stalled compensation requires verified absence of ledger posting.");
        }

        // 5. Verification of Absence of Terminal Settlement
        boolean hasSettle = ops.stream().anyMatch(o -> "SETTLE".equalsIgnoreCase(o.getOperationType()));
        if (hasSettle) {
            recordAudit(principal, roles, transactionId, "REJECTED_SETTLEMENT_EXISTS",
                    Map.of("reason", reason, "error", "Settlement operation already exists"), "REJECTED_POLICY", clientIp, correlationId);
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Safety check failed: Settlement operation already exists for transaction " + transactionId);
        }

        // 6. Verification of Absence of Prior Compensation
        boolean hasCompensate = ops.stream().anyMatch(o -> "COMPENSATE".equalsIgnoreCase(o.getOperationType()));
        if (hasCompensate) {
            tx.setStatus("FAILED");
            tx.setUpdatedAt(Instant.now());
            transactionRepository.save(tx);
            recordAudit(principal, roles, transactionId, "ALREADY_COMPENSATED_CONVERGED",
                    Map.of("reason", reason), "SUCCESS", clientIp, correlationId);
            return StalledSagaCompensationResponse.builder()
                    .transactionId(transactionId)
                    .previousStatus("SOURCE_HELD")
                    .newStatus("FAILED")
                    .resolutionStatus("SAFE_NOOP")
                    .reason(reason)
                    .initiatedBy(principal)
                    .occurredAt(Instant.now())
                    .message("Account hold was already compensated downstream. Transaction status converged to FAILED.")
                    .build();
        }

        // 7. Execute State Transition: SOURCE_HELD -> COMPENSATING
        tx.setStatus("COMPENSATING");
        tx.setUpdatedAt(Instant.now());
        transactionRepository.save(tx);

        HoldCompensationRequested payload = HoldCompensationRequested.builder()
                .sourceAccountId(tx.getSourceAccountId())
                .amount(tx.getAmount())
                .build();

        EventEnvelope<HoldCompensationRequested> nextEnvelope = EventEnvelope.<HoldCompensationRequested>builder()
                .eventId(UUID.randomUUID())
                .eventType("HoldCompensationRequested")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .correlationId(correlationId)
                .transactionId(tx.getTransactionId())
                .causationId("OPERATOR_COMPENSATION_" + UUID.randomUUID())
                .producerService("transaction-service")
                .payload(payload)
                .build();

        publishOutboxEvent(nextEnvelope, tx.getId().toString(), "transaction-events");
        log.info("OPERATOR_INTERVENTION: Transaction {} transitioned SOURCE_HELD -> COMPENSATING by '{}' (reason: '{}')",
                tx.getTransactionId(), principal, reason);

        // 8. Immutable Security Audit Log
        recordAudit(principal, roles, transactionId, "MANUAL_SAGA_COMPENSATION",
                Map.of(
                        "previousState", "SOURCE_HELD",
                        "newState", "COMPENSATING",
                        "reason", reason,
                        "sourceAccountId", tx.getSourceAccountId().toString(),
                        "amount", tx.getAmount(),
                        "verifiedHold", true,
                        "verifiedLedgerAbsent", true,
                        "verifiedSettlementAbsent", true
                ),
                "SUCCESS", clientIp, correlationId);

        return StalledSagaCompensationResponse.builder()
                .transactionId(transactionId)
                .previousStatus("SOURCE_HELD")
                .newStatus("COMPENSATING")
                .resolutionStatus("COMPENSATION_INITIATED")
                .reason(reason)
                .initiatedBy(principal)
                .occurredAt(Instant.now())
                .message("Stalled saga compensation successfully initiated. HoldCompensationRequested event emitted.")
                .build();
    }

    @SneakyThrows
    private void recordAudit(
            String principal,
            String roles,
            String transactionId,
            String action,
            Object details,
            String status,
            String clientIp,
            String correlationId) {
        String jsonDetails = objectMapper.writeValueAsString(details);
        securityAuditService.recordEvent(
                principal != null ? principal : "OPERATOR",
                roles,
                action,
                transactionId,
                jsonDetails,
                status,
                clientIp,
                correlationId
        );
    }

    @SneakyThrows
    private void publishOutboxEvent(EventEnvelope<?> envelope, String aggregateId, String aggregateType) {
        OutboxEvent outboxEvent = OutboxEvent.builder()
                .id(envelope.getEventId())
                .aggregateType(aggregateType)
                .aggregateId(aggregateId)
                .eventType(envelope.getEventType())
                .payload(objectMapper.writeValueAsString(envelope))
                .status("PENDING")
                .build();
        TraceContextPropagator.currentTraceMetadata().ifPresent(tm -> {
            outboxEvent.setTraceparent(tm.traceparent());
            outboxEvent.setTracestate(tm.tracestate());
        });
        outboxEventRepository.save(outboxEvent);
    }
}
