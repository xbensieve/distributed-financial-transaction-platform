package com.dftp.transaction.application;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.FundsHoldRequested;
import com.dftp.common.event.payload.FundsHeld;
import com.dftp.common.event.payload.FundsHoldRejected;
import com.dftp.common.event.payload.LedgerPostRequested;
import com.dftp.common.event.payload.LedgerPostRejected;
import com.dftp.common.event.payload.FundsSettlementRequested;
import com.dftp.common.event.payload.FundsSettled;
import com.dftp.common.event.payload.HoldCompensationRequested;
import com.dftp.common.event.payload.HoldCompensated;
import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.transaction.api.dto.CreateTransactionRequest;
import com.dftp.transaction.domain.AccountReference;
import com.dftp.transaction.domain.AccountReferenceRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import com.dftp.transaction.domain.ApiIdempotencyKey;
import com.dftp.transaction.domain.ApiIdempotencyKeyRepository;
import com.dftp.transaction.api.dto.TransactionResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import com.dftp.common.observability.DftpMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionApplicationService {

    private final TransactionRepository transactionRepository;
    private final AccountReferenceRepository accountReferenceRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ApiIdempotencyKeyRepository apiIdempotencyKeyRepository;
    private final ObjectMapper objectMapper;

    @Autowired(required = false)
    private DftpMetrics dftpMetrics = new DftpMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());

    @Transactional
    public TransactionResponse processTransaction(CreateTransactionRequest request, String correlationId, String idempotencyKey) {
        String payloadHash = computePayloadHash(request);
        Optional<ApiIdempotencyKey> existingKey = apiIdempotencyKeyRepository.findById(idempotencyKey);
        if (existingKey.isPresent()) {
            ApiIdempotencyKey key = existingKey.get();
            if (key.getRequestHash() != null && !key.getRequestHash().equals(payloadHash)) {
                log.warn("Idempotency key {} reused with different payload", idempotencyKey);
                dftpMetrics.recordApiIdempotencyCollision("/transactions");
                throw new IllegalArgumentException("Idempotency key was previously used with a different request payload");
            }
            log.info("Returning cached response for idempotencyKey: {}", idempotencyKey);
            dftpMetrics.recordApiIdempotencyHit("/transactions");
            if (key.getResponseBody() == null) {
                throw new IllegalStateException("Concurrent request processing for key: " + idempotencyKey);
            }
            try {
                return objectMapper.readValue(key.getResponseBody(), TransactionResponse.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize cached response", e);
            }
        }

        // Lock key
        ApiIdempotencyKey newKey = ApiIdempotencyKey.builder()
                .idempotencyKey(idempotencyKey)
                .requestHash(payloadHash)
                .createdAt(Instant.now())
                .build();
        try {
            apiIdempotencyKeyRepository.saveAndFlush(newKey);
        } catch (org.springframework.dao.DataIntegrityViolationException e) {
            log.warn("Concurrent duplicate request detected for idempotencyKey: {}", idempotencyKey);
            throw new IllegalStateException("Concurrent request processing for key: " + idempotencyKey);
        }

        // Business logic
        Optional<Transaction> existingTx = transactionRepository.findByTransactionId(request.getTransactionId());
        if (existingTx.isPresent()) {
            throw new IllegalArgumentException("Transaction ID already exists for a different idempotency key");
        }

        // Validate amount before Saga execution (production contract)
        if (request.getAmount() == null || request.getAmount().compareTo(java.math.BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Transaction amount must be greater than zero");
        }

        Optional<AccountReference> sourceAccountRef = accountReferenceRepository.findById(request.getSourceAccountId());
        if (sourceAccountRef.isEmpty()) {
            log.warn("Transaction rejected: sourceAccountId {} not found in local projection.", request.getSourceAccountId());
            throw new IllegalArgumentException("Source account not found or not yet available in Transaction Service projection");
        }
        
        Optional<AccountReference> destAccountRef = accountReferenceRepository.findById(request.getDestinationAccountId());
        if (destAccountRef.isEmpty()) {
            log.warn("Transaction rejected: destinationAccountId {} not found in local projection.", request.getDestinationAccountId());
            throw new IllegalArgumentException("Destination account not found or not yet available in Transaction Service projection");
        }

        String ownerId;
        org.springframework.security.core.Authentication auth =
                org.springframework.security.core.context.SecurityContextHolder.getContext().getAuthentication();
        if (auth != null) {
            if (!auth.isAuthenticated() || "anonymousUser".equals(auth.getName())) {
                throw new org.springframework.security.access.AccessDeniedException("Full authentication is required to initiate a transaction");
            }

            String caller = auth.getName();
            boolean isPrivileged = auth.getAuthorities().stream()
                    .anyMatch(a -> "ROLE_ADMIN".equals(a.getAuthority()) || "ROLE_SERVICE".equals(a.getAuthority()));

            String sourceOwner = sourceAccountRef.get().getOwnerId();
            boolean isUnassigned = "system-unassigned".equals(sourceOwner) || sourceOwner == null || sourceOwner.isBlank();

            // Enforce ownership:
            // ROLE_ADMIN / ROLE_SERVICE: Allowed per operational / service policy
            // ROLE_USER: Must match source account owner, and unassigned accounts cannot be used by ordinary users
            if (!isPrivileged) {
                if (isUnassigned || !caller.equals(sourceOwner)) {
                    log.warn("SECURITY ALERT: Principal '{}' attempted unauthorized transfer from source account '{}' (owner: '{}')",
                            caller, request.getSourceAccountId(), sourceOwner);
                    throw new org.springframework.security.access.AccessDeniedException(
                            "Caller is not authorized to initiate transactions from source account: " + request.getSourceAccountId());
                }
            }
            ownerId = caller;
        } else {
            // Direct internal / unit test invocation without ambient security context
            ownerId = sourceAccountRef.get().getOwnerId() != null ? sourceAccountRef.get().getOwnerId() : "system-unassigned";
        }

        Transaction transaction = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(request.getTransactionId())
                .ownerId(ownerId)
                .sourceAccountId(request.getSourceAccountId())
                .destinationAccountId(request.getDestinationAccountId())
                .amount(request.getAmount())
                .currency(request.getCurrency())
                .status("PENDING")
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        transaction = transactionRepository.save(transaction);
        dftpMetrics.recordTransactionStarted("TRANSFER");

        FundsHoldRequested payload = FundsHoldRequested.builder()
                .sourceAccountId(transaction.getSourceAccountId())
                .destinationAccountId(transaction.getDestinationAccountId())
                .amount(transaction.getAmount())
                .currency(transaction.getCurrency())
                .build();

        EventEnvelope<FundsHoldRequested> envelope = EventEnvelope.<FundsHoldRequested>builder()
                .eventId(UUID.randomUUID())
                .eventType("FundsHoldRequested")
                .eventVersion("1.0")
                .occurredAt(transaction.getUpdatedAt())
                .correlationId(correlationId)
                .transactionId(transaction.getTransactionId())
                .producerService("transaction-service")
                .payload(payload)
                .build();

        publishOutboxEvent(envelope, transaction.getId().toString(), "transaction-events");
        log.info("Created transaction {} and requested FundsHold", transaction.getId());
        
        TransactionResponse response = TransactionResponse.builder()
                .id(transaction.getId())
                .transactionId(transaction.getTransactionId())
                .sourceAccountId(transaction.getSourceAccountId())
                .destinationAccountId(transaction.getDestinationAccountId())
                .amount(transaction.getAmount())
                .currency(transaction.getCurrency())
                .status(transaction.getStatus())
                .createdAt(transaction.getCreatedAt())
                .updatedAt(transaction.getUpdatedAt())
                .build();
                
        try {
            newKey.setResponseStatus(201);
            newKey.setResponseBody(objectMapper.writeValueAsString(response));
            apiIdempotencyKeyRepository.save(newKey);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize response", e);
        }
        
        return response;
    }

    @Transactional
    public void handleFundsHeld(EventEnvelope<FundsHeld> envelope) {
        Transaction tx = getTransaction(envelope.getTransactionId());
        if (!"PENDING".equals(tx.getStatus())) {
            return;
        }

        tx.setStatus("SOURCE_HELD");
        transactionRepository.save(tx);

        LedgerPostRequested payload = LedgerPostRequested.builder()
                .sourceAccountId(tx.getSourceAccountId())
                .destinationAccountId(tx.getDestinationAccountId())
                .amount(tx.getAmount())
                .currency(tx.getCurrency())
                .build();

        EventEnvelope<LedgerPostRequested> nextEnvelope = buildNextEnvelope(envelope, "LedgerPostRequested", payload);
        publishOutboxEvent(nextEnvelope, tx.getId().toString(), "transaction-events");
        log.info("Transaction {} updated to SOURCE_HELD, requested LedgerPost", tx.getTransactionId());
    }

    @Transactional
    public void handleFundsHoldRejected(EventEnvelope<FundsHoldRejected> envelope) {
        Transaction tx = getTransaction(envelope.getTransactionId());
        if (!"PENDING".equals(tx.getStatus())) {
            return;
        }

        tx.setStatus("FAILED");
        transactionRepository.save(tx);
        dftpMetrics.recordTransactionFailed("TRANSFER", envelope.getPayload() != null ? envelope.getPayload().getReason() : "HOLD_REJECTED");
        java.time.Duration duration = java.time.Duration.between(tx.getCreatedAt(), Instant.now());
        dftpMetrics.recordSagaDuration("TRANSFER", "FAILED", duration);
        log.info("Transaction {} updated to FAILED due to FundsHoldRejected (duration: {}ms)", tx.getTransactionId(), duration.toMillis());
    }

    @Transactional
    public void handleLedgerTransactionPosted(EventEnvelope<?> envelope) {
        Transaction tx = getTransaction(envelope.getTransactionId());
        if (!"SOURCE_HELD".equals(tx.getStatus())) {
            return;
        }

        tx.setStatus("LEDGER_POSTED");
        transactionRepository.save(tx);

        FundsSettlementRequested payload = FundsSettlementRequested.builder()
                .sourceAccountId(tx.getSourceAccountId())
                .destinationAccountId(tx.getDestinationAccountId())
                .amount(tx.getAmount())
                .currency(tx.getCurrency())
                .build();

        EventEnvelope<FundsSettlementRequested> nextEnvelope = buildNextEnvelope(envelope, "FundsSettlementRequested", payload);
        publishOutboxEvent(nextEnvelope, tx.getId().toString(), "transaction-events");
        log.info("Transaction {} updated to LEDGER_POSTED, requested FundsSettlement", tx.getTransactionId());
    }

    @Transactional
    public void handleLedgerPostRejected(EventEnvelope<LedgerPostRejected> envelope) {
        Transaction tx = getTransaction(envelope.getTransactionId());
        if (!"SOURCE_HELD".equals(tx.getStatus())) {
            return;
        }

        tx.setStatus("COMPENSATING");
        transactionRepository.save(tx);

        HoldCompensationRequested payload = HoldCompensationRequested.builder()
                .sourceAccountId(tx.getSourceAccountId())
                .amount(tx.getAmount())
                .build();

        EventEnvelope<HoldCompensationRequested> nextEnvelope = buildNextEnvelope(envelope, "HoldCompensationRequested", payload);
        publishOutboxEvent(nextEnvelope, tx.getId().toString(), "transaction-events");
        log.info("Transaction {} updated to COMPENSATING, requested HoldCompensation due to Ledger rejection", tx.getTransactionId());
    }

    @Transactional
    public void handleFundsSettled(EventEnvelope<FundsSettled> envelope) {
        Transaction tx = getTransaction(envelope.getTransactionId());
        if (!"LEDGER_POSTED".equals(tx.getStatus())) {
            return;
        }

        tx.setStatus("COMPLETED");
        transactionRepository.save(tx);
        dftpMetrics.recordTransactionCompleted("TRANSFER");
        java.time.Duration duration = java.time.Duration.between(tx.getCreatedAt(), Instant.now());
        dftpMetrics.recordSagaDuration("TRANSFER", "SUCCESS", duration);
        log.info("Transaction {} updated to COMPLETED (duration: {}ms)", tx.getTransactionId(), duration.toMillis());
    }

    @Transactional
    public void handleHoldCompensated(EventEnvelope<HoldCompensated> envelope) {
        Transaction tx = getTransaction(envelope.getTransactionId());
        if (!"COMPENSATING".equals(tx.getStatus())) {
            return;
        }

        tx.setStatus("FAILED");
        transactionRepository.save(tx);
        dftpMetrics.recordTransactionCompensated("TRANSFER", "LEDGER_REJECTED");
        java.time.Duration duration = java.time.Duration.between(tx.getCreatedAt(), Instant.now());
        dftpMetrics.recordSagaDuration("TRANSFER", "COMPENSATED", duration);
        log.info("Transaction {} updated to FAILED after compensation (duration: {}ms)", tx.getTransactionId(), duration.toMillis());
    }

    private Transaction getTransaction(String transactionId) {
        return transactionRepository.findByTransactionId(transactionId)
                .orElseThrow(() -> new IllegalArgumentException("Transaction not found for ID: " + transactionId));
    }

    @Transactional
    public void handleAccountCreated(EventEnvelope<com.dftp.transaction.domain.event.AccountCreated> envelope) {
        com.dftp.transaction.domain.event.AccountCreated payload = envelope.getPayload();
        String ownerId = (payload.getOwnerId() != null && !payload.getOwnerId().isBlank())
                ? payload.getOwnerId()
                : "system-unassigned";
        accountReferenceRepository.save(new AccountReference(payload.getAccountId(), payload.getCreatedAt(), ownerId));
        log.info("AccountReference saved for account: {} (owner: {})", payload.getAccountId(), ownerId);
    }

    private <T> EventEnvelope<T> buildNextEnvelope(EventEnvelope<?> currentEnvelope, String nextEventType, T payload) {
        return EventEnvelope.<T>builder()
                .eventId(UUID.randomUUID())
                .eventType(nextEventType)
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .correlationId(currentEnvelope.getCorrelationId())
                .transactionId(currentEnvelope.getTransactionId())
                .causationId(currentEnvelope.getEventId().toString())
                .producerService("transaction-service")
                .payload(payload)
                .build();
    }

    private void publishOutboxEvent(EventEnvelope<?> envelope, String aggregateId, String aggregateType) {
        OutboxEvent outboxEvent = OutboxEvent.builder()
                .id(envelope.getEventId())
                .aggregateType(aggregateType)
                .aggregateId(aggregateId)
                .eventType(envelope.getEventType())
                .payload(serializeEnvelope(envelope))
                .status("PENDING")
                .build();
        outboxEventRepository.save(outboxEvent);
    }

    @SneakyThrows
    private String serializeEnvelope(EventEnvelope<?> envelope) {
        return objectMapper.writeValueAsString(envelope);
    }

    private String computePayloadHash(CreateTransactionRequest request) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            String raw = String.format("%s|%s|%s|%s|%s",
                    request.getTransactionId(),
                    request.getSourceAccountId(),
                    request.getDestinationAccountId(),
                    request.getAmount() != null ? request.getAmount().stripTrailingZeros().toPlainString() : "",
                    request.getCurrency());
            byte[] digest = md.digest(raw.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to compute payload hash", e);
        }
    }
}
