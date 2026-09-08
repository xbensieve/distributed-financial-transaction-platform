package com.dftp.account.application;

import com.dftp.account.api.dto.CreateAccountRequest;
import com.dftp.account.domain.Account;
import com.dftp.account.domain.AccountOperation;
import com.dftp.account.domain.AccountOperationRepository;
import com.dftp.account.domain.AccountRepository;
import com.dftp.account.domain.event.AccountCreated;
import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.FundsHeld;
import com.dftp.common.event.payload.FundsHoldRejected;
import com.dftp.common.event.payload.FundsSettled;
import com.dftp.common.event.payload.HoldCompensated;
import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import com.dftp.common.observability.DftpMetrics;
import com.dftp.common.observability.TraceContextPropagator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountApplicationService {

    private final AccountRepository accountRepository;
    private final AccountOperationRepository accountOperationRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Autowired(required = false)
    private DftpMetrics dftpMetrics = new DftpMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());

    @Transactional
    public Account createAccount(CreateAccountRequest request, String correlationId) {
        Optional<Account> existingAccount = accountRepository.findByTransactionId(request.getTransactionId());
        if (existingAccount.isPresent()) {
            return existingAccount.get();
        }

        org.springframework.security.core.Authentication auth =
                org.springframework.security.core.context.SecurityContextHolder.getContext().getAuthentication();
        boolean isAdmin = auth != null && auth.getAuthorities().stream()
                .anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN"));

        String ownerId;
        if (isAdmin && request.getOwnerId() != null && !request.getOwnerId().trim().isEmpty()) {
            // ADMIN can explicitly assign account owner
            ownerId = request.getOwnerId().trim();
        } else if (auth != null && auth.isAuthenticated() && !"anonymousUser".equals(auth.getName())) {
            // ROLE_USER: ownerId is derived strictly from authenticated principal, client-supplied ownerId is ignored
            ownerId = auth.getName();
        } else {
            ownerId = "system-unassigned";
        }

        Account account = Account.builder()
                .id(UUID.randomUUID())
                .transactionId(request.getTransactionId())
                .ownerId(ownerId)
                .status("ACTIVE")
                .settledBalance(BigDecimal.ZERO)
                .heldFunds(BigDecimal.ZERO)
                .createdAt(Instant.now())
                .build();
        
        account = accountRepository.save(account);

        AccountCreated payload = AccountCreated.builder()
                .accountId(account.getId())
                .createdAt(account.getCreatedAt())
                .ownerId(account.getOwnerId())
                .build();

        EventEnvelope<AccountCreated> envelope = EventEnvelope.<AccountCreated>builder()
                .eventId(UUID.randomUUID())
                .eventType("AccountCreated")
                .eventVersion("1.0")
                .occurredAt(account.getCreatedAt())
                .correlationId(correlationId)
                .transactionId(account.getTransactionId())
                .producerService("account-service")
                .payload(payload)
                .build();

        publishOutboxEvent(envelope, account.getId().toString(), "account-events");
        return account;
    }

    // Helper for testing
    @Transactional
    public void creditAccount(UUID accountId, BigDecimal amount) {
        Account account = accountRepository.findById(accountId)
                .orElseThrow(() -> new IllegalArgumentException("Account not found"));
        account.setSettledBalance(account.getSettledBalance().add(amount));
        accountRepository.save(account);
    }

    @Transactional
    public void holdFunds(UUID accountId, BigDecimal amount, String transactionId, String correlationId, String causationId) {
        // Business idempotency
        if (checkOperationExists(transactionId, "HOLD", accountId)) {
            log.info("Hold operation already processed for transactionId: {}", transactionId);
            return; // Idempotent success
        }

        Account account = accountRepository.findById(accountId)
                .orElseThrow(() -> new IllegalArgumentException("Account not found: " + accountId));

        BigDecimal availableBalance = account.getSettledBalance().subtract(account.getHeldFunds());
        
        if (availableBalance.compareTo(amount) >= 0) {
            // Success
            account.setHeldFunds(account.getHeldFunds().add(amount));
            accountRepository.save(account);
            saveOperation(transactionId, "HOLD", accountId);

            FundsHeld payload = FundsHeld.builder()
                    .sourceAccountId(accountId)
                    .amount(amount)
                    .currency("USD") // Simplification for Phase 06
                    .build();

            EventEnvelope<FundsHeld> envelope = buildEnvelope("FundsHeld", payload, transactionId, correlationId, causationId);
            publishOutboxEvent(envelope, accountId.toString(), "account-events");
            dftpMetrics.recordAccountHoldSuccess();
            log.info("Held {} funds for account {}", amount, accountId);
        } else {
            // Rejected
            FundsHoldRejected payload = FundsHoldRejected.builder()
                    .sourceAccountId(accountId)
                    .amount(amount)
                    .reason("Insufficient available funds")
                    .build();
            
            // Wait: A rejection does not mutate the DB (other than operations table), but we should save idempotency.
            saveOperation(transactionId, "HOLD", accountId);

            EventEnvelope<FundsHoldRejected> envelope = buildEnvelope("FundsHoldRejected", payload, transactionId, correlationId, causationId);
            publishOutboxEvent(envelope, accountId.toString(), "account-events");
            dftpMetrics.recordAccountHoldRejected("insufficient_funds");
            log.warn("Hold rejected for account {}: insufficient funds", accountId);
        }
    }

    @Transactional
    public void settleFunds(UUID sourceAccountId, UUID destAccountId, BigDecimal amount, String transactionId, String correlationId, String causationId) {
        if (checkOperationExists(transactionId, "SETTLE", sourceAccountId)) {
            log.info("Settle operation already processed for transactionId: {}", transactionId);
            return;
        }

        Account source = accountRepository.findById(sourceAccountId)
                .orElseThrow(() -> new IllegalArgumentException("Source account not found: " + sourceAccountId));
        Account dest = accountRepository.findById(destAccountId)
                .orElseThrow(() -> new IllegalArgumentException("Dest account not found: " + destAccountId));

        // Deduct from hold and settled
        source.setHeldFunds(source.getHeldFunds().subtract(amount));
        source.setSettledBalance(source.getSettledBalance().subtract(amount));
        
        // Add to settled
        dest.setSettledBalance(dest.getSettledBalance().add(amount));

        accountRepository.save(source);
        accountRepository.save(dest);

        saveOperation(transactionId, "SETTLE", sourceAccountId);
        saveOperation(transactionId, "SETTLE", destAccountId);

        FundsSettled payload = FundsSettled.builder()
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(amount)
                .build();

        EventEnvelope<FundsSettled> envelope = buildEnvelope("FundsSettled", payload, transactionId, correlationId, causationId);
        publishOutboxEvent(envelope, sourceAccountId.toString(), "account-events");
        dftpMetrics.recordAccountSettlementSuccess();
        log.info("Settled {} funds from {} to {}", amount, sourceAccountId, destAccountId);
    }

    @Transactional
    public void compensateHold(UUID accountId, BigDecimal amount, String transactionId, String correlationId, String causationId) {
        if (checkOperationExists(transactionId, "COMPENSATE", accountId)) {
            log.info("Compensate operation already processed for transactionId: {}", transactionId);
            return;
        }

        Account account = accountRepository.findById(accountId)
                .orElseThrow(() -> new IllegalArgumentException("Account not found: " + accountId));

        account.setHeldFunds(account.getHeldFunds().subtract(amount));
        accountRepository.save(account);

        saveOperation(transactionId, "COMPENSATE", accountId);

        HoldCompensated payload = HoldCompensated.builder()
                .sourceAccountId(accountId)
                .amount(amount)
                .build();

        EventEnvelope<HoldCompensated> envelope = buildEnvelope("HoldCompensated", payload, transactionId, correlationId, causationId);
        publishOutboxEvent(envelope, accountId.toString(), "account-events");
        dftpMetrics.recordAccountCompensationSuccess();
        log.info("Compensated (released) {} held funds for account {}", amount, accountId);
    }

    private boolean checkOperationExists(String transactionId, String operationType, UUID accountId) {
        return accountOperationRepository.findByTransactionIdAndOperationTypeAndAccountId(transactionId, operationType, accountId).isPresent();
    }

    private void saveOperation(String transactionId, String operationType, UUID accountId) {
        int rowsAffected = accountOperationRepository.insertIfAbsent(UUID.randomUUID(), accountId, transactionId, operationType);
        if (rowsAffected == 0) {
            log.info("Duplicate operation detected for {} on account {}", operationType, accountId);
            throw new DataIntegrityViolationException("Duplicate operation detected for " + operationType + " on account " + accountId);
        }
    }

    private <T> EventEnvelope<T> buildEnvelope(String eventType, T payload, String transactionId, String correlationId, String causationId) {
        return EventEnvelope.<T>builder()
                .eventId(UUID.randomUUID())
                .eventType(eventType)
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .correlationId(correlationId)
                .transactionId(transactionId)
                .causationId(causationId)
                .producerService("account-service")
                .payload(payload)
                .build();
    }

    private void publishOutboxEvent(EventEnvelope<?> envelope, String aggregateId, String aggregateType) {
        var traceMetadata = TraceContextPropagator.currentTraceMetadata().orElse(null);
        OutboxEvent outboxEvent = OutboxEvent.builder()
                .id(envelope.getEventId())
                .aggregateType(aggregateType)
                .aggregateId(aggregateId)
                .eventType(envelope.getEventType())
                .payload(serializeEnvelope(envelope))
                .status("PENDING")
                .traceparent(traceMetadata != null ? traceMetadata.traceparent() : null)
                .tracestate(traceMetadata != null ? traceMetadata.tracestate() : null)
                .build();
        outboxEventRepository.save(outboxEvent);
    }

    @SneakyThrows
    private String serializeEnvelope(EventEnvelope<?> envelope) {
        return objectMapper.writeValueAsString(envelope);
    }
}
