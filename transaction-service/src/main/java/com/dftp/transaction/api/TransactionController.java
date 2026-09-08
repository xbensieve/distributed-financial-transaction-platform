package com.dftp.transaction.api;

import com.dftp.transaction.api.dto.CreateTransactionRequest;
import com.dftp.transaction.api.dto.TransactionResponse;
import com.dftp.transaction.application.TransactionApplicationService;
import com.dftp.transaction.domain.AccountReferenceRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/transactions")
@RequiredArgsConstructor
public class TransactionController {

    private final TransactionApplicationService transactionApplicationService;
    private final TransactionRepository transactionRepository;
    private final AccountReferenceRepository accountReferenceRepository;

    @PostMapping
    public ResponseEntity<TransactionResponse> createTransaction(
            @RequestHeader(value = "Idempotency-Key", required = true) String idempotencyKey,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            @Valid @RequestBody CreateTransactionRequest request) {

        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }

        log.info("Received request to create transaction with transactionId: {}, correlationId: {}, idempotencyKey: {}",
                request.getTransactionId(), correlationId, idempotencyKey);

        try {
            TransactionResponse response = transactionApplicationService.processTransaction(request, correlationId, idempotencyKey);
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } catch (IllegalArgumentException e) {
            log.warn("Validation failed for transaction {}: {}", request.getTransactionId(), e.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        } catch (IllegalStateException e) {
            log.warn("Concurrent request or idempotency state issue for key {}: {}", idempotencyKey, e.getMessage());
            throw new ResponseStatusException(HttpStatus.CONFLICT, e.getMessage(), e);
        } catch (org.springframework.security.access.AccessDeniedException e) {
            log.warn("Access denied for transaction {}: {}", request.getTransactionId(), e.getMessage());
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, e.getMessage(), e);
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<TransactionResponse> getTransaction(@PathVariable UUID id) {
        return transactionRepository.findById(id)
                .map(this::toResponse)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Transaction not found"));
    }

    @GetMapping("/by-transaction-id/{txId}")
    public ResponseEntity<TransactionResponse> getTransactionByTransactionId(@PathVariable String txId) {
        return transactionRepository.findByTransactionId(txId)
                .map(this::toResponse)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Transaction not found"));
    }

    private ResponseEntity<TransactionResponse> toResponse(Transaction transaction) {
        verifyTransactionOwnership(transaction);
        return ResponseEntity.ok(TransactionResponse.builder()
                .id(transaction.getId())
                .transactionId(transaction.getTransactionId())
                .sourceAccountId(transaction.getSourceAccountId())
                .destinationAccountId(transaction.getDestinationAccountId())
                .amount(transaction.getAmount())
                .currency(transaction.getCurrency())
                .status(transaction.getStatus())
                .createdAt(transaction.getCreatedAt())
                .updatedAt(transaction.getUpdatedAt())
                .ownerId(transaction.getOwnerId())
                .build());
    }

    private void verifyTransactionOwnership(Transaction transaction) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Transaction not found");
        }
        boolean isPrivileged = auth.getAuthorities().stream()
                .anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN") || a.getAuthority().equals("ROLE_SERVICE"));
        if (isPrivileged) {
            return;
        }
        String principal = auth.getName();
        boolean isTxUnassigned = "system-unassigned".equals(transaction.getOwnerId()) || transaction.getOwnerId() == null;
        // 1. Direct initiating owner (never match unassigned resources for ordinary users)
        if (!isTxUnassigned && principal.equals(transaction.getOwnerId())) {
            return;
        }
        // 2. Counterparty account owners
        boolean isSourceOwner = accountReferenceRepository.findById(transaction.getSourceAccountId())
                .map(ref -> !"system-unassigned".equals(ref.getOwnerId()) && principal.equals(ref.getOwnerId()))
                .orElse(false);
        if (isSourceOwner) {
            return;
        }
        boolean isDestOwner = accountReferenceRepository.findById(transaction.getDestinationAccountId())
                .map(ref -> !"system-unassigned".equals(ref.getOwnerId()) && principal.equals(ref.getOwnerId()))
                .orElse(false);
        if (isDestOwner) {
            return;
        }

        log.warn("SECURITY ALERT: Principal '{}' attempted unauthorized access to transaction '{}' (owner: '{}')",
                principal, transaction.getTransactionId(), transaction.getOwnerId());
        throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Transaction not found");
    }

    @ExceptionHandler(com.dftp.transaction.backpressure.OutboxAdmissionThrottledException.class)
    public ResponseEntity<com.dftp.common.error.ApiError> handleAdmissionThrottled(
            com.dftp.transaction.backpressure.OutboxAdmissionThrottledException e,
            jakarta.servlet.http.HttpServletRequest request) {
        log.warn("Transaction admission throttled for request {}: {}", request.getRequestURI(), e.getMessage());
        com.dftp.common.error.ApiError apiError = com.dftp.common.error.ApiError.builder()
                .errorCode("OUTBOX_ADMISSION_THROTTLED")
                .message(e.getMessage())
                .path(request.getRequestURI())
                .build();
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(org.springframework.http.HttpHeaders.RETRY_AFTER, String.valueOf(e.getRetryAfterSeconds()))
                .body(apiError);
    }
}
