package com.dftp.account.api;

import com.dftp.account.api.dto.AccountSnapshotResponse;
import com.dftp.account.api.dto.BulkAccountOperationsRequest;
import com.dftp.account.api.dto.BulkAccountOperationsResponse;
import com.dftp.account.api.dto.CandidateHoldDto;
import com.dftp.account.domain.Account;
import com.dftp.account.domain.AccountOperation;
import com.dftp.account.domain.AccountOperationRepository;
import com.dftp.account.domain.AccountRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Read-only internal reconciliation controller.
 * Exposes cross-service snapshot verification, candidate enumeration, and bulk read endpoints for Account Service.
 * Accessible strictly to callers with ROLE_ADMIN or ROLE_SERVICE.
 */
@Slf4j
@RestController
@RequestMapping("/internal/reconciliation")
@RequiredArgsConstructor
public class AccountReconciliationController {

    private final AccountRepository accountRepository;
    private final AccountOperationRepository accountOperationRepository;

    @GetMapping("/accounts/{accountId}/snapshot")
    @PreAuthorize("hasAnyRole('ADMIN', 'SERVICE')")
    public ResponseEntity<AccountSnapshotResponse> getAccountSnapshot(@PathVariable UUID accountId) {
        Account account = accountRepository.findById(accountId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Account not found: " + accountId));

        List<AccountOperation> operations = accountOperationRepository.findByAccountId(accountId);
        List<AccountSnapshotResponse.AccountOperationDto> operationDtos = operations.stream()
                .map(this::mapToDto)
                .collect(Collectors.toList());

        AccountSnapshotResponse response = AccountSnapshotResponse.builder()
                .accountId(account.getId())
                .transactionId(account.getTransactionId())
                .status(account.getStatus())
                .createdAt(account.getCreatedAt())
                .settledBalance(account.getSettledBalance())
                .heldFunds(account.getHeldFunds())
                .ownerId(account.getOwnerId())
                .operations(operationDtos)
                .build();

        return ResponseEntity.ok(response);
    }

    @GetMapping("/operations/by-transaction/{transactionId}")
    @PreAuthorize("hasAnyRole('ADMIN', 'SERVICE')")
    public ResponseEntity<List<AccountSnapshotResponse.AccountOperationDto>> getOperationsByTransaction(
            @PathVariable String transactionId) {
        List<AccountOperation> operations = accountOperationRepository.findByTransactionId(transactionId);
        List<AccountSnapshotResponse.AccountOperationDto> dtos = operations.stream()
                .map(this::mapToDto)
                .collect(Collectors.toList());
        return ResponseEntity.ok(dtos);
    }

    /**
     * Candidate enumeration for reverse cross-service reconciliation (P11-REM-02).
     * Discovers account holds older than cutoff that lack SETTLE or COMPENSATE operations.
     */
    @GetMapping("/accounts/orphan-hold-candidates")
    @PreAuthorize("hasAnyRole('ADMIN', 'SERVICE')")
    public ResponseEntity<List<CandidateHoldDto>> getOrphanHoldCandidates(
            @RequestParam(defaultValue = "30") long cutoffSeconds,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {

        Instant cutoff = Instant.now().minusSeconds(cutoffSeconds);
        Pageable pageable = PageRequest.of(page, Math.min(size, 100));
        List<AccountOperation> candidates = accountOperationRepository.findOrphanHoldCandidates(cutoff, pageable);

        List<CandidateHoldDto> dtos = candidates.stream()
                .map(op -> CandidateHoldDto.builder()
                        .transactionId(op.getTransactionId())
                        .accountId(op.getAccountId())
                        .createdAt(op.getCreatedAt())
                        .build())
                .collect(Collectors.toList());

        return ResponseEntity.ok(dtos);
    }

    /**
     * Bulk operations endpoint for scalable cross-service reconciliation (P11-REM-03).
     * Bounded to <= 100 transaction IDs per request.
     */
    @PostMapping("/accounts/bulk-operations")
    @PreAuthorize("hasAnyRole('ADMIN', 'SERVICE')")
    public ResponseEntity<BulkAccountOperationsResponse> getBulkOperations(
            @RequestBody @Valid BulkAccountOperationsRequest request) {

        List<String> ids = request.getTransactionIds() != null ? request.getTransactionIds() : List.of();
        if (ids.size() > 100) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Maximum 100 IDs permitted per bulk request");
        }

        List<AccountOperation> ops = accountOperationRepository.findByTransactionIdIn(ids);
        Map<String, List<AccountSnapshotResponse.AccountOperationDto>> grouped = ops.stream()
                .map(this::mapToDto)
                .collect(Collectors.groupingBy(AccountSnapshotResponse.AccountOperationDto::getTransactionId));

        Map<String, List<AccountSnapshotResponse.AccountOperationDto>> results = new HashMap<>();
        for (String id : ids) {
            results.put(id, grouped.getOrDefault(id, Collections.emptyList()));
        }

        return ResponseEntity.ok(BulkAccountOperationsResponse.builder()
                .results(results)
                .build());
    }

    private AccountSnapshotResponse.AccountOperationDto mapToDto(AccountOperation op) {
        return AccountSnapshotResponse.AccountOperationDto.builder()
                .id(op.getId())
                .accountId(op.getAccountId())
                .transactionId(op.getTransactionId())
                .operationType(op.getOperationType())
                .createdAt(op.getCreatedAt())
                .build();
    }
}
