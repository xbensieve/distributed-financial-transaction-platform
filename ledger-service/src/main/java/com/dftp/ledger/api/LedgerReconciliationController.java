package com.dftp.ledger.api;

import com.dftp.ledger.api.dto.BulkLedgerSnapshotRequest;
import com.dftp.ledger.api.dto.BulkLedgerSnapshotResponse;
import com.dftp.ledger.api.dto.CandidateLedgerTransactionDto;
import com.dftp.ledger.api.dto.LedgerSnapshotResponse;
import com.dftp.ledger.domain.LedgerTransaction;
import com.dftp.ledger.domain.LedgerTransactionRepository;
import com.dftp.ledger.domain.Posting;
import com.dftp.ledger.domain.PostingRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Read-only internal reconciliation controller for Ledger Service.
 * Exposes cross-service snapshot verification, candidate enumeration, and bulk read endpoints.
 * Accessible strictly to callers with ROLE_ADMIN or ROLE_SERVICE.
 */
@Slf4j
@RestController
@RequestMapping("/internal/reconciliation/ledger")
@RequiredArgsConstructor
public class LedgerReconciliationController {

    private final LedgerTransactionRepository ledgerTransactionRepository;
    private final PostingRepository postingRepository;

    @GetMapping("/by-transaction/{businessTransactionId}")
    @PreAuthorize("hasAnyRole('ADMIN', 'SERVICE')")
    public ResponseEntity<LedgerSnapshotResponse> getLedgerSnapshot(@PathVariable String businessTransactionId) {
        LedgerTransaction tx = ledgerTransactionRepository.findByBusinessTransactionId(businessTransactionId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "Ledger transaction not found for businessTransactionId: " + businessTransactionId));

        List<Posting> postings = postingRepository.findByLedgerTransaction_BusinessTransactionId(businessTransactionId);
        List<LedgerSnapshotResponse.PostingDto> postingDtos = postings.stream()
                .map(this::mapPostingToDto)
                .collect(Collectors.toList());

        LedgerSnapshotResponse response = LedgerSnapshotResponse.builder()
                .ledgerTransactionId(tx.getLedgerTransactionId())
                .businessTransactionId(tx.getBusinessTransactionId())
                .status(tx.getStatus())
                .createdAt(tx.getCreatedAt())
                .postings(postingDtos)
                .build();

        return ResponseEntity.ok(response);
    }

    /**
     * Candidate enumeration for reverse cross-service reconciliation (P11-REM-02).
     * Enumerates candidate ledger transactions older than cutoff to detect orphaned ledger entries.
     */
    @GetMapping("/unmatched-candidates")
    @PreAuthorize("hasAnyRole('ADMIN', 'SERVICE')")
    public ResponseEntity<List<CandidateLedgerTransactionDto>> getUnmatchedCandidates(
            @RequestParam(defaultValue = "30") long cutoffSeconds,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {

        Instant cutoff = Instant.now().minusSeconds(cutoffSeconds);
        Pageable pageable = PageRequest.of(page, Math.min(size, 100), Sort.by(Sort.Direction.ASC, "createdAt", "id"));
        Page<LedgerTransaction> candidates = ledgerTransactionRepository.findByCreatedAtBefore(cutoff, pageable);

        List<CandidateLedgerTransactionDto> dtos = candidates.getContent().stream()
                .map(tx -> CandidateLedgerTransactionDto.builder()
                        .ledgerTransactionId(tx.getLedgerTransactionId())
                        .businessTransactionId(tx.getBusinessTransactionId())
                        .status(tx.getStatus())
                        .createdAt(tx.getCreatedAt())
                        .build())
                .collect(Collectors.toList());

        return ResponseEntity.ok(dtos);
    }

    /**
     * Bulk snapshot endpoint for scalable cross-service reconciliation (P11-REM-03).
     * Bounded to <= 100 business transaction IDs per request.
     */
    @PostMapping("/bulk-snapshot")
    @PreAuthorize("hasAnyRole('ADMIN', 'SERVICE')")
    public ResponseEntity<BulkLedgerSnapshotResponse> getBulkSnapshots(
            @RequestBody @Valid BulkLedgerSnapshotRequest request) {

        List<String> ids = request.getBusinessTransactionIds() != null ? request.getBusinessTransactionIds() : List.of();
        if (ids.size() > 100) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Maximum 100 IDs permitted per bulk request");
        }

        List<LedgerTransaction> transactions = ledgerTransactionRepository.findByBusinessTransactionIdIn(ids);
        Map<String, LedgerTransaction> txMap = transactions.stream()
                .collect(Collectors.toMap(LedgerTransaction::getBusinessTransactionId, tx -> tx, (a, b) -> a));

        List<Posting> postings = postingRepository.findByLedgerTransaction_BusinessTransactionIdIn(ids);
        Map<String, List<LedgerSnapshotResponse.PostingDto>> postingsMap = postings.stream()
                .collect(Collectors.groupingBy(
                        p -> p.getLedgerTransaction() != null ? p.getLedgerTransaction().getBusinessTransactionId() : "",
                        Collectors.mapping(this::mapPostingToDto, Collectors.toList())
                ));

        List<BulkLedgerSnapshotResponse.Item> items = new ArrayList<>();
        for (String id : ids) {
            LedgerTransaction tx = txMap.get(id);
            if (tx != null) {
                LedgerSnapshotResponse snapshot = LedgerSnapshotResponse.builder()
                        .ledgerTransactionId(tx.getLedgerTransactionId())
                        .businessTransactionId(tx.getBusinessTransactionId())
                        .status(tx.getStatus())
                        .createdAt(tx.getCreatedAt())
                        .postings(postingsMap.getOrDefault(id, List.of()))
                        .build();
                items.add(BulkLedgerSnapshotResponse.Item.builder()
                        .businessTransactionId(id)
                        .status("FOUND")
                        .snapshot(snapshot)
                        .build());
            } else {
                items.add(BulkLedgerSnapshotResponse.Item.builder()
                        .businessTransactionId(id)
                        .status("NOT_FOUND")
                        .build());
            }
        }

        return ResponseEntity.ok(BulkLedgerSnapshotResponse.builder()
                .results(items)
                .build());
    }

    private LedgerSnapshotResponse.PostingDto mapPostingToDto(Posting p) {
        return LedgerSnapshotResponse.PostingDto.builder()
                .id(p.getId())
                .accountId(p.getAccountId())
                .amount(p.getAmount())
                .currency(p.getCurrency())
                .postingType(p.getPostingType().name())
                .createdAt(p.getCreatedAt())
                .build();
    }
}
