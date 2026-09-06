package com.dftp.ledger.api;

import com.dftp.ledger.domain.LedgerTransaction;
import com.dftp.ledger.domain.LedgerTransactionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/ledger/transactions")
@RequiredArgsConstructor
public class LedgerController {

    private final LedgerTransactionRepository ledgerTransactionRepository;

    @GetMapping("/{ledgerTransactionId}")
    public ResponseEntity<LedgerTransaction> getByLedgerTransactionId(@PathVariable String ledgerTransactionId) {
        return ledgerTransactionRepository.findByLedgerTransactionId(ledgerTransactionId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/by-transaction/{businessTransactionId}")
    public ResponseEntity<LedgerTransaction> getByBusinessTransactionId(@PathVariable String businessTransactionId) {
        return ledgerTransactionRepository.findByBusinessTransactionId(businessTransactionId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
