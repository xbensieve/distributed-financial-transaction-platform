package com.dftp.transaction;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.FundsHeld;
import com.dftp.common.event.payload.FundsSettled;
import com.dftp.common.event.payload.HoldCompensated;
import com.dftp.common.event.payload.LedgerPostRejected;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.transaction.api.dto.CreateTransactionRequest;
import com.dftp.transaction.application.TransactionApplicationService;
import com.dftp.transaction.domain.AccountReference;
import com.dftp.transaction.domain.AccountReferenceRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test Classification: Application Transaction Test
 *
 * Tests the Transaction Saga state machine transitions including happy path,
 * compensation path, and §5 — Compensation State Integrity (P0).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TransactionSagaIntegrationTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private TransactionApplicationService transactionService;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private AccountReferenceRepository accountReferenceRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    private UUID sourceId;
    private UUID destId;

    @BeforeEach
    void setUp() {
        outboxEventRepository.deleteAll();
        transactionRepository.deleteAll();
        accountReferenceRepository.deleteAll();

        sourceId = UUID.randomUUID();
        destId = UUID.randomUUID();

        accountReferenceRepository.save(new AccountReference(sourceId, Instant.now()));
        accountReferenceRepository.save(new AccountReference(destId, Instant.now()));
    }

    @Test
    void testSagaHappyPath() {
        // 1. Create PENDING transaction
        CreateTransactionRequest request = CreateTransactionRequest.builder()
                .transactionId("tx-1")
                .sourceAccountId(sourceId)
                .destinationAccountId(destId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .build();
        
        com.dftp.transaction.api.dto.TransactionResponse txResponse = transactionService.processTransaction(request, "corr-1", UUID.randomUUID().toString());
        Transaction tx = transactionRepository.findById(txResponse.getId()).orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("PENDING");
        assertThat(outboxEventRepository.findAll().get(0).getEventType()).isEqualTo("FundsHoldRequested");

        outboxEventRepository.deleteAll();

        // 2. Handle FundsHeld -> SOURCE_HELD
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld", new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-1"));
        tx = transactionRepository.findById(tx.getId()).orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("SOURCE_HELD");
        assertThat(outboxEventRepository.findAll().get(0).getEventType()).isEqualTo("LedgerPostRequested");

        outboxEventRepository.deleteAll();

        ObjectNode node = new ObjectMapper().createObjectNode();
        transactionService.handleLedgerTransactionPosted(buildEnvelope("LedgerTransactionPosted", node, "tx-1"));
        tx = transactionRepository.findById(tx.getId()).orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("LEDGER_POSTED");
        assertThat(outboxEventRepository.findAll().get(0).getEventType()).isEqualTo("FundsSettlementRequested");

        outboxEventRepository.deleteAll();

        // 4. Handle FundsSettled -> COMPLETED
        transactionService.handleFundsSettled(buildEnvelope("FundsSettled", new FundsSettled(sourceId, destId, new BigDecimal("100.00")), "tx-1"));
        tx = transactionRepository.findById(tx.getId()).orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("COMPLETED");
    }

    @Test
    void testSagaCompensationPath() {
        // 1. Create PENDING
        CreateTransactionRequest request = CreateTransactionRequest.builder()
                .transactionId("tx-2")
                .sourceAccountId(sourceId)
                .destinationAccountId(destId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .build();
        com.dftp.transaction.api.dto.TransactionResponse txResponse = transactionService.processTransaction(request, "corr-1", UUID.randomUUID().toString());
        Transaction tx = transactionRepository.findById(txResponse.getId()).orElseThrow();
        
        // 2. Handle FundsHeld -> SOURCE_HELD
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld", new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-2"));
        outboxEventRepository.deleteAll();

        // 3. Handle LedgerPostRejected -> COMPENSATING
        transactionService.handleLedgerPostRejected(buildEnvelope("LedgerPostRejected", new LedgerPostRejected("Invalid amount"), "tx-2"));
        tx = transactionRepository.findById(tx.getId()).orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("COMPENSATING");
        assertThat(outboxEventRepository.findAll().get(0).getEventType()).isEqualTo("HoldCompensationRequested");
        
        // 4. Handle HoldCompensated -> FAILED
        transactionService.handleHoldCompensated(buildEnvelope("HoldCompensated", new HoldCompensated(sourceId, new BigDecimal("100.00")), "tx-2"));
        tx = transactionRepository.findById(tx.getId()).orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("FAILED");
    }

    /**
     * §5 — Compensation State Integrity (P0)
     *
     * Proves the COMPLETE compensation path with assertions at each step:
     * SOURCE_HELD → LedgerPostRejected → COMPENSATING → HoldCompensationRequested
     * → HoldCompensated → FAILED
     *
     * At no point does the system expose FAILED + funds still held,
     * because FAILED only transitions from COMPENSATING upon receiving HoldCompensated.
     */
    @Test
    @DisplayName("§5: Complete compensation path with state integrity at every step")
    void testCompensationStateIntegrity() {
        // 1. PENDING
        CreateTransactionRequest request = CreateTransactionRequest.builder()
                .transactionId("tx-comp-integrity")
                .sourceAccountId(sourceId)
                .destinationAccountId(destId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .build();
        com.dftp.transaction.api.dto.TransactionResponse txResponse = transactionService.processTransaction(request, "corr-comp", UUID.randomUUID().toString());
        Transaction tx = transactionRepository.findById(txResponse.getId()).orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("PENDING");

        // 2. FundsHeld → SOURCE_HELD
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-comp-integrity"));
        tx = transactionRepository.findByTransactionId("tx-comp-integrity").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("SOURCE_HELD");
        outboxEventRepository.deleteAll();

        // 3. LedgerPostRejected → COMPENSATING
        transactionService.handleLedgerPostRejected(buildEnvelope("LedgerPostRejected",
                new LedgerPostRejected("TEST FAULT INJECTION: Invalid amount"), "tx-comp-integrity"));
        tx = transactionRepository.findByTransactionId("tx-comp-integrity").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("COMPENSATING");

        // §5 CRITICAL: HoldCompensationRequested was emitted
        assertThat(outboxEventRepository.findAll().stream()
                .filter(e -> "HoldCompensationRequested".equals(e.getEventType()))
                .count()).isEqualTo(1);

        // 4. HoldCompensated → FAILED
        transactionService.handleHoldCompensated(buildEnvelope("HoldCompensated",
                new HoldCompensated(sourceId, new BigDecimal("100.00")), "tx-comp-integrity"));
        tx = transactionRepository.findByTransactionId("tx-comp-integrity").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("FAILED");

        // §5: At NO intermediate point can Transaction be FAILED without compensation.
        // The only path to FAILED from SOURCE_HELD goes through COMPENSATING.
        // The state machine enforces:
        //   handleHoldCompensated: requires status == COMPENSATING
        //   handleLedgerPostRejected: requires status == SOURCE_HELD
        // So FAILED + funds-still-held is impossible because:
        //   - FAILED only comes from COMPENSATING (via HoldCompensated)
        //   - HoldCompensated means the Account already released the hold
    }

    private <T> EventEnvelope<T> buildEnvelope(String eventType, T payload, String txId) {
        return EventEnvelope.<T>builder()
                .eventId(UUID.randomUUID())
                .eventType(eventType)
                .transactionId(txId)
                .correlationId("corr-1")
                .payload(payload)
                .build();
    }
}

