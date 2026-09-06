package com.dftp.transaction;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.FundsHeld;
import com.dftp.common.event.payload.FundsSettled;
import com.dftp.common.event.payload.HoldCompensated;
import com.dftp.common.event.payload.LedgerPostRejected;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.transaction.api.dto.CreateTransactionRequest;
import com.dftp.transaction.application.TransactionApplicationService;
import com.dftp.transaction.domain.AccountReference;
import com.dftp.transaction.domain.AccountReferenceRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
 * §11 — Stale Events After Compensation (P1)
 *
 * Verifies that stale events arriving after the Saga has entered
 * COMPENSATING or FAILED states are safely ignored. The state machine
 * guards prevent any backward or impossible state transitions.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TransactionStaleEventProtectionTest extends AbstractTransactionIntegrationTest {

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
    @DisplayName("§11: Stale FundsSettled arriving during COMPENSATING is ignored")
    void testStaleFundsSettledDuringCompensation() {
        // Progress: PENDING → SOURCE_HELD → COMPENSATING (via LedgerPostRejected)
        Transaction tx = createPendingTransaction("tx-stale-1");
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-stale-1"));
        transactionService.handleLedgerPostRejected(buildEnvelope("LedgerPostRejected",
                new LedgerPostRejected("Validation error"), "tx-stale-1"));

        tx = transactionRepository.findByTransactionId("tx-stale-1").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("COMPENSATING");

        // Stale FundsSettled arrives (from a parallel universe / old message)
        transactionService.handleFundsSettled(buildEnvelope("FundsSettled",
                new FundsSettled(sourceId, destId, new BigDecimal("100.00")), "tx-stale-1"));

        // State MUST remain COMPENSATING — NOT move to COMPLETED
        tx = transactionRepository.findByTransactionId("tx-stale-1").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("COMPENSATING");
    }

    @Test
    @DisplayName("§11: Stale FundsSettled arriving after FAILED is ignored")
    void testStaleFundsSettledAfterFailed() {
        // Progress: PENDING → SOURCE_HELD → COMPENSATING → FAILED
        Transaction tx = createPendingTransaction("tx-stale-2");
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-stale-2"));
        transactionService.handleLedgerPostRejected(buildEnvelope("LedgerPostRejected",
                new LedgerPostRejected("Validation error"), "tx-stale-2"));
        transactionService.handleHoldCompensated(buildEnvelope("HoldCompensated",
                new HoldCompensated(sourceId, new BigDecimal("100.00")), "tx-stale-2"));

        tx = transactionRepository.findByTransactionId("tx-stale-2").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("FAILED");

        // Stale FundsSettled arrives
        transactionService.handleFundsSettled(buildEnvelope("FundsSettled",
                new FundsSettled(sourceId, destId, new BigDecimal("100.00")), "tx-stale-2"));

        // FAILED must NEVER move back to COMPLETED
        tx = transactionRepository.findByTransactionId("tx-stale-2").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("FAILED");
    }

    @Test
    @DisplayName("§11: Stale FundsHeld arriving during COMPENSATING is ignored")
    void testStaleFundsHeldDuringCompensation() {
        // Progress: PENDING → SOURCE_HELD → COMPENSATING
        Transaction tx = createPendingTransaction("tx-stale-3");
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-stale-3"));
        transactionService.handleLedgerPostRejected(buildEnvelope("LedgerPostRejected",
                new LedgerPostRejected("Validation error"), "tx-stale-3"));

        tx = transactionRepository.findByTransactionId("tx-stale-3").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("COMPENSATING");

        // Stale FundsHeld arrives (shouldn't recreate a hold or change state)
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-stale-3"));

        // State stays COMPENSATING
        tx = transactionRepository.findByTransactionId("tx-stale-3").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("COMPENSATING");
    }

    @Test
    @DisplayName("§11: Stale FundsHeld arriving after FAILED is ignored")
    void testStaleFundsHeldAfterFailed() {
        // Progress to FAILED
        Transaction tx = createPendingTransaction("tx-stale-4");
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-stale-4"));
        transactionService.handleLedgerPostRejected(buildEnvelope("LedgerPostRejected",
                new LedgerPostRejected("Validation error"), "tx-stale-4"));
        transactionService.handleHoldCompensated(buildEnvelope("HoldCompensated",
                new HoldCompensated(sourceId, new BigDecimal("100.00")), "tx-stale-4"));

        tx = transactionRepository.findByTransactionId("tx-stale-4").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("FAILED");

        // Stale FundsHeld
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-stale-4"));

        tx = transactionRepository.findByTransactionId("tx-stale-4").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("FAILED");
    }

    // ──────────────────────────────────────────────────────────────

    private Transaction createPendingTransaction(String txId) {
        CreateTransactionRequest request = CreateTransactionRequest.builder()
                .transactionId(txId)
                .sourceAccountId(sourceId)
                .destinationAccountId(destId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .build();
        com.dftp.transaction.api.dto.TransactionResponse res = transactionService.processTransaction(request, "corr-stale", UUID.randomUUID().toString());
        return transactionRepository.findById(res.getId()).orElseThrow();
    }

    private <T> EventEnvelope<T> buildEnvelope(String eventType, T payload, String txId) {
        return EventEnvelope.<T>builder()
                .eventId(UUID.randomUUID())
                .eventType(eventType)
                .transactionId(txId)
                .correlationId("corr-stale")
                .payload(payload)
                .build();
    }
}
