package com.dftp.transaction;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.FundsHeld;
import com.dftp.common.event.payload.FundsSettled;
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
 * §10 — Settlement Completion Semantics (P1)
 *
 * Proves that {@code LedgerTransactionPosted} does NOT automatically mean
 * {@code Transaction = COMPLETED}. The Transaction must reach LEDGER_POSTED
 * as an intermediate state and only transition to COMPLETED when
 * {@code FundsSettled} arrives (destination settlement confirmation).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TransactionCompletionGuardTest extends AbstractTransactionIntegrationTest {

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
    @DisplayName("§10: Ledger posted → Transaction = LEDGER_POSTED (NOT COMPLETED)")
    void testLedgerPostedDoesNotMeanCompleted() {
        // 1. PENDING
        Transaction tx = createPendingTransaction("tx-guard-1");

        // 2. FundsHeld → SOURCE_HELD
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-guard-1"));
        tx = transactionRepository.findByTransactionId("tx-guard-1").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("SOURCE_HELD");

        // 3. LedgerTransactionPosted → LEDGER_POSTED
        ObjectNode ledgerPayload = new ObjectMapper().createObjectNode();
        transactionService.handleLedgerTransactionPosted(
                buildEnvelope("LedgerTransactionPosted", ledgerPayload, "tx-guard-1"));

        // CRITICAL: Must be LEDGER_POSTED, NOT COMPLETED
        tx = transactionRepository.findByTransactionId("tx-guard-1").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("LEDGER_POSTED");
        assertThat(tx.getStatus()).isNotEqualTo("COMPLETED");

        // 4. Verify FundsSettlementRequested was produced (next step in Saga)
        assertThat(outboxEventRepository.findAll().stream()
                .filter(e -> "FundsSettlementRequested".equals(e.getEventType()))
                .count()).isEqualTo(1);
    }

    @Test
    @DisplayName("§10: Interrupted scenario — Ledger posted + no settlement yet → LEDGER_POSTED")
    void testInterruptedScenario_ledgerPostedButNoSettlement() {
        // Progress to LEDGER_POSTED
        Transaction tx = createPendingTransaction("tx-guard-2");
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-guard-2"));
        ObjectNode ledgerPayload = new ObjectMapper().createObjectNode();
        transactionService.handleLedgerTransactionPosted(
                buildEnvelope("LedgerTransactionPosted", ledgerPayload, "tx-guard-2"));

        // At this point, destination settlement has NOT been processed
        // Transaction must be at the repository-defined intermediate state
        tx = transactionRepository.findByTransactionId("tx-guard-2").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("LEDGER_POSTED");
        assertThat(tx.getStatus()).isNotEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("§10: Only FundsSettled transitions LEDGER_POSTED → COMPLETED")
    void testOnlyFundsSettledCompletesTransaction() {
        // Progress to LEDGER_POSTED
        Transaction tx = createPendingTransaction("tx-guard-3");
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-guard-3"));
        ObjectNode ledgerPayload = new ObjectMapper().createObjectNode();
        transactionService.handleLedgerTransactionPosted(
                buildEnvelope("LedgerTransactionPosted", ledgerPayload, "tx-guard-3"));

        tx = transactionRepository.findByTransactionId("tx-guard-3").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("LEDGER_POSTED");

        // Now deliver FundsSettled — THIS is what completes the transaction
        transactionService.handleFundsSettled(buildEnvelope("FundsSettled",
                new FundsSettled(sourceId, destId, new BigDecimal("100.00")), "tx-guard-3"));

        tx = transactionRepository.findByTransactionId("tx-guard-3").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("COMPLETED");
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
        com.dftp.transaction.api.dto.TransactionResponse res = transactionService.processTransaction(request, "corr-guard", UUID.randomUUID().toString());
        return transactionRepository.findById(res.getId()).orElseThrow();
    }

    private <T> EventEnvelope<T> buildEnvelope(String eventType, T payload, String txId) {
        return EventEnvelope.<T>builder()
                .eventId(UUID.randomUUID())
                .eventType(eventType)
                .transactionId(txId)
                .correlationId("corr-guard")
                .payload(payload)
                .build();
    }
}
