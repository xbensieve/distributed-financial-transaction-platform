package com.dftp.transaction;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.FundsHeld;
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
 * §3 — Crash After Successful Hold (P0)
 * §4 — Crash After Ledger Posting (P0)
 *
 * These tests prove that the Transaction Saga orchestrator can safely recover
 * from crashes by replaying events. The state machine guards in each handler
 * ensure idempotent transitions.
 *
 * <p>Crash simulation: We simulate the "crash before processing" scenario by
 * delivering the event to the application service after the state has already
 * been set to the target state (proving re-delivery is safe), and also by
 * delivering when the state is still at the pre-crash state (proving recovery
 * actually works).</p>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TransactionCrashRecoveryTest extends AbstractTransactionIntegrationTest {

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

    // ──────────────────────────────────────────────────────────────
    // §3 — Crash After Successful Hold
    // ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("§3: Crash after hold — replay FundsHeld recovers PENDING → SOURCE_HELD")
    void testCrashAfterHold_replayFundsHeld_recoversToPENDING() {
        // 1. Create transaction (PENDING)
        Transaction tx = createPendingTransaction("tx-crash-hold-1");
        assertThat(tx.getStatus()).isEqualTo("PENDING");
        outboxEventRepository.deleteAll();

        /*
         * CRASH SIMULATION:
         * The account successfully held funds and produced FundsHeld.
         * The Transaction Service crashes BEFORE processing FundsHeld.
         *
         * At this point:
         *   Transaction = PENDING (persisted before crash)
         *   Account = HELD (persisted by Account Service)
         *
         * This is a RECOVERABLE INTERMEDIATE STATE.
         * Recovery: Kafka redelivers FundsHeld after restart.
         */

        // 2. After restart, replay FundsHeld
        EventEnvelope<FundsHeld> fundsHeld = buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-crash-hold-1");
        transactionService.handleFundsHeld(fundsHeld);

        // 3. Verify recovery
        tx = transactionRepository.findByTransactionId("tx-crash-hold-1").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("SOURCE_HELD");

        // Verify exactly one LedgerPostRequested was produced
        assertThat(outboxEventRepository.findAll().stream()
                .filter(e -> "LedgerPostRequested".equals(e.getEventType()))
                .count()).isEqualTo(1);
    }

    @Test
    @DisplayName("§3: Replay FundsHeld when already SOURCE_HELD is idempotent")
    void testReplayFundsHeld_alreadySourceHeld_noDoubleTransition() {
        // 1. Create transaction and process FundsHeld normally
        Transaction tx = createPendingTransaction("tx-crash-hold-2");
        EventEnvelope<FundsHeld> fundsHeld1 = buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-crash-hold-2");
        transactionService.handleFundsHeld(fundsHeld1);

        tx = transactionRepository.findByTransactionId("tx-crash-hold-2").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("SOURCE_HELD");

        long outboxCountBefore = outboxEventRepository.count();

        // 2. Replay FundsHeld (simulates redelivery after crash recovery)
        EventEnvelope<FundsHeld> fundsHeld2 = buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-crash-hold-2");
        transactionService.handleFundsHeld(fundsHeld2);

        // 3. State unchanged, no additional outbox events
        tx = transactionRepository.findByTransactionId("tx-crash-hold-2").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("SOURCE_HELD");
        assertThat(outboxEventRepository.count()).isEqualTo(outboxCountBefore);
    }

    // ──────────────────────────────────────────────────────────────
    // §4 — Crash After Ledger Posting
    // ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("§4: Crash after Ledger post — replay LedgerTransactionPosted recovers SOURCE_HELD → LEDGER_POSTED")
    void testCrashAfterLedgerPost_replayRecovery() {
        // 1. Create transaction, process FundsHeld → SOURCE_HELD
        Transaction tx = createPendingTransaction("tx-crash-ledger-1");
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-crash-ledger-1"));
        
        tx = transactionRepository.findByTransactionId("tx-crash-ledger-1").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("SOURCE_HELD");
        outboxEventRepository.deleteAll();

        /*
         * CRASH SIMULATION:
         * Ledger successfully commits and produces LedgerTransactionPosted.
         * Transaction Service crashes BEFORE processing the event.
         *
         * At this point:
         *   Transaction = SOURCE_HELD (persisted before crash)
         *   Account = HELD
         *   Ledger = POSTED (persisted by Ledger Service)
         *
         * This is a RECOVERABLE INTERMEDIATE STATE.
         * Recovery: Kafka redelivers LedgerTransactionPosted after restart.
         */

        // 2. After restart, replay LedgerTransactionPosted
        ObjectNode ledgerPayload = new ObjectMapper().createObjectNode();
        transactionService.handleLedgerTransactionPosted(
                buildEnvelope("LedgerTransactionPosted", ledgerPayload, "tx-crash-ledger-1"));

        // 3. Verify recovery
        tx = transactionRepository.findByTransactionId("tx-crash-ledger-1").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("LEDGER_POSTED");

        // One FundsSettlementRequested produced
        assertThat(outboxEventRepository.findAll().stream()
                .filter(e -> "FundsSettlementRequested".equals(e.getEventType()))
                .count()).isEqualTo(1);
    }

    @Test
    @DisplayName("§4: Replay LedgerTransactionPosted when already LEDGER_POSTED is idempotent")
    void testReplayLedgerPosted_alreadyLedgerPosted_noDoubleTransition() {
        // 1. Progress to LEDGER_POSTED
        Transaction tx = createPendingTransaction("tx-crash-ledger-2");
        transactionService.handleFundsHeld(buildEnvelope("FundsHeld",
                new FundsHeld(sourceId, new BigDecimal("100.00"), "USD"), "tx-crash-ledger-2"));
        
        ObjectNode ledgerPayload = new ObjectMapper().createObjectNode();
        transactionService.handleLedgerTransactionPosted(
                buildEnvelope("LedgerTransactionPosted", ledgerPayload, "tx-crash-ledger-2"));

        tx = transactionRepository.findByTransactionId("tx-crash-ledger-2").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("LEDGER_POSTED");

        long outboxCountBefore = outboxEventRepository.count();

        // 2. Replay (redelivery)
        transactionService.handleLedgerTransactionPosted(
                buildEnvelope("LedgerTransactionPosted", ledgerPayload, "tx-crash-ledger-2"));

        // 3. No additional effect
        tx = transactionRepository.findByTransactionId("tx-crash-ledger-2").orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("LEDGER_POSTED");
        assertThat(outboxEventRepository.count()).isEqualTo(outboxCountBefore);
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
        com.dftp.transaction.api.dto.TransactionResponse res = transactionService.processTransaction(request, "corr-crash", UUID.randomUUID().toString());
        return transactionRepository.findById(res.getId()).orElseThrow();
    }

    private <T> EventEnvelope<T> buildEnvelope(String eventType, T payload, String txId) {
        return EventEnvelope.<T>builder()
                .eventId(UUID.randomUUID())
                .eventType(eventType)
                .transactionId(txId)
                .correlationId("corr-crash")
                .payload(payload)
                .build();
    }
}
