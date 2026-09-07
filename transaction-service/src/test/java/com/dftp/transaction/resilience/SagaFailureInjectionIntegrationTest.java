package com.dftp.transaction.resilience;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.FundsHeld;
import com.dftp.common.event.payload.FundsHoldRejected;
import com.dftp.common.event.payload.FundsSettled;
import com.dftp.common.event.payload.HoldCompensated;
import com.dftp.common.event.payload.LedgerPostRejected;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
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
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 12 Chaos & Resilience: Saga State Machine Failure Injection.
 * Adversarially verifies state transition guards under out-of-order delivery,
 * duplicate delivery, late compensation, and zombie message arrival.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SagaFailureInjectionIntegrationTest extends AbstractTransactionIntegrationTest {

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
    @DisplayName("CHAOS-SAGA-01: Out-of-order event rejected safely, then self-heals to COMPLETED when in-order events arrive")
    void testOutOfOrderLedgerPostedBeforeHold() {
        String txId = "chaos-tx-ooo-1";
        createTransaction(txId);

        EventEnvelope<Object> prematureLedgerEvent = EventEnvelope.builder()
                .eventId(UUID.randomUUID())
                .eventType("LedgerTransactionPosted")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(Map.of("ledgerTransactionId", "ledger-ooo-1", "status", "POSTED"))
                .build();

        long outboxCountBefore = outboxEventRepository.count();

        // Step 1: Premature event is safely rejected because transaction is PENDING, not SOURCE_HELD
        transactionService.handleLedgerTransactionPosted(prematureLedgerEvent);

        Transaction tx = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("PENDING");
        assertThat(outboxEventRepository.count()).isEqualTo(outboxCountBefore);

        // Step 2: Delayed FundsHeld arrives
        EventEnvelope<FundsHeld> holdEvent = EventEnvelope.<FundsHeld>builder()
                .eventId(UUID.randomUUID())
                .eventType("FundsHeld")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(FundsHeld.builder()
                        .sourceAccountId(sourceId)
                        .amount(new BigDecimal("50.00"))
                        .currency("USD")
                        .build())
                .build();
        transactionService.handleFundsHeld(holdEvent);

        Transaction txHeld = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(txHeld.getStatus()).isEqualTo("SOURCE_HELD");

        // Step 3: Legitimate LedgerTransactionPosted arrives in response to LedgerPostRequested
        EventEnvelope<Object> validLedgerEvent = EventEnvelope.builder()
                .eventId(UUID.randomUUID())
                .eventType("LedgerTransactionPosted")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(Map.of("ledgerTransactionId", "ledger-valid-1", "status", "POSTED"))
                .build();
        transactionService.handleLedgerTransactionPosted(validLedgerEvent);

        Transaction txLedger = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(txLedger.getStatus()).isEqualTo("LEDGER_POSTED");

        // Step 4: FundsSettled arrives -> converges to COMPLETED
        EventEnvelope<FundsSettled> settleEvent = EventEnvelope.<FundsSettled>builder()
                .eventId(UUID.randomUUID())
                .eventType("FundsSettled")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(FundsSettled.builder()
                        .sourceAccountId(sourceId)
                        .destinationAccountId(destId)
                        .amount(new BigDecimal("50.00"))
                        .build())
                .build();
        transactionService.handleFundsSettled(settleEvent);

        Transaction txFinal = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(txFinal.getStatus()).isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("CHAOS-SAGA-02: Stale FundsHeld replayed after transaction COMPLETED is a safe no-op")
    void testStaleFundsHeldAfterCompletion() {
        String txId = "chaos-tx-stale-1";
        createTransaction(txId);

        // Advance to COMPLETED
        advanceToCompleted(txId);

        Transaction txBefore = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(txBefore.getStatus()).isEqualTo("COMPLETED");
        Long versionBefore = txBefore.getVersion();
        long outboxCountBefore = outboxEventRepository.count();

        // Replay stale FundsHeld
        EventEnvelope<FundsHeld> staleHold = EventEnvelope.<FundsHeld>builder()
                .eventId(UUID.randomUUID())
                .eventType("FundsHeld")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(FundsHeld.builder()
                        .sourceAccountId(sourceId)
                        .amount(new BigDecimal("50.00"))
                        .currency("USD")
                        .build())
                .build();

        transactionService.handleFundsHeld(staleHold);

        Transaction txAfter = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(txAfter.getStatus()).isEqualTo("COMPLETED");
        assertThat(txAfter.getVersion()).isEqualTo(versionBefore);
        assertThat(outboxEventRepository.count()).isEqualTo(outboxCountBefore);
    }

    @Test
    @DisplayName("CHAOS-SAGA-03: Delayed compensation arriving on COMPLETED transaction must not regress to FAILED")
    void testLateCompensationOnCompletedTransactionRejected() {
        String txId = "chaos-tx-comp-late-1";
        createTransaction(txId);
        advanceToCompleted(txId);

        Transaction txBefore = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(txBefore.getStatus()).isEqualTo("COMPLETED");
        long outboxCountBefore = outboxEventRepository.count();

        // Delayed hold rejection attempt
        EventEnvelope<FundsHoldRejected> rejectedHold = EventEnvelope.<FundsHoldRejected>builder()
                .eventId(UUID.randomUUID())
                .eventType("FundsHoldRejected")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(FundsHoldRejected.builder()
                        .sourceAccountId(sourceId)
                        .reason("Late timeout rejection")
                        .build())
                .build();
        transactionService.handleFundsHoldRejected(rejectedHold);

        // Delayed ledger rejection attempt
        EventEnvelope<LedgerPostRejected> rejectedLedger = EventEnvelope.<LedgerPostRejected>builder()
                .eventId(UUID.randomUUID())
                .eventType("LedgerPostRejected")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(LedgerPostRejected.builder()
                        .reason("Late ledger failure")
                        .build())
                .build();
        transactionService.handleLedgerPostRejected(rejectedLedger);

        Transaction txAfter = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(txAfter.getStatus()).isEqualTo("COMPLETED");
        assertThat(outboxEventRepository.count()).isEqualTo(outboxCountBefore);
    }

    @Test
    @DisplayName("CHAOS-SAGA-04: Delayed settlement arriving on FAILED transaction must not transition to COMPLETED")
    void testSettlementOnFailedTransactionRejected() {
        String txId = "chaos-tx-fail-late-1";
        createTransaction(txId);

        // Transition PENDING -> SOURCE_HELD -> COMPENSATING -> FAILED
        EventEnvelope<FundsHeld> holdEvent = EventEnvelope.<FundsHeld>builder()
                .eventId(UUID.randomUUID())
                .eventType("FundsHeld")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(FundsHeld.builder()
                        .sourceAccountId(sourceId)
                        .amount(new BigDecimal("50.00"))
                        .currency("USD")
                        .build())
                .build();
        transactionService.handleFundsHeld(holdEvent);

        // Fail during ledger posting -> COMPENSATING
        EventEnvelope<LedgerPostRejected> rejectEvent = EventEnvelope.<LedgerPostRejected>builder()
                .eventId(UUID.randomUUID())
                .eventType("LedgerPostRejected")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(LedgerPostRejected.builder().reason("Ledger test failure").build())
                .build();
        transactionService.handleLedgerPostRejected(rejectEvent);

        // Funds compensation complete -> FAILED
        EventEnvelope<HoldCompensated> compEvent = EventEnvelope.<HoldCompensated>builder()
                .eventId(UUID.randomUUID())
                .eventType("HoldCompensated")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(HoldCompensated.builder().sourceAccountId(sourceId).build())
                .build();
        transactionService.handleHoldCompensated(compEvent);

        Transaction txFailed = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(txFailed.getStatus()).isEqualTo("FAILED");

        // Now simulate delayed FundsSettled event arriving
        EventEnvelope<FundsSettled> lateSettle = EventEnvelope.<FundsSettled>builder()
                .eventId(UUID.randomUUID())
                .eventType("FundsSettled")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(FundsSettled.builder()
                        .sourceAccountId(sourceId)
                        .destinationAccountId(destId)
                        .amount(new BigDecimal("50.00"))
                        .build())
                .build();

        transactionService.handleFundsSettled(lateSettle);

        Transaction txFinal = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(txFinal.getStatus()).isEqualTo("FAILED");
    }

    @Test
    @DisplayName("CHAOS-SAGA-05: Repeated duplicate FundsSettled produces exactly 1 completion and 0 new outbox entries")
    void testDuplicateFundsSettledReplayIdempotency() {
        String txId = "chaos-tx-dup-settle-1";
        createTransaction(txId);
        advanceToCompleted(txId);

        long outboxCountAtCompletion = outboxEventRepository.count();

        EventEnvelope<FundsSettled> dupSettle = EventEnvelope.<FundsSettled>builder()
                .eventId(UUID.randomUUID())
                .eventType("FundsSettled")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(FundsSettled.builder()
                        .sourceAccountId(sourceId)
                        .destinationAccountId(destId)
                        .amount(new BigDecimal("50.00"))
                        .build())
                .build();

        // Replay 5 times
        for (int i = 0; i < 5; i++) {
            transactionService.handleFundsSettled(dupSettle);
        }

        Transaction tx = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(tx.getStatus()).isEqualTo("COMPLETED");
        assertThat(outboxEventRepository.count()).isEqualTo(outboxCountAtCompletion);
    }

    private void createTransaction(String txId) {
        CreateTransactionRequest req = CreateTransactionRequest.builder()
                .transactionId(txId)
                .sourceAccountId(sourceId)
                .destinationAccountId(destId)
                .amount(new BigDecimal("50.00"))
                .currency("USD")
                .build();
        transactionService.processTransaction(req, UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    private void advanceToCompleted(String txId) {
        EventEnvelope<FundsHeld> holdEvent = EventEnvelope.<FundsHeld>builder()
                .eventId(UUID.randomUUID())
                .eventType("FundsHeld")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(FundsHeld.builder()
                        .sourceAccountId(sourceId)
                        .amount(new BigDecimal("50.00"))
                        .currency("USD")
                        .build())
                .build();
        transactionService.handleFundsHeld(holdEvent);

        EventEnvelope<Object> ledgerEvent = EventEnvelope.builder()
                .eventId(UUID.randomUUID())
                .eventType("LedgerTransactionPosted")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(Map.of("ledgerTransactionId", "ledger-" + txId, "status", "POSTED"))
                .build();
        transactionService.handleLedgerTransactionPosted(ledgerEvent);

        EventEnvelope<FundsSettled> settleEvent = EventEnvelope.<FundsSettled>builder()
                .eventId(UUID.randomUUID())
                .eventType("FundsSettled")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .transactionId(txId)
                .payload(FundsSettled.builder()
                        .sourceAccountId(sourceId)
                        .destinationAccountId(destId)
                        .amount(new BigDecimal("50.00"))
                        .build())
                .build();
        transactionService.handleFundsSettled(settleEvent);
    }
}
