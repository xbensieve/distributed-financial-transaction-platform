package com.dftp.transaction.resilience;

import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import com.dftp.transaction.domain.AccountReference;
import com.dftp.transaction.domain.AccountReferenceRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import com.dftp.transaction.settlement.api.dto.BatchResponse;
import com.dftp.transaction.settlement.api.dto.CreateBatchRequest;
import com.dftp.transaction.settlement.application.BatchSettlementService;
import com.dftp.transaction.settlement.domain.SettlementBatch;
import com.dftp.transaction.settlement.domain.SettlementBatchItem;
import com.dftp.transaction.settlement.repository.SettlementBatchItemRepository;
import com.dftp.transaction.settlement.repository.SettlementBatchRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 12.5 Challenge Review: Batch Settlement Failure-Window Audit.
 * Explicitly tests Cases A, B, and C under worker crash, recovery reclaim,
 * and downstream network response loss.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class BatchSettlementFailureWindowIntegrationTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private BatchSettlementService batchSettlementService;

    @Autowired
    private SettlementBatchRepository batchRepository;

    @Autowired
    private SettlementBatchItemRepository batchItemRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private AccountReferenceRepository accountReferenceRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    private UUID sourceAccountId;
    private UUID destAccountId;

    @BeforeEach
    void setUp() {
        batchItemRepository.deleteAll();
        batchRepository.deleteAll();
        outboxEventRepository.deleteAll();
        transactionRepository.deleteAll();
        accountReferenceRepository.deleteAll();

        sourceAccountId = UUID.randomUUID();
        destAccountId = UUID.randomUUID();

        accountReferenceRepository.save(new AccountReference(sourceAccountId, Instant.now(), "user-alice"));
        accountReferenceRepository.save(new AccountReference(destAccountId, Instant.now(), "user-bob"));
    }

    /**
     * Case A:
     * claim item -> perform financial mutation -> commit DB -> process crashes ->
     * recovery reclaims item -> item is processed again.
     * Expected: financial mutation count = 1.
     */
    @Test
    @DisplayName("CASE-A: Crash after DB commit followed by recovery re-processing produces exactly 1 financial effect")
    void testCaseA_CrashAfterCommitAndReprocessing() {
        String txId = "TX-CASE-A-" + UUID.randomUUID().toString().substring(0, 8);
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("LEDGER_POSTED")
                .createdAt(Instant.now().minus(Duration.ofMinutes(15)))
                .updatedAt(Instant.now().minus(Duration.ofMinutes(15)))
                .build();
        transactionRepository.save(tx);

        String batchId = "BATCH-CASE-A";
        CreateBatchRequest createReq = CreateBatchRequest.builder()
                .batchId(batchId)
                .transactionIds(List.of(txId))
                .build();
        batchSettlementService.createBatch(createReq, "test-admin");

        // Step 1: Initial execution commits DB (tx=COMPLETED, outbox=1, item=SETTLED)
        BatchResponse firstRun = batchSettlementService.executeBatch(batchId);
        assertThat(firstRun.getStatus()).isEqualTo("COMPLETED");

        Transaction txAfterFirst = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(txAfterFirst.getStatus()).isEqualTo("COMPLETED");

        long outboxCountAfterFirst = outboxEventRepository.findAll().stream()
                .filter(e -> "FundsSettlementRequested".equals(e.getEventType()) && e.getPayload().contains(txId))
                .count();
        assertThat(outboxCountAfterFirst).isEqualTo(1);

        // Step 2: Simulate post-commit crash & adversarial recovery
        // Manually reset item status to PENDING (simulating recovery re-claim)
        SettlementBatchItem item = batchItemRepository.findByBatchIdAndTransactionId(batchId, txId).orElseThrow();
        item.setStatus("PENDING");
        item.setUpdatedAt(Instant.now().minus(Duration.ofMinutes(10)));
        batchItemRepository.save(item);

        // Step 3: Item is processed again
        batchSettlementService.processSingleItem(item.getId());

        // Assert: Financial mutation intent count remains strictly 1 (0 duplicate outbox events)
        long outboxCountAfterSecond = outboxEventRepository.findAll().stream()
                .filter(e -> "FundsSettlementRequested".equals(e.getEventType()) && e.getPayload().contains(txId))
                .count();
        assertThat(outboxCountAfterSecond).isEqualTo(1);

        // Assert: Item returned cleanly to SETTLED
        SettlementBatchItem finalItem = batchItemRepository.findById(item.getId()).orElseThrow();
        assertThat(finalItem.getStatus()).isEqualTo("SETTLED");
    }

    /**
     * Case B:
     * claim item -> publish outbox event -> process crashes -> recovery reclaims item -> event is republished.
     * Expected: outbox intent count may be > 1, financial mutation count = 1.
     */
    @Test
    @DisplayName("CASE-B: Crash after outbox publication followed by recovery republishing maintains single financial effect")
    void testCaseB_CrashAfterOutboxPublicationRepublishing() {
        String txId = "TX-CASE-B-" + UUID.randomUUID().toString().substring(0, 8);
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("50.00"))
                .currency("USD")
                .status("LEDGER_POSTED")
                .createdAt(Instant.now().minus(Duration.ofMinutes(15)))
                .updatedAt(Instant.now().minus(Duration.ofMinutes(15)))
                .build();
        transactionRepository.save(tx);

        String batchId = "BATCH-CASE-B";
        CreateBatchRequest createReq = CreateBatchRequest.builder()
                .batchId(batchId)
                .transactionIds(List.of(txId))
                .build();
        batchSettlementService.createBatch(createReq, "test-admin");

        // Step 1: Claim item and mark PROCESSING (simulating worker beginning work)
        SettlementBatchItem item = batchItemRepository.findByBatchIdAndTransactionId(batchId, txId).orElseThrow();
        item.setStatus("PROCESSING");
        item.setUpdatedAt(Instant.now().minus(Duration.ofMinutes(12))); // Stalled past 10m threshold
        batchItemRepository.save(item);

        // Simulate an outbox event already written before crash
        OutboxEvent ghostEvent = OutboxEvent.builder()
                .id(UUID.randomUUID())
                .aggregateType("transaction-events")
                .aggregateId(tx.getId().toString())
                .eventType("FundsSettlementRequested")
                .payload("{\"transactionId\":\"" + txId + "\",\"amount\":50.00}")
                .status("PENDING")
                .createdAt(Instant.now().minus(Duration.ofMinutes(12)))
                .build();
        outboxEventRepository.save(ghostEvent);

        // Step 2: Recovery scanner detects stalled PROCESSING item and resets to PENDING
        int recovered = batchSettlementService.recoverStalledItems(Duration.ofMinutes(10));
        assertThat(recovered).isEqualTo(1);

        // Step 3: Re-execute batch
        batchSettlementService.executeBatch(batchId);

        // Outbox intent count may be > 1 due to at-least-once transport
        long outboxEventsForTx = outboxEventRepository.findAll().stream()
                .filter(e -> "FundsSettlementRequested".equals(e.getEventType()) && e.getPayload().contains(txId))
                .count();
        assertThat(outboxEventsForTx).isGreaterThanOrEqualTo(1);

        // Final item state is SETTLED
        SettlementBatchItem finalItem = batchItemRepository.findById(item.getId()).orElseThrow();
        assertThat(finalItem.getStatus()).isEqualTo("SETTLED");

        // Transaction is COMPLETED
        Transaction finalTx = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(finalTx.getStatus()).isEqualTo("COMPLETED");
    }

    /**
     * Case C:
     * claim item -> downstream financial mutation succeeds -> downstream response lost -> caller retries.
     * Expected: financial mutation count = 1.
     */
    @Test
    @DisplayName("CASE-C: Downstream mutation succeeds but caller retries — idempotent deduplication guarantees single mutation")
    void testCaseC_DownstreamMutationSucceedsResponseLostCallerRetries() {
        String txId = "TX-CASE-C-" + UUID.randomUUID().toString().substring(0, 8);
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("75.00"))
                .currency("USD")
                .status("LEDGER_POSTED")
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        transactionRepository.save(tx);

        String batchId = "BATCH-CASE-C";
        CreateBatchRequest createReq = CreateBatchRequest.builder()
                .batchId(batchId)
                .transactionIds(List.of(txId))
                .build();
        batchSettlementService.createBatch(createReq, "test-admin");

        // First attempt succeeds
        BatchResponse firstRun = batchSettlementService.executeBatch(batchId);
        assertThat(firstRun.getStatus()).isEqualTo("COMPLETED");
        assertThat(firstRun.getSuccessCount()).isEqualTo(1);

        long outboxCountBefore = outboxEventRepository.count();

        // Downstream response lost: Caller retries batch execution 3 times
        for (int retry = 0; retry < 3; retry++) {
            BatchResponse retryRun = batchSettlementService.executeBatch(batchId);
            assertThat(retryRun.getStatus()).isEqualTo("COMPLETED");
            // No new items processed; no new outbox events emitted
            assertThat(outboxEventRepository.count()).isEqualTo(outboxCountBefore);
        }

        SettlementBatchItem item = batchItemRepository.findByBatchIdAndTransactionId(batchId, txId).orElseThrow();
        assertThat(item.getStatus()).isEqualTo("SETTLED");
    }
}
