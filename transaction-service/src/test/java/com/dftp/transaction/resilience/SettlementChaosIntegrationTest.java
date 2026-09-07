package com.dftp.transaction.resilience;

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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 12 Chaos & Resilience: Settlement Engine Adversarial Attacks.
 * Stresses worker crashes, concurrent multi-worker batch processing,
 * stalled item recovery, and repeated execution idempotency.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SettlementChaosIntegrationTest extends AbstractTransactionIntegrationTest {

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

    @Test
    @DisplayName("CHAOS-SETTLE-01: Multi-worker race on same batch produces <= 1 financial effect per transaction")
    void testConcurrentWorkersSameBatch() throws Exception {
        int txCount = 15;
        List<String> txIds = new ArrayList<>();

        for (int i = 0; i < txCount; i++) {
            String txId = "TX-CHAOS-SETTLE-" + i + "-" + UUID.randomUUID().toString().substring(0, 8);
            Transaction tx = Transaction.builder()
                    .id(UUID.randomUUID())
                    .transactionId(txId)
                    .ownerId("user-alice")
                    .sourceAccountId(sourceAccountId)
                    .destinationAccountId(destAccountId)
                    .amount(new BigDecimal("10.00"))
                    .currency("USD")
                    .status("LEDGER_POSTED")
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();
            transactionRepository.save(tx);
            txIds.add(txId);
        }

        String batchId = "BATCH-CHAOS-01";
        CreateBatchRequest createReq = CreateBatchRequest.builder()
                .batchId(batchId)
                .transactionIds(txIds)
                .build();
        BatchResponse created = batchSettlementService.createBatch(createReq, "test-admin");
        assertThat(created.getTotalItems()).isEqualTo(txCount);

        outboxEventRepository.deleteAll();

        int workerCount = 6;
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(workerCount);
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < workerCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    batchSettlementService.executeBatch(batchId);
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(errors.get()).isEqualTo(0);

        // Invariant 1: Exactly txCount items settled
        long settledCount = batchItemRepository.countByBatchIdAndStatus(batchId, "SETTLED");
        assertThat(settledCount).isEqualTo(txCount);

        // Invariant 2: Exactly txCount outbox events produced (ZERO duplicates)
        long outboxCount = outboxEventRepository.count();
        assertThat(outboxCount).isEqualTo(txCount);

        // Invariant 3: Batch state is COMPLETED
        BatchResponse finalized = batchSettlementService.getBatch(batchId);
        assertThat(finalized.getStatus()).isEqualTo("COMPLETED");
        assertThat(finalized.getSuccessCount()).isEqualTo(txCount);
    }

    @Test
    @DisplayName("CHAOS-SETTLE-02: Repeated execution of a completed batch is an idempotent no-op")
    void testRepeatedExecutionOfCompletedBatch() {
        String txId = "TX-CHAOS-REPEAT-" + UUID.randomUUID().toString().substring(0, 8);
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("25.00"))
                .currency("USD")
                .status("LEDGER_POSTED")
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        transactionRepository.save(tx);

        String batchId = "BATCH-CHAOS-REPEAT";
        CreateBatchRequest createReq = CreateBatchRequest.builder()
                .batchId(batchId)
                .transactionIds(List.of(txId))
                .build();
        batchSettlementService.createBatch(createReq, "test-admin");
        BatchResponse firstRun = batchSettlementService.executeBatch(batchId);
        assertThat(firstRun.getStatus()).isEqualTo("COMPLETED");
        assertThat(firstRun.getSuccessCount()).isEqualTo(1);

        long outboxBefore = outboxEventRepository.count();

        // Repeated execution
        BatchResponse secondRun = batchSettlementService.executeBatch(batchId);
        assertThat(secondRun.getStatus()).isEqualTo("COMPLETED");
        assertThat(outboxEventRepository.count()).isEqualTo(outboxBefore);
    }

    @Test
    @DisplayName("CHAOS-SETTLE-03: Stalled worker crash recovery reclaims hung PROCESSING items")
    void testStalledWorkerCrashRecovery() {
        String txId = "TX-CHAOS-STALL-" + UUID.randomUUID().toString().substring(0, 8);
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("30.00"))
                .currency("USD")
                .status("LEDGER_POSTED")
                .createdAt(Instant.now().minus(Duration.ofMinutes(20)))
                .updatedAt(Instant.now().minus(Duration.ofMinutes(20)))
                .build();
        transactionRepository.save(tx);

        String batchId = "BATCH-CHAOS-STALL";
        CreateBatchRequest createReq = CreateBatchRequest.builder()
                .batchId(batchId)
                .transactionIds(List.of(txId))
                .build();
        batchSettlementService.createBatch(createReq, "test-admin");

        // Manually simulate a crashed worker leaving the item stuck in PROCESSING
        SettlementBatchItem item = batchItemRepository.findByBatchIdAndTransactionId(batchId, txId).orElseThrow();
        item.setStatus("PROCESSING");
        item.setUpdatedAt(Instant.now().minus(Duration.ofMinutes(15))); // Stalled past 10m threshold
        batchItemRepository.save(item);

        // Execute recovery
        int recoveredCount = batchSettlementService.recoverStalledItems(Duration.ofMinutes(10));
        assertThat(recoveredCount).isGreaterThanOrEqualTo(1);

        // Verify item returned to PENDING so next cycle can complete it
        SettlementBatchItem recovered = batchItemRepository.findByBatchIdAndTransactionId(batchId, txId).orElseThrow();
        assertThat(recovered.getStatus()).isEqualTo("PENDING");

        // Now process batch: completes successfully
        BatchResponse resp = batchSettlementService.executeBatch(batchId);
        assertThat(resp.getStatus()).isEqualTo("COMPLETED");
        assertThat(resp.getSuccessCount()).isEqualTo(1);
    }
}
