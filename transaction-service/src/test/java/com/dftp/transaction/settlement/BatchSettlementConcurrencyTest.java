package com.dftp.transaction.settlement;

import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import com.dftp.transaction.domain.AccountReference;
import com.dftp.transaction.domain.AccountReferenceRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import com.dftp.transaction.settlement.api.dto.BatchResponse;
import com.dftp.transaction.settlement.api.dto.CreateBatchRequest;
import com.dftp.transaction.settlement.application.BatchSettlementService;
import com.dftp.transaction.settlement.domain.SettlementBatchItem;
import com.dftp.transaction.settlement.repository.SettlementBatchItemRepository;
import com.dftp.transaction.settlement.repository.SettlementBatchRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class BatchSettlementConcurrencyTest extends AbstractTransactionIntegrationTest {

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
    @DisplayName("CONC-BATCH-01: 10 concurrent workers executing the same batch result in exactly one settlement per item")
    void testConcurrentWorkersSameBatch() throws Exception {
        int txCount = 20;
        List<String> txIds = new ArrayList<>();

        for (int i = 0; i < txCount; i++) {
            String txId = "TX-BATCH-" + i + "-" + UUID.randomUUID().toString().substring(0, 8);
            Transaction tx = Transaction.builder()
                    .id(UUID.randomUUID())
                    .transactionId(txId)
                    .ownerId("user-alice")
                    .sourceAccountId(sourceAccountId)
                    .destinationAccountId(destAccountId)
                    .amount(new BigDecimal("50.00"))
                    .currency("USD")
                    .status("LEDGER_POSTED")
                    .createdAt(Instant.now().minusSeconds(60))
                    .updatedAt(Instant.now().minusSeconds(60))
                    .build();
            transactionRepository.save(tx);
            txIds.add(txId);
        }

        CreateBatchRequest createReq = CreateBatchRequest.builder()
                .batchId("BATCH-CONC-01")
                .transactionIds(txIds)
                .build();
        BatchResponse created = batchSettlementService.createBatch(createReq, "test-admin");
        assertThat(created.getTotalItems()).isEqualTo(txCount);
        assertThat(created.getStatus()).isEqualTo("CREATED");

        // Execute batch concurrently using 10 worker threads
        int workerCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(workerCount);
        AtomicInteger workerErrors = new AtomicInteger(0);

        for (int w = 0; w < workerCount; w++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    batchSettlementService.executeBatch("BATCH-CONC-01");
                } catch (Exception e) {
                    log.error("Worker error during concurrent batch execution", e);
                    workerErrors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(workerErrors.get()).isEqualTo(0);

        // Verify final batch state
        BatchResponse finalized = batchSettlementService.getBatch("BATCH-CONC-01");
        assertThat(finalized.getStatus()).isEqualTo("COMPLETED");
        assertThat(finalized.getSuccessCount()).isEqualTo(txCount);
        assertThat(finalized.getFailureCount()).isEqualTo(0);

        // Verify every transaction is COMPLETED with exactly one settlement outbox event
        for (String txId : txIds) {
            Transaction tx = transactionRepository.findByTransactionId(txId).orElseThrow();
            assertThat(tx.getStatus()).isEqualTo("COMPLETED");

            long outboxCount = outboxEventRepository.findAll().stream()
                    .filter(ev -> "FundsSettlementRequested".equals(ev.getEventType()) && ev.getPayload().contains(txId))
                    .count();
            assertThat(outboxCount).isEqualTo(1);
        }

        // Verify batch item rows
        List<SettlementBatchItem> items = batchItemRepository.findByBatchId("BATCH-CONC-01");
        assertThat(items).hasSize(txCount);
        for (SettlementBatchItem item : items) {
            assertThat(item.getStatus()).isEqualTo("SETTLED");
            assertThat(item.getProcessedAt()).isNotNull();
        }
    }

    @Test
    @DisplayName("IDEMP-BATCH-02: Executing the same batch multiple times produces zero duplicate financial side-effects")
    void testBatchExecutionIdempotency() {
        String txId = "TX-IDEMP-" + UUID.randomUUID().toString().substring(0, 8);
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("LEDGER_POSTED")
                .createdAt(Instant.now().minusSeconds(120))
                .updatedAt(Instant.now().minusSeconds(120))
                .build();
        transactionRepository.save(tx);

        CreateBatchRequest createReq = CreateBatchRequest.builder()
                .batchId("BATCH-IDEMP-01")
                .transactionIds(List.of(txId))
                .build();
        batchSettlementService.createBatch(createReq, "test-admin");

        // First execution
        BatchResponse run1 = batchSettlementService.executeBatch("BATCH-IDEMP-01");
        assertThat(run1.getStatus()).isEqualTo("COMPLETED");
        assertThat(run1.getSuccessCount()).isEqualTo(1);

        long outboxCount1 = outboxEventRepository.findAll().stream()
                .filter(ev -> "FundsSettlementRequested".equals(ev.getEventType()) && ev.getPayload().contains(txId))
                .count();
        assertThat(outboxCount1).isEqualTo(1);

        // Second execution (same batch)
        BatchResponse run2 = batchSettlementService.executeBatch("BATCH-IDEMP-01");
        assertThat(run2.getStatus()).isEqualTo("COMPLETED");
        assertThat(run2.getSuccessCount()).isEqualTo(1);

        long outboxCount2 = outboxEventRepository.findAll().stream()
                .filter(ev -> "FundsSettlementRequested".equals(ev.getEventType()) && ev.getPayload().contains(txId))
                .count();
        // Still exactly 1 outbox event emitted
        assertThat(outboxCount2).isEqualTo(1);
    }
}
