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
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class BatchSettlementRecoveryTest extends AbstractTransactionIntegrationTest {

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
    @DisplayName("REC-BATCH-01: Stalled PROCESSING items from worker crash are recovered and successfully retried")
    void testCrashRecoveryAndRetry() {
        String txId = "TX-CRASH-" + UUID.randomUUID().toString().substring(0, 8);
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(txId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("75.00"))
                .currency("USD")
                .status("LEDGER_POSTED")
                .createdAt(Instant.now().minusSeconds(180))
                .updatedAt(Instant.now().minusSeconds(180))
                .build();
        transactionRepository.save(tx);

        CreateBatchRequest createReq = CreateBatchRequest.builder()
                .batchId("BATCH-RECOVERY-01")
                .transactionIds(List.of(txId))
                .build();
        batchSettlementService.createBatch(createReq, "test-admin");

        // Simulate crash: mark item as PROCESSING with stale updatedAt (3 minutes ago)
        SettlementBatchItem item = batchItemRepository.findByBatchIdAndTransactionId("BATCH-RECOVERY-01", txId).orElseThrow();
        item.setStatus("PROCESSING");
        item.setUpdatedAt(Instant.now().minusSeconds(180));
        batchItemRepository.save(item);

        // Run recovery scanner with 2-minute threshold
        int recoveredCount = batchSettlementService.recoverStalledItems(Duration.ofMinutes(2));
        assertThat(recoveredCount).isEqualTo(1);

        // Verify item was reset to PENDING
        SettlementBatchItem recoveredItem = batchItemRepository.findById(item.getId()).orElseThrow();
        assertThat(recoveredItem.getStatus()).isEqualTo("PENDING");
        assertThat(recoveredItem.getErrorMessage()).contains("Recovered from worker crash");

        // Execute batch to complete
        BatchResponse completedBatch = batchSettlementService.executeBatch("BATCH-RECOVERY-01");
        assertThat(completedBatch.getStatus()).isEqualTo("COMPLETED");
        assertThat(completedBatch.getSuccessCount()).isEqualTo(1);
        assertThat(completedBatch.getFailureCount()).isEqualTo(0);

        Transaction finalTx = transactionRepository.findByTransactionId(txId).orElseThrow();
        assertThat(finalTx.getStatus()).isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("REC-BATCH-02: Non-settleable failed transactions are skipped, resulting in PARTIAL_FAILURE with retry safety")
    void testNonSettleableTransactionHandling() {
        String goodTxId = "TX-GOOD-" + UUID.randomUUID().toString().substring(0, 8);
        Transaction goodTx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(goodTxId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("10.00"))
                .currency("USD")
                .status("LEDGER_POSTED")
                .createdAt(Instant.now().minusSeconds(60))
                .updatedAt(Instant.now().minusSeconds(60))
                .build();
        transactionRepository.save(goodTx);

        String badTxId = "TX-BAD-" + UUID.randomUUID().toString().substring(0, 8);
        Transaction badTx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId(badTxId)
                .ownerId("user-alice")
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("20.00"))
                .currency("USD")
                .status("FAILED") // Failed transaction
                .createdAt(Instant.now().minusSeconds(60))
                .updatedAt(Instant.now().minusSeconds(60))
                .build();
        transactionRepository.save(badTx);

        CreateBatchRequest createReq = CreateBatchRequest.builder()
                .batchId("BATCH-PARTIAL-01")
                .transactionIds(List.of(goodTxId, badTxId))
                .build();
        batchSettlementService.createBatch(createReq, "test-admin");

        BatchResponse response = batchSettlementService.executeBatch("BATCH-PARTIAL-01");
        assertThat(response.getStatus()).isEqualTo("PARTIAL_FAILURE");
        assertThat(response.getSuccessCount()).isEqualTo(1);
        assertThat(response.getFailureCount()).isEqualTo(1);

        SettlementBatchItem badItem = batchItemRepository.findByBatchIdAndTransactionId("BATCH-PARTIAL-01", badTxId).orElseThrow();
        assertThat(badItem.getStatus()).isEqualTo("SKIPPED");
        assertThat(badItem.getErrorMessage()).contains("non-settleable status: FAILED");
    }
}
