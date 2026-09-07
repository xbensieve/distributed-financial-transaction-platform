package com.dftp.transaction.settlement.application;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.FundsSettlementRequested;
import com.dftp.common.observability.DftpMetrics;
import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import com.dftp.transaction.settlement.api.dto.BatchItemResponse;
import com.dftp.transaction.settlement.api.dto.BatchResponse;
import com.dftp.transaction.settlement.api.dto.CreateBatchRequest;
import com.dftp.transaction.settlement.domain.SettlementBatch;
import com.dftp.transaction.settlement.domain.SettlementBatchItem;
import com.dftp.transaction.settlement.repository.SettlementBatchItemRepository;
import com.dftp.transaction.settlement.repository.SettlementBatchRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BatchSettlementService {

    private final SettlementBatchRepository batchRepository;
    private final SettlementBatchItemRepository itemRepository;
    private final TransactionRepository transactionRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;

    @Autowired(required = false)
    private DftpMetrics dftpMetrics = new DftpMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());

    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final int MAX_PAGE_SIZE = 100;

    public BatchSettlementService(
            SettlementBatchRepository batchRepository,
            SettlementBatchItemRepository itemRepository,
            TransactionRepository transactionRepository,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper,
            PlatformTransactionManager transactionManager) {
        this.batchRepository = batchRepository;
        this.itemRepository = itemRepository;
        this.transactionRepository = transactionRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /**
     * Creates a new settlement batch.
     * Can accept explicit transaction IDs or scan candidate transactions in LEDGER_POSTED status.
     */
    @Transactional
    public BatchResponse createBatch(CreateBatchRequest request, String createdBy) {
        String batchId = (request.getBatchId() != null && !request.getBatchId().isBlank())
                ? request.getBatchId()
                : "BATCH-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        if (batchRepository.existsByBatchId(batchId)) {
            throw new IllegalArgumentException("Settlement batch already exists: " + batchId);
        }

        List<Transaction> candidates = new ArrayList<>();
        if (request.getTransactionIds() != null && !request.getTransactionIds().isEmpty()) {
            for (String txId : request.getTransactionIds()) {
                transactionRepository.findByTransactionId(txId).ifPresent(candidates::add);
            }
        } else {
            // Select bounded candidates in LEDGER_POSTED status
            Page<Transaction> page = transactionRepository.findAnomalousTransactions(
                    List.of("LEDGER_POSTED"), Instant.now(), PageRequest.of(0, MAX_PAGE_SIZE));
            candidates.addAll(page.getContent());
        }

        SettlementBatch batch = SettlementBatch.builder()
                .id(UUID.randomUUID())
                .batchId(batchId)
                .status("CREATED")
                .totalItems(candidates.size())
                .successCount(0)
                .failureCount(0)
                .retryCount(0)
                .createdAt(Instant.now())
                .createdBy(createdBy != null ? createdBy : "system")
                .metadata(request.getMetadata())
                .build();

        batch = batchRepository.save(batch);

        List<SettlementBatchItem> items = new ArrayList<>();
        for (Transaction tx : candidates) {
            SettlementBatchItem item = SettlementBatchItem.builder()
                    .id(UUID.randomUUID())
                    .batchId(batchId)
                    .transactionId(tx.getTransactionId())
                    .sourceAccountId(tx.getSourceAccountId())
                    .destinationAccountId(tx.getDestinationAccountId())
                    .amount(tx.getAmount())
                    .currency(tx.getCurrency())
                    .status("PENDING")
                    .retryCount(0)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();
            items.add(item);
        }
        itemRepository.saveAll(items);

        dftpMetrics.recordBatchStarted("SETTLEMENT");
        dftpMetrics.recordBatchItems("SETTLEMENT", items.size());
        log.info("Settlement batch {} created with {} items by {}", batchId, items.size(), createdBy);

        return mapToResponse(batch, items);
    }

    /**
     * Executes the settlement batch with bounded pagination and row locking.
     * Multiple workers can call this concurrently without double processing.
     */
    public BatchResponse executeBatch(String batchId) {
        SettlementBatch batch = batchRepository.findByBatchId(batchId)
                .orElseThrow(() -> new IllegalArgumentException("Batch not found: " + batchId));

        startBatchExecution(batchId);

        boolean hasMore = true;
        while (hasMore) {
            List<UUID> claimedItemIds = claimNextItemIds(batchId, DEFAULT_PAGE_SIZE);
            if (claimedItemIds.isEmpty()) {
                hasMore = false;
            } else {
                for (UUID itemId : claimedItemIds) {
                    processSingleItem(itemId);
                }
            }
        }

        return finalizeBatchExecution(batchId);
    }

    public void startBatchExecution(String batchId) {
        try {
            transactionTemplate.executeWithoutResult(status -> {
                SettlementBatch batch = batchRepository.findByBatchId(batchId)
                        .orElseThrow(() -> new IllegalArgumentException("Batch not found: " + batchId));
                if ("CREATED".equals(batch.getStatus()) || "RETRYING".equals(batch.getStatus()) || "PARTIAL_FAILURE".equals(batch.getStatus())) {
                    batch.setStatus("RUNNING");
                    if (batch.getStartedAt() == null) {
                        batch.setStartedAt(Instant.now());
                    }
                    batchRepository.save(batch);
                }
            });
        } catch (org.springframework.dao.ConcurrencyFailureException e) {
            log.debug("Concurrent start collision for batch {}, already started by another worker", batchId);
        }
    }

    public List<UUID> claimNextItemIds(String batchId, int limit) {
        return transactionTemplate.execute(status -> {
            List<SettlementBatchItem> claimed = itemRepository.claimNextItemsForProcessing(batchId, limit);
            List<UUID> ids = new ArrayList<>();
            for (SettlementBatchItem item : claimed) {
                item.setStatus("PROCESSING");
                item.setRetryCount(item.getRetryCount() + 1);
                item.setUpdatedAt(Instant.now());
                itemRepository.save(item);
                ids.add(item.getId());
            }
            return ids;
        });
    }

    /**
     * Individual item execution in its own transactional boundary.
     * Prevents single item errors from rolling back the entire batch.
     */
    public void processSingleItem(UUID itemId) {
        transactionTemplate.executeWithoutResult(status -> doProcessSingleItem(itemId));
    }

    private void doProcessSingleItem(UUID itemId) {
        SettlementBatchItem item = itemRepository.findById(itemId).orElse(null);
        if (item == null) {
            return;
        }

        // Idempotency: If already settled, do not re-process
        if ("SETTLED".equals(item.getStatus())) {
            return;
        }

        Transaction tx = transactionRepository.findByTransactionId(item.getTransactionId()).orElse(null);
        if (tx == null) {
            item.setStatus("FAILED");
            item.setErrorMessage("Transaction not found in database: " + item.getTransactionId());
            item.setProcessedAt(Instant.now());
            itemRepository.save(item);
            dftpMetrics.recordSettlementFailed("TRANSACTION_NOT_FOUND");
            return;
        }

        // Handle transaction state
        if ("COMPLETED".equals(tx.getStatus())) {
            // Already completed in Saga
            item.setStatus("SETTLED");
            item.setProcessedAt(Instant.now());
            itemRepository.save(item);
            dftpMetrics.recordSettlementSuccess();
            log.info("Batch item {} (tx: {}) marked SETTLED (already completed in saga)", item.getId(), tx.getTransactionId());
            return;
        }

        if ("LEDGER_POSTED".equals(tx.getStatus())) {
            // Transition to COMPLETED and emit FundsSettlementRequested event
            tx.setStatus("COMPLETED");
            transactionRepository.save(tx);

            FundsSettlementRequested payload = FundsSettlementRequested.builder()
                    .sourceAccountId(tx.getSourceAccountId())
                    .destinationAccountId(tx.getDestinationAccountId())
                    .amount(tx.getAmount())
                    .currency(tx.getCurrency())
                    .build();

            EventEnvelope<FundsSettlementRequested> envelope = EventEnvelope.<FundsSettlementRequested>builder()
                    .eventId(UUID.randomUUID())
                    .eventType("FundsSettlementRequested")
                    .eventVersion("1.0")
                    .occurredAt(Instant.now())
                    .correlationId(UUID.randomUUID().toString())
                    .transactionId(tx.getTransactionId())
                    .causationId(item.getId().toString())
                    .producerService("transaction-service")
                    .payload(payload)
                    .build();

            try {
                OutboxEvent outboxEvent = OutboxEvent.builder()
                        .id(envelope.getEventId())
                        .aggregateType("transaction-events")
                        .aggregateId(tx.getId().toString())
                        .eventType(envelope.getEventType())
                        .payload(objectMapper.writeValueAsString(envelope))
                        .status("PENDING")
                        .build();
                outboxEventRepository.save(outboxEvent);
            } catch (Exception e) {
                log.error("Failed to serialize outbox event for batch settlement tx {}", tx.getTransactionId(), e);
            }

            item.setStatus("SETTLED");
            item.setProcessedAt(Instant.now());
            itemRepository.save(item);

            dftpMetrics.recordSettlementSuccess();
            log.info("Batch item {} (tx: {}) successfully settled", item.getId(), tx.getTransactionId());
            return;
        }

        if ("FAILED".equals(tx.getStatus()) || "COMPENSATING".equals(tx.getStatus())) {
            item.setStatus("SKIPPED");
            item.setErrorMessage("Transaction in terminal non-settleable status: " + tx.getStatus());
            item.setProcessedAt(Instant.now());
            itemRepository.save(item);
            dftpMetrics.recordSettlementFailed("NON_SETTLEABLE_STATUS");
            log.warn("Batch item {} (tx: {}) skipped due to status {}", item.getId(), tx.getTransactionId(), tx.getStatus());
            return;
        }

        // Any other state (PENDING, SOURCE_HELD)
        item.setStatus("FAILED");
        item.setErrorMessage("Transaction not yet ledger-posted. Current status: " + tx.getStatus());
        item.setProcessedAt(Instant.now());
        itemRepository.save(item);
        dftpMetrics.recordSettlementFailed("NOT_LEDGER_POSTED");
        log.warn("Batch item {} (tx: {}) failed due to premature status {}", item.getId(), tx.getTransactionId(), tx.getStatus());
    }

    /**
     * Finalizes the batch execution, updating counters and terminal status.
     * Retries on concurrent optimistic locking collisions.
     */
    public BatchResponse finalizeBatchExecution(String batchId) {
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                return transactionTemplate.execute(status -> doFinalizeBatchExecution(batchId));
            } catch (org.springframework.dao.ConcurrencyFailureException e) {
                log.debug("Concurrent finalize collision for batch {}, retry attempt {}", batchId, attempt);
                try {
                    Thread.sleep(30L * (attempt + 1));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                SettlementBatch b = batchRepository.findByBatchId(batchId).orElse(null);
                if (b != null && ("COMPLETED".equals(b.getStatus()) || "PARTIAL_FAILURE".equals(b.getStatus()) || "FAILED".equals(b.getStatus()))) {
                    return getBatch(batchId);
                }
            }
        }
        return getBatch(batchId);
    }

    private BatchResponse doFinalizeBatchExecution(String batchId) {
        SettlementBatch batch = batchRepository.findByBatchId(batchId)
                .orElseThrow(() -> new IllegalArgumentException("Batch not found: " + batchId));

        int settledCount = (int) itemRepository.countByBatchIdAndStatus(batchId, "SETTLED");
        int failedCount = (int) itemRepository.countByBatchIdAndStatus(batchId, "FAILED");
        int skippedCount = (int) itemRepository.countByBatchIdAndStatus(batchId, "SKIPPED");
        int pendingCount = (int) itemRepository.countByBatchIdAndStatus(batchId, "PENDING");
        int processingCount = (int) itemRepository.countByBatchIdAndStatus(batchId, "PROCESSING");

        // If batch is already finalized into terminal status and no items pending or in-flight, return directly without saving
        if (("COMPLETED".equals(batch.getStatus()) || "PARTIAL_FAILURE".equals(batch.getStatus()) || "FAILED".equals(batch.getStatus()))
                && pendingCount == 0 && processingCount == 0) {
            List<SettlementBatchItem> items = itemRepository.findByBatchId(batchId);
            return mapToResponse(batch, items);
        }

        int totalFailures = failedCount + skippedCount;
        batch.setSuccessCount(settledCount);
        batch.setFailureCount(totalFailures);

        if (pendingCount == 0 && processingCount == 0) {
            if (totalFailures == 0) {
                batch.setStatus("COMPLETED");
            } else if (settledCount > 0) {
                batch.setStatus("PARTIAL_FAILURE");
            } else {
                batch.setStatus("FAILED");
            }
            batch.setCompletedAt(Instant.now());

            if (batch.getStartedAt() != null) {
                Duration duration = Duration.between(batch.getStartedAt(), batch.getCompletedAt());
                dftpMetrics.recordBatchDuration("SETTLEMENT", batch.getStatus(), duration);
            }
            if ("COMPLETED".equals(batch.getStatus())) {
                dftpMetrics.recordBatchCompleted("SETTLEMENT");
            } else {
                dftpMetrics.recordBatchFailed("SETTLEMENT", batch.getStatus());
            }
            dftpMetrics.recordBatchItemsSuccess("SETTLEMENT", settledCount);
            if (totalFailures > 0) {
                dftpMetrics.recordBatchItemsFailed("SETTLEMENT", "ITEMS_UNSETTLED", totalFailures);
            }
        }

        batch = batchRepository.save(batch);
        List<SettlementBatchItem> items = itemRepository.findByBatchId(batchId);
        log.info("Batch {} finalized: status={}, settled={}, failed={}, pending={}",
                batchId, batch.getStatus(), settledCount, totalFailures, pendingCount);

        return mapToResponse(batch, items);
    }

    /**
     * Safely retries failed or skipped items in a batch.
     * Already settled items are NEVER re-processed.
     */
    @Transactional
    public BatchResponse retryBatch(String batchId) {
        SettlementBatch batch = batchRepository.findByBatchId(batchId)
                .orElseThrow(() -> new IllegalArgumentException("Batch not found: " + batchId));

        if ("COMPLETED".equals(batch.getStatus())) {
            log.info("Batch {} already COMPLETED, nothing to retry", batchId);
            return getBatch(batchId);
        }

        List<SettlementBatchItem> failedItems = itemRepository.findByBatchId(batchId).stream()
                .filter(i -> "FAILED".equals(i.getStatus()) || "PROCESSING".equals(i.getStatus()))
                .collect(Collectors.toList());

        for (SettlementBatchItem item : failedItems) {
            item.setStatus("PENDING");
            item.setErrorMessage(null);
            item.setUpdatedAt(Instant.now());
        }
        itemRepository.saveAll(failedItems);

        batch.setStatus("RETRYING");
        batch.setRetryCount(batch.getRetryCount() + 1);
        batchRepository.save(batch);

        dftpMetrics.recordBatchRetry("SETTLEMENT");
        log.info("Retrying batch {}: reset {} failed items to PENDING", batchId, failedItems.size());

        return executeBatch(batchId);
    }

    /**
     * Worker crash recovery scanner.
     * Sweeps items left in PROCESSING past the recovery cutoff.
     */
    @Transactional
    public int recoverStalledItems(Duration timeout) {
        Instant cutoff = Instant.now().minus(timeout != null ? timeout : Duration.ofMinutes(2));
        List<SettlementBatchItem> stalled = itemRepository.claimStalledProcessingItems(cutoff, MAX_PAGE_SIZE);
        for (SettlementBatchItem item : stalled) {
            log.warn("Recovering stalled batch item {} (tx: {}) in status PROCESSING since {}",
                    item.getId(), item.getTransactionId(), item.getUpdatedAt());
            item.setStatus("PENDING");
            item.setErrorMessage("Recovered from worker crash / timeout");
            item.setUpdatedAt(Instant.now());
        }
        itemRepository.saveAll(stalled);
        return stalled.size();
    }

    @Transactional(readOnly = true)
    public BatchResponse getBatch(String batchId) {
        SettlementBatch batch = batchRepository.findByBatchId(batchId)
                .orElseThrow(() -> new IllegalArgumentException("Batch not found: " + batchId));
        List<SettlementBatchItem> items = itemRepository.findByBatchId(batchId);
        return mapToResponse(batch, items);
    }

    private BatchResponse mapToResponse(SettlementBatch batch, List<SettlementBatchItem> items) {
        List<BatchItemResponse> itemResponses = items != null ? items.stream()
                .map(i -> BatchItemResponse.builder()
                        .id(i.getId())
                        .batchId(i.getBatchId())
                        .transactionId(i.getTransactionId())
                        .sourceAccountId(i.getSourceAccountId())
                        .destinationAccountId(i.getDestinationAccountId())
                        .amount(i.getAmount())
                        .currency(i.getCurrency())
                        .status(i.getStatus())
                        .retryCount(i.getRetryCount())
                        .errorMessage(i.getErrorMessage())
                        .processedAt(i.getProcessedAt())
                        .createdAt(i.getCreatedAt())
                        .build())
                .collect(Collectors.toList()) : List.of();

        return BatchResponse.builder()
                .batchId(batch.getBatchId())
                .status(batch.getStatus())
                .totalItems(batch.getTotalItems())
                .successCount(batch.getSuccessCount())
                .failureCount(batch.getFailureCount())
                .retryCount(batch.getRetryCount())
                .createdAt(batch.getCreatedAt())
                .startedAt(batch.getStartedAt())
                .completedAt(batch.getCompletedAt())
                .createdBy(batch.getCreatedBy())
                .items(itemResponses)
                .build();
    }
}
