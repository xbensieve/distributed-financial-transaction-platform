package com.dftp.ledger.application;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.event.payload.LedgerPostRequested;
import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.ledger.domain.AccountReferenceRepository;
import com.dftp.ledger.domain.LedgerTransaction;
import com.dftp.ledger.domain.LedgerTransactionRepository;
import com.dftp.ledger.domain.Posting;
import com.dftp.ledger.domain.PostingRepository;
import com.dftp.ledger.domain.PostingType;
import com.dftp.ledger.domain.event.LedgerTransactionPosted;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.dftp.common.observability.DftpMetrics;
import com.dftp.common.observability.TraceContextPropagator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class LedgerApplicationService {

    private final LedgerTransactionRepository ledgerTransactionRepository;
    private final PostingRepository postingRepository;
    private final AccountReferenceRepository accountReferenceRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Autowired(required = false)
    private DftpMetrics dftpMetrics = new DftpMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());

    @Transactional
    public void postTransaction(EventEnvelope<LedgerPostRequested> envelope) {
        LedgerPostRequested payload = envelope.getPayload();
        String businessTransactionId = envelope.getTransactionId();

        if (ledgerTransactionRepository.findByBusinessTransactionId(businessTransactionId).isPresent()) {
            log.info("Ledger transaction already exists for businessTransactionId: {}", businessTransactionId);
            return;
        }

        accountReferenceRepository.findByAccountId(payload.getSourceAccountId())
                .orElseThrow(() -> new RuntimeException("Source account reference not found for accountId: " + payload.getSourceAccountId() + ". Retrying..."));
                
        accountReferenceRepository.findByAccountId(payload.getDestinationAccountId())
                .orElseThrow(() -> new RuntimeException("Destination account reference not found for accountId: " + payload.getDestinationAccountId() + ". Retrying..."));


        BigDecimal amount = payload.getAmount();

        UUID ledgerTransactionId = UUID.randomUUID();
        LedgerTransaction ledgerTransaction = LedgerTransaction.builder()
                .id(ledgerTransactionId)
                .ledgerTransactionId("LT-" + ledgerTransactionId)
                .businessTransactionId(businessTransactionId)
                .status("POSTED")
                .build();
        try {
            ledgerTransaction = ledgerTransactionRepository.saveAndFlush(ledgerTransaction);
        } catch (DataIntegrityViolationException e) {
            log.info("Concurrent duplicate transaction detected for businessTransactionId: {}, ignoring.", businessTransactionId);
            return;
        }

        Posting debitPosting = Posting.builder()
                .id(UUID.randomUUID())
                .ledgerTransaction(ledgerTransaction)
                .accountId(payload.getSourceAccountId())
                .amount(amount)
                .currency(payload.getCurrency())
                .postingType(PostingType.DEBIT)
                .build();

        Posting creditPosting = Posting.builder()
                .id(UUID.randomUUID())
                .ledgerTransaction(ledgerTransaction)
                .accountId(payload.getDestinationAccountId())
                .amount(amount)
                .currency(payload.getCurrency())
                .postingType(PostingType.CREDIT)
                .build();

        postingRepository.save(debitPosting);
        postingRepository.save(creditPosting);

        BigDecimal totalDebits = debitPosting.getAmount();
        BigDecimal totalCredits = creditPosting.getAmount();
        if (totalDebits.compareTo(totalCredits) != 0) {
            dftpMetrics.recordLedgerPostRejected("DOUBLE_ENTRY_VIOLATED");
            throw new IllegalStateException("Double-entry invariant violated: debits (" + totalDebits + ") != credits (" + totalCredits + ")");
        }

        try {
            LedgerTransactionPosted outboxPayload = LedgerTransactionPosted.builder()
                    .ledgerTransactionId(ledgerTransaction.getLedgerTransactionId())
                    .businessTransactionId(businessTransactionId)
                    .amount(amount)
                    .currency(payload.getCurrency())
                    .postedAt(ledgerTransaction.getCreatedAt())
                    .build();

            EventEnvelope<LedgerTransactionPosted> outboxEnvelope = EventEnvelope.<LedgerTransactionPosted>builder()
                    .eventId(UUID.randomUUID())
                    .transactionId(envelope.getTransactionId())
                    .correlationId(envelope.getCorrelationId())
                    .causationId(envelope.getEventId().toString())
                    .eventType("LedgerTransactionPosted")
                    .eventVersion("1.0")
                    .occurredAt(Instant.now())
                    .producerService("ledger-service")
                    .payload(outboxPayload)
                    .build();

            var traceMetadata = TraceContextPropagator.currentTraceMetadata().orElse(null);
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .id(outboxEnvelope.getEventId())
                    .aggregateType("ledger-events")
                    .aggregateId(ledgerTransaction.getId().toString())
                    .eventType(outboxEnvelope.getEventType())
                    .status("PENDING")
                    .payload(objectMapper.writeValueAsString(outboxEnvelope))
                    .traceparent(traceMetadata != null ? traceMetadata.traceparent() : null)
                    .tracestate(traceMetadata != null ? traceMetadata.tracestate() : null)
                    .build();

            outboxEventRepository.save(outboxEvent);
            dftpMetrics.recordLedgerPostSuccess();
            log.info("Successfully posted ledger transaction {} for business transaction {}", ledgerTransaction.getLedgerTransactionId(), businessTransactionId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize outbox event", e);
        }
    }
}
