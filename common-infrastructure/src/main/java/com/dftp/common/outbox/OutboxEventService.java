package com.dftp.common.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxEventService {

    private final OutboxEventRepository outboxEventRepository;
    private final OutboxRelayProperties properties;

    /**
     * Claims up to a batch size of events for processing.
     * Uses FOR UPDATE SKIP LOCKED to prevent concurrent relay instances from claiming the same rows.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<OutboxEvent> claimEventsForPublishing() {
        List<OutboxEvent> events = outboxEventRepository.findEventsForProcessing(properties.getBatchSize());
        for (OutboxEvent event : events) {
            event.setStatus("CLAIMED");
            // updatedAt is automatically set by @PreUpdate
        }
        if (!events.isEmpty()) {
            outboxEventRepository.saveAll(events);
        }
        return events;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markPublished(UUID eventId) {
        outboxEventRepository.findById(eventId).ifPresent(event -> {
            event.setStatus("PUBLISHED");
            event.setProcessedAt(Instant.now());
            outboxEventRepository.save(event);
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markFailed(UUID eventId, String errorMessage) {
        outboxEventRepository.findById(eventId).ifPresent(event -> {
            // Keep status as PENDING or CLAIMED if we want it to be picked up again immediately?
            // Wait, if it fails, the relay handles retry, but if it ultimately fails Kafka send locally, 
            // we should set it to PENDING so it can be retried in the next polling cycle.
            event.setStatus("PENDING");
            event.setErrorMessage(errorMessage != null && errorMessage.length() > 255 
                    ? errorMessage.substring(0, 255) 
                    : errorMessage);
            outboxEventRepository.save(event);
        });
    }
}
