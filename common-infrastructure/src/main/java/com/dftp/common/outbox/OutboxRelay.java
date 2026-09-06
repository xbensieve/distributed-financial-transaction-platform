package com.dftp.common.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@EnableScheduling
@RequiredArgsConstructor
@ConditionalOnProperty(name = "dftp.outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private final OutboxEventService outboxEventService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelayString = "${dftp.outbox.relay.polling-interval:1000}")
    public void processOutbox() {
        List<OutboxEvent> eventsToPublish;
        
        try {
            // Transaction 1: Claim
            eventsToPublish = outboxEventService.claimEventsForPublishing();
        } catch (Exception e) {
            log.error("Failed to claim outbox events", e);
            return;
        }

        if (eventsToPublish.isEmpty()) {
            return;
        }
        
        log.debug("Claimed {} outbox events for publishing", eventsToPublish.size());

        for (OutboxEvent event : eventsToPublish) {
            try {
                // Publish to Kafka (Network call)
                // We use String keys and values because the payload field is a JSONB String.
                // However, wait: the requirement says we should use EventEnvelope<T> deserialization safely.
                // If the event.payload is already a JSON string of the EventEnvelope, we just send it as a String.
                kafkaTemplate.send(event.getAggregateType(), event.getAggregateId(), event.getPayload())
                        .get(5, TimeUnit.SECONDS);

                // Transaction 2: Mark Published
                outboxEventService.markPublished(event.getId());

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                outboxEventService.markFailed(event.getId(), "Interrupted during send");
                log.error("Interrupted while sending event {}", event.getId());
                break; // Stop processing this batch
            } catch (ExecutionException | TimeoutException e) {
                log.error("Failed to send event {} to Kafka", event.getId(), e);
                outboxEventService.markFailed(event.getId(), e.getMessage());
            } catch (Exception e) {
                log.error("Unexpected error sending event {}", event.getId(), e);
                outboxEventService.markFailed(event.getId(), e.getMessage());
            }
        }
    }
}
