package com.dftp.common.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.dftp.common.observability.TraceContextPropagator;
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
    private final String workerId = "outbox-relay-" + java.util.UUID.randomUUID().toString().substring(0, 8);

    @Scheduled(fixedDelayString = "${dftp.outbox.relay.polling-interval:1000}")
    public void processOutbox() {
        List<OutboxEvent> eventsToPublish;
        String currentWorker = workerId + "-" + Thread.currentThread().getName();
        
        try {
            // Transaction 1: Claim with FOR UPDATE SKIP LOCKED
            eventsToPublish = outboxEventService.claimEventsForPublishing(currentWorker);
        } catch (Exception e) {
            log.error("Failed to claim outbox events for worker {}", currentWorker, e);
            return;
        }

        if (eventsToPublish.isEmpty()) {
            return;
        }
        
        log.debug("Worker {} claimed {} outbox events for publishing", currentWorker, eventsToPublish.size());

        for (OutboxEvent event : eventsToPublish) {
            TraceContextPropagator.TraceMetadata traceMetadata = (event.getTraceparent() != null)
                    ? TraceContextPropagator.parseTraceparent(event.getTraceparent(), event.getTracestate(), null)
                            .orElseGet(TraceContextPropagator.TraceMetadata::createNew)
                    : TraceContextPropagator.TraceMetadata.createNew();

            TraceContextPropagator.populateMdc(traceMetadata);
            org.slf4j.MDC.put("eventId", event.getId().toString());

            try {
                // Publish to Kafka with W3C distributed trace headers
                org.apache.kafka.clients.producer.ProducerRecord<String, String> record =
                        new org.apache.kafka.clients.producer.ProducerRecord<>(
                                event.getAggregateType(),
                                event.getAggregateId(),
                                event.getPayload()
                        );

                TraceContextPropagator.injectIntoKafkaHeaders(record.headers(), traceMetadata);
                if (event.getEventType() != null) {
                    record.headers().add(new org.apache.kafka.common.header.internals.RecordHeader(
                            "eventType", event.getEventType().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                }
                record.headers().add(new org.apache.kafka.common.header.internals.RecordHeader(
                        "eventId", event.getId().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)));

                kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

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
            } finally {
                TraceContextPropagator.clearMdc();
                org.slf4j.MDC.remove("eventId");
            }
        }
    }
}
