package com.dftp.common.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Standard event envelope for all async messages across the system.
 * Distinguishes between Message Identity (eventId) and Business Operation Identity (correlationId / payload content).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class EventEnvelope<T> {

    /**
     * Unique identifier for this specific message emission.
     * Used for Inbox deduplication to guarantee single-business-effect processing.
     */
    private UUID eventId;

    /**
     * The type of the event (e.g., "FundsHeldEvent").
     * Used for routing and deserialization.
     */
    private String eventType;

    /**
     * Version of the event schema for evolution.
     */
    private String eventVersion;

    /**
     * When the event actually occurred in the domain.
     */
    private Instant occurredAt;

    /**
     * Used to trace an entire Saga or business workflow across multiple services.
     */
    private String correlationId;

    /**
     * The business operation identity. Used to deduplicate repeated business intents.
     * Different from eventId which deduplicates infrastructure network deliveries.
     */
    private String transactionId;

    /**
     * The eventId of the message that caused this event to be generated.
     */
    private String causationId;

    /**
     * The service that produced this event.
     */
    private String producerService;

    /**
     * The actual business payload (must not contain generic envelope fields).
     */
    private T payload;
}
