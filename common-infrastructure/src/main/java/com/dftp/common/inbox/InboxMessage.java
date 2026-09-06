package com.dftp.common.inbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "inbox_messages")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InboxMessage implements Persistable<UUID> {

    @Id
    @Column(name = "event_id")
    private UUID eventId; // Message Identity, guarantees idempotency at the DB level

    @Column(name = "consumer_group", nullable = false)
    private String consumerGroup;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "status", nullable = false)
    @Builder.Default
    private String status = "PROCESSED";

    @Column(name = "received_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant receivedAt = Instant.now();

    @Transient
    @Override
    public UUID getId() {
        return eventId;
    }

    @Transient
    @Override
    public boolean isNew() {
        return true; // Force INSERT, allowing DB to throw constraint violation on duplicate
    }
}
