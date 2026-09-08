package com.dftp.common.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {
    
    // For Polling-based Outbox Relay
    // Claims PENDING events, or CLAIMED events that haven't been updated in 2 minutes (crash recovery)
    @Query(value = "SELECT * FROM outbox_events WHERE status = 'PENDING' OR (status = 'CLAIMED' AND updated_at < (NOW() - INTERVAL '2 MINUTES')) ORDER BY created_at ASC LIMIT :batchSize FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<OutboxEvent> findEventsForProcessing(int batchSize);

    long countByStatus(String status);

    boolean existsByStatusAndCreatedAtBefore(String status, java.time.Instant cutoff);

    List<OutboxEvent> findByStatusAndCreatedAtBefore(String status, java.time.Instant cutoff);

    List<OutboxEvent> findByAggregateTypeAndAggregateIdOrderByCreatedAtAsc(String aggregateType, String aggregateId);
}
