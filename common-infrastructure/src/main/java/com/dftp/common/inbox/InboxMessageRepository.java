package com.dftp.common.inbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface InboxMessageRepository extends JpaRepository<InboxMessage, UUID> {
    boolean existsByEventId(UUID eventId);

    /**
     * Atomically inserts an inbox message record using ON CONFLICT (event_id) DO NOTHING.
     * Guarantees atomic message deduplication at the database level, eliminating check-then-act race conditions.
     *
     * @return 1 if newly inserted (caller owns processing); 0 if already present (duplicate).
     */
    @Modifying
    @Query(value = "INSERT INTO inbox_messages (event_id, consumer_group, event_type, status, received_at) " +
                   "VALUES (:eventId, :consumerGroup, :eventType, 'PROCESSED', NOW()) " +
                   "ON CONFLICT (event_id) DO NOTHING", nativeQuery = true)
    int insertIfAbsent(@Param("eventId") UUID eventId,
                       @Param("consumerGroup") String consumerGroup,
                       @Param("eventType") String eventType);
}
