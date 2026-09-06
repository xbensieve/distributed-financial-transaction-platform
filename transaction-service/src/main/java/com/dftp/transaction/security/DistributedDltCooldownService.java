package com.dftp.transaction.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

/**
 * Distributed DLT Replay Cooldown enforcement backed by PostgreSQL.
 * Ensures that across any number of Transaction Service replicas,
 * rapid replay requests on the same topic are globally throttled.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DistributedDltCooldownService {

    private final JdbcTemplate jdbcTemplate;
    public static final Duration REPLAY_COOLDOWN = Duration.ofSeconds(3);

    @Transactional
    public boolean acquireReplaySlot(String topic, String instanceId) {
        Instant now = Instant.now();
        Instant cutoff = now.minus(REPLAY_COOLDOWN);

        // 1. Try insert if topic record does not exist
        int inserted = jdbcTemplate.update(
                "INSERT INTO dlt_replay_cooldown (topic, last_replay_at, locked_by) VALUES (?, ?, ?) ON CONFLICT (topic) DO NOTHING",
                topic, Timestamp.from(now), instanceId
        );
        if (inserted > 0) {
            log.info("Distributed replay slot acquired (initial) for topic '{}' by replica '{}'", topic, instanceId);
            return true;
        }

        // 2. Atomically update only if the cooldown window has elapsed
        int updated = jdbcTemplate.update(
                "UPDATE dlt_replay_cooldown SET last_replay_at = ?, locked_by = ? WHERE topic = ? AND last_replay_at <= ?",
                Timestamp.from(now), instanceId, topic, Timestamp.from(cutoff)
        );

        if (updated > 0) {
            log.info("Distributed replay slot acquired (updated) for topic '{}' by replica '{}'", topic, instanceId);
            return true;
        } else {
            log.warn("Distributed replay cooldown active for topic '{}'. Rejected request from replica '{}'", topic, instanceId);
            return false;
        }
    }
}
