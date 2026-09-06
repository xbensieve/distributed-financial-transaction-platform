package com.dftp.transaction.api;

import com.dftp.common.security.JwtTokenService;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import com.dftp.transaction.application.DltReplayService;
import com.dftp.transaction.security.DistributedDltCooldownService;
import com.dftp.transaction.security.SecurityAuditLog;
import com.dftp.transaction.security.SecurityAuditLogRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class DistributedDltCooldownTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JwtTokenService jwtTokenService;

    @Autowired
    private SecurityAuditLogRepository securityAuditLogRepository;

    @Autowired
    private DistributedDltCooldownService distributedDltCooldownService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @MockBean
    private DltReplayService dltReplayService;

    @BeforeEach
    void setUp() {
        securityAuditLogRepository.deleteAll();
        jdbcTemplate.update("DELETE FROM dlt_replay_cooldown");
    }

    @Test
    @DisplayName("DIST-DLT-01: Two simulated replicas competing concurrently on the same topic — only one acquires slot")
    void testConcurrentReplicasCompetingOnSameTopic() throws Exception {
        int threads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch readyLatch = new CountDownLatch(threads);
        CountDownLatch startLatch = new CountDownLatch(1);

        AtomicInteger acquiredCount = new AtomicInteger(0);
        AtomicInteger rejectedCount = new AtomicInteger(0);
        List<String> lockedByInstances = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < threads; i++) {
            final String replicaId = "replica-instance-" + i;
            executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    boolean acquired = distributedDltCooldownService.acquireReplaySlot("account-events", replicaId);
                    if (acquired) {
                        acquiredCount.incrementAndGet();
                        lockedByInstances.add(replicaId);
                    } else {
                        rejectedCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        readyLatch.await(5, TimeUnit.SECONDS);
        startLatch.countDown();
        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        // Exactly 1 replica must win the slot, 9 must be rejected
        assertThat(acquiredCount.get()).isEqualTo(1);
        assertThat(rejectedCount.get()).isEqualTo(9);
        assertThat(lockedByInstances).hasSize(1);
    }

    @Test
    @DisplayName("DIST-DLT-02: HTTP API multi-replica replay request — replica 1 succeeds (200), replica 2 blocked (429)")
    void testMultiReplicaHttpReplayEnforcement() throws Exception {
        String adminToken = jwtTokenService.generateToken("admin-operator", List.of("ROLE_ADMIN"));
        when(dltReplayService.replayDlt(anyString(), anyString(), anyInt())).thenReturn(3);

        // Simulated Replica A requests replay -> 200 OK
        mockMvc.perform(post("/admin/dlt/replay/transaction-events")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .header("X-Correlation-Id", "corr-replica-A"))
                .andExpect(status().isOk());

        // Simulated Replica B requests replay within cooldown window -> 429 Too Many Requests
        mockMvc.perform(post("/admin/dlt/replay/transaction-events")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .header("X-Correlation-Id", "corr-replica-B"))
                .andExpect(status().isTooManyRequests());

        // Verify audit logs captured both actions with appropriate statuses
        List<SecurityAuditLog> auditLogs = securityAuditLogRepository.findByTargetResourceOrderByCreatedAtDesc("transaction-events");
        assertThat(auditLogs).hasSize(2);
        assertThat(auditLogs.stream().map(SecurityAuditLog::getStatus))
                .containsExactlyInAnyOrder("SUCCESS", "REJECTED_COOLDOWN_ACTIVE");
    }

    @Test
    @DisplayName("DIST-DLT-03: Independent topics do not block each other across replicas")
    void testIndependentTopicsDoNotBlock() {
        boolean acquiredAccount = distributedDltCooldownService.acquireReplaySlot("account-events", "replica-1");
        boolean acquiredLedger = distributedDltCooldownService.acquireReplaySlot("ledger-events", "replica-2");
        boolean acquiredTransaction = distributedDltCooldownService.acquireReplaySlot("transaction-events", "replica-3");

        assertThat(acquiredAccount).isTrue();
        assertThat(acquiredLedger).isTrue();
        assertThat(acquiredTransaction).isTrue();

        // Re-attempting any of them immediately must fail
        assertThat(distributedDltCooldownService.acquireReplaySlot("account-events", "replica-2")).isFalse();
        assertThat(distributedDltCooldownService.acquireReplaySlot("ledger-events", "replica-1")).isFalse();
    }

    @Test
    @DisplayName("DIST-DLT-04: After cooldown window expires, next replica can acquire replay slot")
    void testCooldownExpirationPermitsSubsequentReplay() {
        boolean firstAcquisition = distributedDltCooldownService.acquireReplaySlot("ledger-events", "replica-A");
        assertThat(firstAcquisition).isTrue();

        // Manually simulate expiration by setting last_replay_at to 5 seconds ago in the database
        Instant past = Instant.now().minusSeconds(5);
        jdbcTemplate.update("UPDATE dlt_replay_cooldown SET last_replay_at = ? WHERE topic = ?",
                Timestamp.from(past), "ledger-events");

        // Replica B now attempts acquisition -> must succeed!
        boolean secondAcquisition = distributedDltCooldownService.acquireReplaySlot("ledger-events", "replica-B");
        assertThat(secondAcquisition).isTrue();
    }
}
