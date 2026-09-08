package com.dftp.common.outbox;

import com.dftp.common.TestInfrastructureApplication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = TestInfrastructureApplication.class)
@Testcontainers(disabledWithoutDocker = true)
class MultiInstanceOutboxRelayIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("dftp_test")
            .withUsername("postgres")
            .withPassword("postgres");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("spring.flyway.baseline-on-migrate", () -> "true");
        registry.add("spring.flyway.locations", () -> "classpath:db/migration-technical");
        registry.add("dftp.outbox.relay.batch-size", () -> "10");
        registry.add("spring.kafka.bootstrap-servers", () -> "localhost:9092");
    }

    @Autowired
    private OutboxEventService outboxEventService;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void cleanDatabase() {
        outboxEventRepository.deleteAll();
    }

    @Test
    @DisplayName("Multiple concurrent worker instances polling outbox with SKIP LOCKED must never claim duplicate records")
    void testConcurrentWorkerOutboxPolling() throws Exception {
        final int TOTAL_EVENTS = 100;
        final int WORKER_THREADS = 4;

        // Populate 100 PENDING outbox events
        List<OutboxEvent> initialEvents = new ArrayList<>();
        for (int i = 0; i < TOTAL_EVENTS; i++) {
            initialEvents.add(OutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("Transaction")
                    .aggregateId("tx-" + i)
                    .eventType("TransactionCreated")
                    .payload("{\"seq\":" + i + "}")
                    .status("PENDING")
                    .createdAt(Instant.now().minusSeconds(TOTAL_EVENTS - i))
                    .build());
        }
        outboxEventRepository.saveAll(initialEvents);
        assertThat(outboxEventRepository.count()).isEqualTo(TOTAL_EVENTS);

        // Run concurrent workers
        ExecutorService executor = Executors.newFixedThreadPool(WORKER_THREADS);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(WORKER_THREADS);

        ConcurrentHashMap<UUID, String> claimedEventRegistry = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, AtomicInteger> workerClaimCounts = new ConcurrentHashMap<>();
        List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        for (int w = 0; w < WORKER_THREADS; w++) {
            final String workerId = "worker-instance-" + w;
            workerClaimCounts.put(workerId, new AtomicInteger(0));

            executor.submit(() -> {
                try {
                    startLatch.await(); // Synchronize thread start
                    while (true) {
                        List<OutboxEvent> claimed = outboxEventService.claimEventsForPublishing(workerId);
                        if (claimed.isEmpty()) {
                            // Verify if all events in DB are claimed
                            long pendingCount = outboxEventRepository.countByStatus("PENDING");
                            if (pendingCount == 0) {
                                break;
                            }
                            Thread.sleep(10);
                            continue;
                        }

                        for (OutboxEvent ev : claimed) {
                            String previousOwner = claimedEventRegistry.putIfAbsent(ev.getId(), workerId);
                            if (previousOwner != null) {
                                throw new IllegalStateException("DUPLICATE CLAIM DETECTED! Event " + ev.getId()
                                        + " was claimed by both " + previousOwner + " and " + workerId);
                            }
                            workerClaimCounts.get(workerId).incrementAndGet();
                        }
                    }
                } catch (Throwable t) {
                    exceptions.add(t);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertThat(completed).isTrue();
        assertThat(exceptions).isEmpty();

        // Verifications
        assertThat(claimedEventRegistry.size()).isEqualTo(TOTAL_EVENTS);
        long pendingRemaining = outboxEventRepository.countByStatus("PENDING");
        long claimedInDb = outboxEventRepository.countByStatus("CLAIMED");

        assertThat(pendingRemaining).isEqualTo(0);
        assertThat(claimedInDb).isEqualTo(TOTAL_EVENTS);

        // Verify multiple workers participated (no single worker monopolized everything)
        int participatingWorkers = 0;
        for (Map.Entry<String, AtomicInteger> entry : workerClaimCounts.entrySet()) {
            if (entry.getValue().get() > 0) {
                participatingWorkers++;
            }
        }
        assertThat(participatingWorkers).isGreaterThan(1);

        // Verify all rows in PostgreSQL have claimed_by populated
        List<String> claimedByList = jdbcTemplate.query(
                "SELECT claimed_by FROM outbox_events WHERE status = 'CLAIMED'",
                (rs, rowNum) -> rs.getString("claimed_by")
        );
        assertThat(claimedByList).hasSize(TOTAL_EVENTS);
        assertThat(claimedByList).doesNotContainNull();
    }

    @Test
    @DisplayName("Stale claimed events older than 2 minutes must be safely reclaimed by active workers")
    void testStaleClaimedEventRecovery() {
        UUID staleEventId = UUID.randomUUID();
        // Insert a CLAIMED event with updated_at 3 minutes in the past (simulating crashed worker)
        jdbcTemplate.update(
                "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, status, created_at, updated_at, claimed_by) " +
                "VALUES (?, 'Transaction', 'tx-stale-1', 'StaleEvent', '{}', 'CLAIMED', NOW() - INTERVAL '5 MINUTES', NOW() - INTERVAL '3 MINUTES', 'dead-worker-99')",
                staleEventId
        );

        List<OutboxEvent> reclaimed = outboxEventService.claimEventsForPublishing("recovery-worker-1");

        assertThat(reclaimed).isNotEmpty();
        assertThat(reclaimed.stream().anyMatch(e -> e.getId().equals(staleEventId))).isTrue();

        OutboxEvent recovered = outboxEventRepository.findById(staleEventId).orElseThrow();
        assertThat(recovered.getClaimedBy()).isEqualTo("recovery-worker-1");
        assertThat(recovered.getStatus()).isEqualTo("CLAIMED");
    }
}
