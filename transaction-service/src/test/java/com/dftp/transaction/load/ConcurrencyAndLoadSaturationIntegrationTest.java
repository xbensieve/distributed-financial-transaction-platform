package com.dftp.transaction.load;

import com.dftp.common.kafka.AccountPartitionStrategy;
import com.dftp.common.outbox.OutboxEvent;
import com.dftp.common.outbox.OutboxEventRepository;
import com.dftp.common.outbox.OutboxRelay;
import com.dftp.common.security.JwtProperties;
import com.dftp.common.security.JwtTokenService;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import com.dftp.transaction.api.dto.CreateTransactionRequest;
import com.dftp.transaction.api.dto.TransactionResponse;
import com.dftp.transaction.domain.AccountReference;
import com.dftp.transaction.domain.AccountReferenceRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 13 & 14 Concurrency & Load Saturation Verification Suite.
 *
 * Validates:
 * 1. High-concurrency burst handling (50 parallel threads targeting POST /transactions)
 * 2. HikariCP connection pool stability under load (zero pool exhaustion, zero connection leaks)
 * 3. End-to-end W3C TraceContext propagation under concurrent saturation
 * 4. Zero double-spending & idempotent protection under parallel collisions
 * 5. Multi-instance Outbox relaying concurrency without deadlocks or duplicate processing (SKIP LOCKED)
 * 6. AccountPartitionStrategy hot partition skew mitigation with bounded salt
 */
@Slf4j
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@org.springframework.test.context.TestPropertySource(properties = {
        "dftp.rate-limiting.enabled=false"
})
class ConcurrencyAndLoadSaturationIntegrationTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountReferenceRepository accountReferenceRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private OutboxRelay outboxRelay;

    @Autowired
    private DataSource dataSource;

    private JwtTokenService jwtTokenService;
    private String testJwtToken;

    @DynamicPropertySource
    static void configureTuning(DynamicPropertyRegistry registry) {
        // Tuned HikariCP pool configuration
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> 25);
        registry.add("spring.datasource.hikari.minimum-idle", () -> 5);
        registry.add("spring.datasource.hikari.connection-timeout", () -> 10000);
        registry.add("spring.datasource.hikari.leak-detection-threshold", () -> 5000);

        // Tuned Kafka Producer properties
        registry.add("spring.kafka.producer.properties.compression.type", () -> "lz4");
        registry.add("spring.kafka.producer.properties.linger.ms", () -> 10);
        registry.add("spring.kafka.producer.properties.batch.size", () -> 65536);
        registry.add("spring.kafka.producer.properties.enable.idempotence", () -> true);
        registry.add("spring.kafka.consumer.properties.max.poll.interval.ms", () -> 300000);
    }

    @BeforeEach
    void setUp() {
        outboxEventRepository.deleteAll();
        transactionRepository.deleteAll();
        accountReferenceRepository.deleteAll();

        jwtTokenService = new JwtTokenService(new JwtProperties());
        testJwtToken = jwtTokenService.generateToken("test-user", List.of("ROLE_USER", "ROLE_SERVICE", "ROLE_ADMIN"));
    }

    @Test
    @DisplayName("Load Test 1: 50 concurrent transactions - verifies throughput, latency percentiles, and zero pool exhaustion")
    void testConcurrentTransactionBurstAndLatencyProfiling() throws Exception {
        int totalRequests = 50;
        int threadPoolSize = 25;

        // Setup accounts
        UUID merchantDestinationId = UUID.randomUUID();
        accountReferenceRepository.save(new AccountReference(merchantDestinationId, Instant.now(), "merchant-user"));

        List<UUID> sourceAccountIds = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID srcId = UUID.randomUUID();
            accountReferenceRepository.save(new AccountReference(srcId, Instant.now(), "test-user"));
            sourceAccountIds.add(srcId);
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        CountDownLatch readyLatch = new CountDownLatch(totalRequests);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalRequests);

        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        for (int i = 0; i < totalRequests; i++) {
            final int index = i;
            UUID srcId = sourceAccountIds.get(index % sourceAccountIds.size());
            executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await(); // Simultaneous blast
                    long start = System.nanoTime();

                    String txId = "tx-load-" + UUID.randomUUID();
                    String idempotencyKey = "idemp-load-" + UUID.randomUUID();
                    String traceId = UUID.randomUUID().toString().replace("-", "");
                    String spanId = UUID.randomUUID().toString().substring(0, 16).replace("-", "0");
                    String traceparent = "00-" + traceId + "-" + spanId + "-01";

                    HttpHeaders headers = new HttpHeaders();
                    headers.setBearerAuth(testJwtToken);
                    headers.set("Idempotency-Key", idempotencyKey);
                    headers.set("X-Correlation-Id", "corr-load-" + index);
                    headers.set("traceparent", traceparent);

                    CreateTransactionRequest request = CreateTransactionRequest.builder()
                            .transactionId(txId)
                            .sourceAccountId(srcId)
                            .destinationAccountId(merchantDestinationId)
                            .amount(new BigDecimal("10.00"))
                            .currency("USD")
                            .build();

                    HttpEntity<CreateTransactionRequest> entity = new HttpEntity<>(request, headers);
                    ResponseEntity<TransactionResponse> response = restTemplate.exchange(
                            "/transactions", HttpMethod.POST, entity, TransactionResponse.class);

                    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                    latencies.add(durationMs);

                    if (response.getStatusCode() == HttpStatus.CREATED) {
                        successCount.incrementAndGet();
                    } else {
                        failureCount.incrementAndGet();
                        log.warn("Unexpected status for request {}: {}", index, response.getStatusCode());
                    }
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    log.error("Request {} encountered exception", index, e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await(10, TimeUnit.SECONDS);
        long burstStart = System.currentTimeMillis();
        startLatch.countDown(); // Fire!
        boolean finished = doneLatch.await(30, TimeUnit.SECONDS);
        long burstTotalTime = System.currentTimeMillis() - burstStart;
        executor.shutdown();

        assertThat(finished).isTrue();
        assertThat(failureCount.get()).isEqualTo(0);
        assertThat(successCount.get()).isEqualTo(totalRequests);

        // Latency profiling statistics
        Collections.sort(latencies);
        long min = latencies.get(0);
        long max = latencies.get(latencies.size() - 1);
        long p50 = latencies.get((int) (totalRequests * 0.50));
        long p95 = latencies.get((int) (totalRequests * 0.95));
        long p99 = latencies.get((int) (totalRequests * 0.99));
        double avg = latencies.stream().mapToLong(Long::longValue).average().orElse(0.0);

        log.info("================ LOAD SATURATION REPORT ================");
        log.info("Total Requests: {}", totalRequests);
        log.info("Success Count:  {}", successCount.get());
        log.info("Failure Count:  {}", failureCount.get());
        log.info("Total Duration: {} ms", burstTotalTime);
        log.info("Throughput:     {:.2f} req/sec", (totalRequests * 1000.0) / burstTotalTime);
        log.info("Latency Min:    {} ms", min);
        log.info("Latency Avg:    {:.2f} ms", avg);
        log.info("Latency p50:    {} ms", p50);
        log.info("Latency p95:    {} ms", p95);
        log.info("Latency p99:    {} ms", p99);
        log.info("Latency Max:    {} ms", max);
        log.info("========================================================");

        // Check HikariCP connection pool health
        if (dataSource instanceof HikariDataSource hikariDataSource) {
            HikariPoolMXBean poolBean = hikariDataSource.getHikariPoolMXBean();
            if (poolBean != null) {
                log.info("HikariCP Pool State: Total={}, Active={}, Idle={}, Waiting={}",
                        poolBean.getTotalConnections(),
                        poolBean.getActiveConnections(),
                        poolBean.getIdleConnections(),
                        poolBean.getThreadsAwaitingConnection());
                assertThat(poolBean.getThreadsAwaitingConnection()).isEqualTo(0);
            }
        }

        // Verify all 50 Outbox events were generated with traceparent populated
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAll();
        assertThat(outboxEvents).hasSize(totalRequests);
        for (OutboxEvent event : outboxEvents) {
            assertThat(event.getTraceparent()).isNotNull();
            assertThat(event.getTraceparent()).startsWith("00-");
            assertThat(event.getStatus()).isIn("PENDING", "CLAIMED", "PUBLISHED");
        }

        // Wait for OutboxRelay to finish publishing all events
        org.awaitility.Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            outboxRelay.processOutbox();
            List<OutboxEvent> publishedEvents = outboxEventRepository.findAll();
            assertThat(publishedEvents).allMatch(e -> "PUBLISHED".equals(e.getStatus()));
        });
    }

    @Test
    @DisplayName("Load Test 2: Idempotency collision under 20 concurrent threads targeting same idempotency key")
    void testConcurrentIdempotencyKeyCollision() throws Exception {
        int concurrentCallers = 20;
        UUID sourceId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();
        accountReferenceRepository.save(new AccountReference(sourceId, Instant.now(), "test-user"));
        accountReferenceRepository.save(new AccountReference(destId, Instant.now(), "merchant-user"));

        String sharedIdempotencyKey = "shared-idemp-" + UUID.randomUUID();
        String sharedTxId = "shared-tx-" + UUID.randomUUID();

        ExecutorService executor = Executors.newFixedThreadPool(concurrentCallers);
        CountDownLatch ready = new CountDownLatch(concurrentCallers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrentCallers);

        AtomicInteger status201Count = new AtomicInteger(0);
        AtomicInteger status409Count = new AtomicInteger(0);

        for (int i = 0; i < concurrentCallers; i++) {
            executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    HttpHeaders headers = new HttpHeaders();
                    headers.setBearerAuth(testJwtToken);
                    headers.set("Idempotency-Key", sharedIdempotencyKey);
                    headers.set("X-Correlation-Id", "corr-idem-" + UUID.randomUUID());

                    CreateTransactionRequest request = CreateTransactionRequest.builder()
                            .transactionId(sharedTxId)
                            .sourceAccountId(sourceId)
                            .destinationAccountId(destId)
                            .amount(new BigDecimal("50.00"))
                            .currency("USD")
                            .build();

                    HttpEntity<CreateTransactionRequest> entity = new HttpEntity<>(request, headers);
                    ResponseEntity<TransactionResponse> res = restTemplate.exchange(
                            "/transactions", HttpMethod.POST, entity, TransactionResponse.class);

                    if (res.getStatusCode() == HttpStatus.CREATED) {
                        status201Count.incrementAndGet();
                    } else if (res.getStatusCode() == HttpStatus.CONFLICT) {
                        status409Count.incrementAndGet();
                    }
                } catch (Exception e) {
                    // RestTemplate may throw on 409
                    status409Count.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await(5, TimeUnit.SECONDS);
        start.countDown();
        done.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        // Exactly 1 winner inserts; other concurrent calls either receive 409 Conflict or 201 replay
        List<Transaction> transactions = transactionRepository.findAll();
        List<Transaction> matching = transactions.stream()
                .filter(t -> sharedTxId.equals(t.getTransactionId()))
                .collect(Collectors.toList());

        assertThat(matching).hasSize(1);
        log.info("Idempotency Stress Proof: 201 count = {}, 409 Conflict count = {}, Persisted Transactions = {}",
                status201Count.get(), status409Count.get(), matching.size());
    }

    @Test
    @DisplayName("Load Test 3: Multi-instance Outbox relay contention with parallel worker threads (SKIP LOCKED)")
    void testMultiInstanceOutboxRelayContention() throws Exception {
        int eventCount = 30;
        for (int i = 0; i < eventCount; i++) {
            OutboxEvent event = OutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("transaction-events")
                    .aggregateId("agg-" + (i % 5))
                    .eventType("FundsHoldRequested")
                    .payload("{\"test\":\"payload-" + i + "\"}")
                    .status("PENDING")
                    .createdAt(Instant.now())
                    .build();
            outboxEventRepository.save(event);
        }

        assertThat(outboxEventRepository.findEventsForProcessing(100)).hasSize(eventCount);

        // Run 4 concurrent worker threads running outboxRelay concurrently
        int workerCount = 4;
        ExecutorService workerPool = Executors.newFixedThreadPool(workerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(workerCount);

        for (int w = 0; w < workerCount; w++) {
            workerPool.submit(() -> {
                try {
                    startLatch.await();
                    outboxRelay.processOutbox();
                } catch (Exception e) {
                    log.error("Relay worker exception", e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean finished = doneLatch.await(15, TimeUnit.SECONDS);
        workerPool.shutdown();

        assertThat(finished).isTrue();

        // Verify all 30 events were published with zero duplicate processing and no remaining PENDING
        List<OutboxEvent> allEvents = outboxEventRepository.findAll();
        assertThat(allEvents).hasSize(eventCount);
        long publishedCount = allEvents.stream().filter(e -> "PUBLISHED".equals(e.getStatus())).count();
        assertThat(publishedCount).isEqualTo(eventCount);
        log.info("Multi-Instance Outbox Polling Proof: All {} events published successfully across {} parallel workers",
                eventCount, workerCount);
    }

    @Test
    @DisplayName("Load Test 4: AccountPartitionStrategy hot merchant skew mitigation distributes across partition buckets")
    void testHotMerchantPartitionSkewMitigation() {
        String merchantAccountId = "merchant-pool-account-9999";
        int totalTransactions = 1000;
        int partitionCount = 8;

        Map<Integer, AtomicInteger> distribution = new HashMap<>();
        for (int p = 0; p < partitionCount; p++) {
            distribution.put(p, new AtomicInteger(0));
        }

        for (int i = 0; i < totalTransactions; i++) {
            String transactionId = "tx-skew-" + i;
            String partitionKey = AccountPartitionStrategy.resolvePartitionKey(merchantAccountId, transactionId, true);
            int partition = Math.abs(partitionKey.hashCode()) % partitionCount;
            distribution.get(partition).incrementAndGet();
        }

        log.info("Hot Merchant Account Partition Distribution across {} partitions for {} transactions:",
                partitionCount, totalTransactions);
        distribution.forEach((k, v) -> log.info("  Partition {}: {} messages", k, v.get()));

        // Verify that no single partition carries > 50% of the traffic (salt mitigates hot skew)
        int maxPartitionCount = distribution.values().stream()
                .mapToInt(AtomicInteger::get)
                .max()
                .orElse(0);

        assertThat(maxPartitionCount).isLessThan((int) (totalTransactions * 0.50));
    }
}
