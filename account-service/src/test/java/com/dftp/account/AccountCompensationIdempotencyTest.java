package com.dftp.account;

import com.dftp.account.application.AccountApplicationService;
import com.dftp.account.domain.Account;
import com.dftp.account.domain.AccountOperation;
import com.dftp.account.domain.AccountOperationRepository;
import com.dftp.account.domain.AccountRepository;
import com.dftp.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test Classification: Database Integration Test
 *
 * §6 — Compensation Idempotency (P0)
 *
 * Dedicated compensation idempotency tests. Each test exercises
 * {@code AccountApplicationService.compensateHold()} and verifies:
 * <ul>
 *   <li>Exactly one compensation financial effect</li>
 *   <li>Exactly one AccountOperation(COMPENSATE)</li>
 *   <li>heldFunds never goes negative</li>
 * </ul>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AccountCompensationIdempotencyTest extends AbstractAccountIntegrationTest {

    @Autowired
    private AccountApplicationService accountService;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private AccountOperationRepository accountOperationRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void setUp() {
        outboxEventRepository.deleteAll();
        accountOperationRepository.deleteAll();
        accountRepository.deleteAll();
    }

    @Test
    @DisplayName("§6-A: Duplicate compensation with same transactionId releases funds once")
    void testDuplicateCompensationSameEventId() {
        Account account = createAccountWithHold(new BigDecimal("100.00"), new BigDecimal("50.00"), "tx-comp-1");
        outboxEventRepository.deleteAll();

        // First compensation
        accountService.compensateHold(account.getId(), new BigDecimal("50.00"), "tx-comp-1", "corr-1", UUID.randomUUID().toString());

        // Second compensation — same transactionId (redelivery)
        accountService.compensateHold(account.getId(), new BigDecimal("50.00"), "tx-comp-1", "corr-1", UUID.randomUUID().toString());

        Account updated = accountRepository.findById(account.getId()).orElseThrow();
        System.out.println("EVIDENCE §6-A: heldFunds before=" + account.getHeldFunds() + ", after=" + updated.getHeldFunds());

        // One compensation effect
        assertThat(updated.getHeldFunds()).isEqualByComparingTo("0.00");
        assertThat(updated.getSettledBalance()).isEqualByComparingTo("100.00");

        // heldFunds never negative
        assertThat(updated.getHeldFunds()).isGreaterThanOrEqualTo(BigDecimal.ZERO);

        // Exactly one COMPENSATE operation
        long compensateOps = accountOperationRepository.findAll().stream()
                .filter(op -> "COMPENSATE".equals(op.getOperationType()) && op.getAccountId().equals(account.getId()))
                .count();
        assertThat(compensateOps).isEqualTo(1);

        // One outbox event
        assertThat(outboxEventRepository.findAll().stream()
                .filter(e -> "HoldCompensated".equals(e.getEventType()))
                .count()).isEqualTo(1);
    }

    @Test
    @DisplayName("§6-B: Different eventIds for same transactionId produce one compensation effect")
    void testDuplicateCompensationDifferentEventIds() {
        Account account = createAccountWithHold(new BigDecimal("200.00"), new BigDecimal("200.00"), "tx-comp-2");
        outboxEventRepository.deleteAll();

        // E1 / T1
        accountService.compensateHold(account.getId(), new BigDecimal("200.00"), "tx-comp-2", "corr-2", UUID.randomUUID().toString());

        // E2 / T1 — different eventId, same transactionId
        accountService.compensateHold(account.getId(), new BigDecimal("200.00"), "tx-comp-2", "corr-2", UUID.randomUUID().toString());

        Account updated = accountRepository.findById(account.getId()).orElseThrow();
        System.out.println("EVIDENCE §6-B: heldFunds before=" + account.getHeldFunds() + ", after=" + updated.getHeldFunds());

        assertThat(updated.getHeldFunds()).isEqualByComparingTo("0.00");
        assertThat(updated.getSettledBalance()).isEqualByComparingTo("200.00");
        assertThat(updated.getHeldFunds()).isGreaterThanOrEqualTo(BigDecimal.ZERO);
    }

    @Test
    @DisplayName("§6-C: Concurrent compensation releases funds exactly once, heldFunds never < 0")
    void testConcurrentCompensation() throws InterruptedException {
        Account account = createAccountWithHold(new BigDecimal("100.00"), new BigDecimal("100.00"), "tx-comp-conc");
        outboxEventRepository.deleteAll();

        int threads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch barrier = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failCount = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    barrier.await();
                    accountService.compensateHold(account.getId(), new BigDecimal("100.00"),
                            "tx-comp-conc", "corr-c", UUID.randomUUID().toString());
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        barrier.countDown();
        done.await();
        executor.shutdown();

        // Final database state is the authority
        Account updated = accountRepository.findById(account.getId()).orElseThrow();
        System.out.println("EVIDENCE §6-C: heldFunds before=" + account.getHeldFunds() + ", after=" + updated.getHeldFunds());

        assertThat(updated.getHeldFunds()).isEqualByComparingTo("0.00");
        assertThat(updated.getSettledBalance()).isEqualByComparingTo("100.00");

        // CRITICAL: heldFunds must NEVER be negative
        assertThat(updated.getHeldFunds()).isGreaterThanOrEqualTo(BigDecimal.ZERO);

        // Exactly one logical COMPENSATE operation
        long compensateOps = accountOperationRepository.findAll().stream()
                .filter(op -> "COMPENSATE".equals(op.getOperationType()) && op.getAccountId().equals(account.getId()))
                .count();
        assertThat(compensateOps).isEqualTo(1);
    }

    // ──────────────────────────────────────────────────────────────

    /**
     * Creates an account with a pre-existing hold.
     * This simulates the state after a successful FundsHoldRequested → FundsHeld cycle.
     */
    private Account createAccountWithHold(BigDecimal settledBalance, BigDecimal heldAmount, String txId) {
        Account account = Account.builder()
                .id(UUID.randomUUID())
                .transactionId(UUID.randomUUID().toString())
                .status("ACTIVE")
                .settledBalance(settledBalance)
                .heldFunds(BigDecimal.ZERO)
                .createdAt(Instant.now())
                .version(0L)
                .build();
        account = accountRepository.saveAndFlush(account);

        // Create the hold via the application service (so AccountOperation is recorded)
        accountService.holdFunds(account.getId(), heldAmount, txId, "corr-setup", UUID.randomUUID().toString());

        return accountRepository.findById(account.getId()).orElseThrow();
    }
}
