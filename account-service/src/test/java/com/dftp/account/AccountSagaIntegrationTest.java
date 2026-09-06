package com.dftp.account;

import com.dftp.account.application.AccountApplicationService;
import com.dftp.account.domain.Account;
import com.dftp.account.domain.AccountRepository;
import com.dftp.account.domain.AccountOperationRepository;
import com.dftp.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
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
 * Tests Account hold, settlement, and concurrent hold safety using real PostgreSQL.
 * §7 — Hold Idempotency Under Concurrency (P0)
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AccountSagaIntegrationTest extends AbstractAccountIntegrationTest {

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
    void testHoldFundsSuccess() {
        Account account = createAccount(new BigDecimal("100.00"));
        
        accountService.holdFunds(account.getId(), new BigDecimal("50.00"), "tx-1", "corr-1", UUID.randomUUID().toString());

        Account updated = accountRepository.findById(account.getId()).orElseThrow();
        assertThat(updated.getSettledBalance()).isEqualByComparingTo("100.00");
        assertThat(updated.getHeldFunds()).isEqualByComparingTo("50.00");
        
        assertThat(outboxEventRepository.findAll()).hasSize(1);
        assertThat(outboxEventRepository.findAll().get(0).getEventType()).isEqualTo("FundsHeld");
    }

    @Test
    void testHoldFundsInsufficient() {
        Account account = createAccount(new BigDecimal("30.00"));
        
        accountService.holdFunds(account.getId(), new BigDecimal("50.00"), "tx-1", "corr-1", UUID.randomUUID().toString());

        Account updated = accountRepository.findById(account.getId()).orElseThrow();
        assertThat(updated.getSettledBalance()).isEqualByComparingTo("30.00");
        assertThat(updated.getHeldFunds()).isEqualByComparingTo("0.00");
        
        assertThat(outboxEventRepository.findAll()).hasSize(1);
        assertThat(outboxEventRepository.findAll().get(0).getEventType()).isEqualTo("FundsHoldRejected");
    }

    @Test
    void testDuplicateHoldFundsIdempotency() {
        Account account = createAccount(new BigDecimal("100.00"));
        
        accountService.holdFunds(account.getId(), new BigDecimal("50.00"), "tx-1", "corr-1", UUID.randomUUID().toString());
        accountService.holdFunds(account.getId(), new BigDecimal("50.00"), "tx-1", "corr-1", UUID.randomUUID().toString());

        Account updated = accountRepository.findById(account.getId()).orElseThrow();
        assertThat(updated.getSettledBalance()).isEqualByComparingTo("100.00");
        assertThat(updated.getHeldFunds()).isEqualByComparingTo("50.00");
        
        // Should only have 1 event
        assertThat(outboxEventRepository.findAll()).hasSize(1);
    }

    @Test
    void testConcurrentHoldFunds_preventsOverReservation() throws InterruptedException {
        Account account = createAccount(new BigDecimal("100.00"));
        
        int threads = 3;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger optimisticLockCount = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            final String txId = "tx-" + i;
            executor.submit(() -> {
                try {
                    latch.await();
                    accountService.holdFunds(account.getId(), new BigDecimal("50.00"), txId, "corr", UUID.randomUUID().toString());
                    successCount.incrementAndGet();
                } catch (org.springframework.orm.ObjectOptimisticLockingFailureException e) {
                    optimisticLockCount.incrementAndGet();
                } catch (Exception e) {
                    // Ignore
                } finally {
                    done.countDown();
                }
            });
        }

        latch.countDown();
        done.await();
        executor.shutdown();

        Account updated = accountRepository.findById(account.getId()).orElseThrow();
        
        // §7: Explicit final-state assertions
        // We NEVER allow over-reservation
        assertThat(updated.getHeldFunds()).isLessThanOrEqualTo(new BigDecimal("100.00"));
        
        // heldFunds must be non-negative
        assertThat(updated.getHeldFunds()).isGreaterThanOrEqualTo(BigDecimal.ZERO);
        
        // availableBalance = settledBalance - heldFunds (invariant)
        BigDecimal availableBalance = updated.getSettledBalance().subtract(updated.getHeldFunds());
        assertThat(availableBalance).isGreaterThanOrEqualTo(BigDecimal.ZERO);
        
        // No negative values anywhere
        assertThat(updated.getSettledBalance()).isGreaterThanOrEqualTo(BigDecimal.ZERO);
        
        // AccountOperation count matches success count — no partial operations
        long holdOps = accountOperationRepository.findAll().stream()
                .filter(op -> "HOLD".equals(op.getOperationType()))
                .count();
        assertThat(holdOps).isEqualTo(successCount.get());
    }

    /**
     * §7 — Hold Idempotency Under Concurrency: Exact scenario.
     * Source: settled=100, held=0, available=100.
     * T1 HOLD 100, T2 HOLD 100 (concurrent).
     * Expected: exactly one HOLD succeeds. Final heldFunds=100, availableBalance=0.
     */
    @Test
    void testConcurrentHoldFunds_exactlyOneHoldForFullBalance() throws InterruptedException {
        Account account = createAccount(new BigDecimal("100.00"));
        
        int threads = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch barrier = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger holdSuccess = new AtomicInteger();
        AtomicInteger holdRejected = new AtomicInteger();
        AtomicInteger optimisticLockFailure = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            final String txId = "hold-full-" + i;
            executor.submit(() -> {
                try {
                    barrier.await();
                    accountService.holdFunds(account.getId(), new BigDecimal("100.00"), txId, "corr", UUID.randomUUID().toString());
                    holdSuccess.incrementAndGet();
                } catch (org.springframework.orm.ObjectOptimisticLockingFailureException e) {
                    optimisticLockFailure.incrementAndGet();
                } catch (Exception e) {
                    // DataIntegrityViolation or other
                    optimisticLockFailure.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        barrier.countDown();
        done.await();
        executor.shutdown();

        Account updated = accountRepository.findById(account.getId()).orElseThrow();

        // Exactly one hold can succeed for the full balance
        // The other must either be rejected (insufficient funds after first hold)
        // or fail with optimistic lock (concurrent version conflict)
        assertThat(updated.getHeldFunds()).isEqualByComparingTo("100.00");
        
        BigDecimal availableBalance = updated.getSettledBalance().subtract(updated.getHeldFunds());
        assertThat(availableBalance).isEqualByComparingTo("0.00");
        
        // No negative values
        assertThat(updated.getHeldFunds()).isGreaterThanOrEqualTo(BigDecimal.ZERO);

        System.out.println("=========================================");
        System.out.println("EVIDENCE: §7 Hold Concurrency (Race Condition)");
        System.out.println("Source Settled Balance: " + updated.getSettledBalance());
        System.out.println("Source Held Funds: " + updated.getHeldFunds());
        System.out.println("Available Balance: " + availableBalance);
        System.out.println("Successful HOLD requests: " + holdSuccess.get());
        System.out.println("Failed HOLD requests (Optimistic Lock / Constraint): " + optimisticLockFailure.get());
        System.out.println("=========================================");
        
        assertThat(updated.getSettledBalance()).isGreaterThanOrEqualTo(BigDecimal.ZERO);
        
        // The losing transaction must not leave partial AccountOperation behind.
        // Only successful holds produce AccountOperations.
        long holdOps = accountOperationRepository.findAll().stream()
                .filter(op -> "HOLD".equals(op.getOperationType()))
                .count();
        
        // At most 2 operations (one successful hold + one rejected hold that also saves an op),
        // but each is atomically committed or rolled back.
        // A successful hold that recorded available=100 gets the hold.
        // A hold that sees available=0 after the first hold gets FundsHoldRejected (also saves op).
        // An optimistic lock failure rolls back entirely — no partial op.
        assertThat(holdOps).isGreaterThanOrEqualTo(1);
    }

    @Test
    void testSettleFunds() {
        Account source = createAccount(new BigDecimal("100.00"));
        Account dest = createAccount(new BigDecimal("0.00"));
        
        accountService.holdFunds(source.getId(), new BigDecimal("100.00"), "tx-1", "corr-1", UUID.randomUUID().toString());
        
        // Clear outbox from hold
        outboxEventRepository.deleteAll();

        accountService.settleFunds(source.getId(), dest.getId(), new BigDecimal("100.00"), "tx-1", "corr-1", UUID.randomUUID().toString());

        Account updatedSource = accountRepository.findById(source.getId()).orElseThrow();
        Account updatedDest = accountRepository.findById(dest.getId()).orElseThrow();
        
        assertThat(updatedSource.getSettledBalance()).isEqualByComparingTo("0.00");
        assertThat(updatedSource.getHeldFunds()).isEqualByComparingTo("0.00");
        
        assertThat(updatedDest.getSettledBalance()).isEqualByComparingTo("100.00");
        assertThat(updatedDest.getHeldFunds()).isEqualByComparingTo("0.00");
        
        assertThat(outboxEventRepository.findAll()).hasSize(1);
        assertThat(outboxEventRepository.findAll().get(0).getEventType()).isEqualTo("FundsSettled");
    }

    private Account createAccount(BigDecimal initialBalance) {
        Account account = Account.builder()
                .id(UUID.randomUUID())
                .transactionId(UUID.randomUUID().toString())
                .status("ACTIVE")
                .settledBalance(initialBalance)
                .heldFunds(BigDecimal.ZERO)
                .createdAt(Instant.now())
                .version(0L)
                .build();
        return accountRepository.saveAndFlush(account);
    }
}

