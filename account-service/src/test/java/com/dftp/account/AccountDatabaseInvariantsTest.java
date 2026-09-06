package com.dftp.account;

import com.dftp.account.application.AccountApplicationService;
import com.dftp.account.domain.Account;
import com.dftp.account.domain.AccountOperationRepository;
import com.dftp.account.domain.AccountRepository;
import com.dftp.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test Classification: Database Integration Test
 *
 * §16 — Database Invariants
 *
 * Verifies that the database CHECK constraints and UNIQUE constraints
 * enforce financial safety independently of application logic.
 * <ul>
 *   <li>heldFunds >= 0</li>
 *   <li>settledBalance >= 0</li>
 *   <li>AccountOperation uniqueness: (transactionId, operationType, accountId)</li>
 * </ul>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AccountDatabaseInvariantsTest extends AbstractAccountIntegrationTest {

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
    @DisplayName("§16: CHECK constraint prevents heldFunds < 0")
    void testHeldFundsCannotBeNegative() {
        Account account = createAccount(new BigDecimal("100.00"));

        // Attempt to set heldFunds to a negative value directly
        account.setHeldFunds(new BigDecimal("-1.00"));

        assertThatThrownBy(() -> accountRepository.saveAndFlush(account))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("§16: CHECK constraint prevents settledBalance < 0")
    void testSettledBalanceCannotBeNegative() {
        Account account = createAccount(new BigDecimal("100.00"));

        account.setSettledBalance(new BigDecimal("-1.00"));

        assertThatThrownBy(() -> accountRepository.saveAndFlush(account))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("§16: availableBalance = settledBalance - heldFunds invariant holds after hold")
    void testAvailableBalanceInvariantAfterHold() {
        Account account = createAccount(new BigDecimal("100.00"));
        
        // Hold 60
        account.setHeldFunds(new BigDecimal("60.00"));
        accountRepository.saveAndFlush(account);

        Account updated = accountRepository.findById(account.getId()).orElseThrow();
        BigDecimal available = updated.getSettledBalance().subtract(updated.getHeldFunds());

        assertThat(available).isEqualByComparingTo("40.00");
        assertThat(updated.getHeldFunds()).isLessThanOrEqualTo(updated.getSettledBalance());
    }

    @Test
    @DisplayName("§16: UNIQUE constraint on (transactionId, operationType, accountId) prevents duplicate operations")
    void testAccountOperationUniquenessConstraint() {
        Account account = createAccount(new BigDecimal("100.00"));

        com.dftp.account.domain.AccountOperation op1 = com.dftp.account.domain.AccountOperation.builder()
                .id(UUID.randomUUID())
                .accountId(account.getId())
                .transactionId("tx-unique-test")
                .operationType("HOLD")
                .createdAt(Instant.now())
                .build();
        accountOperationRepository.saveAndFlush(op1);

        // Attempt to save another operation with the same (transactionId, operationType, accountId)
        com.dftp.account.domain.AccountOperation op2 = com.dftp.account.domain.AccountOperation.builder()
                .id(UUID.randomUUID())
                .accountId(account.getId())
                .transactionId("tx-unique-test")
                .operationType("HOLD")
                .createdAt(Instant.now())
                .build();

        assertThatThrownBy(() -> accountOperationRepository.saveAndFlush(op2))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("§16: Different operation types for same transaction are allowed")
    void testDifferentOperationTypesAllowed() {
        Account account = createAccount(new BigDecimal("100.00"));

        com.dftp.account.domain.AccountOperation holdOp = com.dftp.account.domain.AccountOperation.builder()
                .id(UUID.randomUUID())
                .accountId(account.getId())
                .transactionId("tx-multi-op")
                .operationType("HOLD")
                .createdAt(Instant.now())
                .build();
        accountOperationRepository.saveAndFlush(holdOp);

        com.dftp.account.domain.AccountOperation settleOp = com.dftp.account.domain.AccountOperation.builder()
                .id(UUID.randomUUID())
                .accountId(account.getId())
                .transactionId("tx-multi-op")
                .operationType("SETTLE")
                .createdAt(Instant.now())
                .build();
        accountOperationRepository.saveAndFlush(settleOp);

        // Both should exist — different operation types for same transaction
        assertThat(accountOperationRepository.findAll().stream()
                .filter(op -> "tx-multi-op".equals(op.getTransactionId()))
                .count()).isEqualTo(2);
    }

    // ──────────────────────────────────────────────────────────────

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
