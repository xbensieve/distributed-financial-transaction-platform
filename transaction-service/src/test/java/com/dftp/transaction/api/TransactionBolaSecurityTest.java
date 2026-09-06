package com.dftp.transaction.api;

import com.dftp.common.security.JwtTokenService;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import com.dftp.transaction.domain.AccountReference;
import com.dftp.transaction.domain.AccountReferenceRepository;
import com.dftp.transaction.domain.Transaction;
import com.dftp.transaction.domain.TransactionRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class TransactionBolaSecurityTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JwtTokenService jwtTokenService;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private AccountReferenceRepository accountReferenceRepository;

    @Test
    @DisplayName("BOLA-TX-01: Initiating user (USER_A) accessing Transaction_A is ALLOWED (200 OK)")
    void testUserCanAccessOwnInitiatedTransaction() throws Exception {
        UUID srcId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();
        accountReferenceRepository.save(new AccountReference(srcId, Instant.now(), "user-a"));
        accountReferenceRepository.save(new AccountReference(destId, Instant.now(), "user-b"));

        Transaction txA = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId("tx-user-a-transfer-" + UUID.randomUUID())
                .ownerId("user-a")
                .sourceAccountId(srcId)
                .destinationAccountId(destId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("COMPLETED")
                .build();
        transactionRepository.save(txA);

        String tokenUserA = jwtTokenService.generateToken("user-a", List.of("ROLE_USER"));

        mockMvc.perform(get("/transactions/" + txA.getId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserA))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(txA.getId().toString()))
                .andExpect(jsonPath("$.ownerId").value("user-a"))
                .andExpect(jsonPath("$.amount").value(100.00));

        mockMvc.perform(get("/transactions/by-transaction-id/" + txA.getTransactionId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserA))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.amount").value(100.00));
    }

    @Test
    @DisplayName("BOLA-TX-02: Counterparty recipient (USER_B) accessing Transaction_A is ALLOWED (200 OK)")
    void testRecipientCanAccessTransaction() throws Exception {
        UUID srcId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();
        accountReferenceRepository.save(new AccountReference(srcId, Instant.now(), "user-a"));
        accountReferenceRepository.save(new AccountReference(destId, Instant.now(), "user-b"));

        Transaction txA = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId("tx-counterparty-" + UUID.randomUUID())
                .ownerId("user-a")
                .sourceAccountId(srcId)
                .destinationAccountId(destId)
                .amount(new BigDecimal("75.00"))
                .currency("USD")
                .status("COMPLETED")
                .build();
        transactionRepository.save(txA);

        String tokenUserB = jwtTokenService.generateToken("user-b", List.of("ROLE_USER"));

        mockMvc.perform(get("/transactions/" + txA.getId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserB))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.amount").value(75.00));
    }

    @Test
    @DisplayName("BOLA-TX-03: Unrelated third party (USER_C) accessing Transaction_A is CONCEALED (404 Not Found)")
    void testUnrelatedUserCannotAccessTransactionConcealedWith404() throws Exception {
        UUID srcId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();
        accountReferenceRepository.save(new AccountReference(srcId, Instant.now(), "user-a"));
        accountReferenceRepository.save(new AccountReference(destId, Instant.now(), "user-b"));

        Transaction txA = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId("tx-private-" + UUID.randomUUID())
                .ownerId("user-a")
                .sourceAccountId(srcId)
                .destinationAccountId(destId)
                .amount(new BigDecimal("500.00"))
                .currency("USD")
                .status("COMPLETED")
                .build();
        transactionRepository.save(txA);

        String tokenUserC = jwtTokenService.generateToken("user-c", List.of("ROLE_USER"));

        // Both by DB ID and by transactionId must return 404 to prevent resource existence disclosure
        mockMvc.perform(get("/transactions/" + txA.getId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserC))
                .andExpect(status().isNotFound());

        mockMvc.perform(get("/transactions/by-transaction-id/" + txA.getTransactionId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserC))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("BOLA-TX-04: ROLE_ADMIN accessing any transaction is ALLOWED (200 OK)")
    void testAdminCanAccessAnyTransaction() throws Exception {
        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId("tx-admin-view-" + UUID.randomUUID())
                .ownerId("user-x")
                .sourceAccountId(UUID.randomUUID())
                .destinationAccountId(UUID.randomUUID())
                .amount(new BigDecimal("300.00"))
                .currency("USD")
                .status("COMPLETED")
                .build();
        transactionRepository.save(tx);

        String tokenAdmin = jwtTokenService.generateToken("admin-auditor", List.of("ROLE_ADMIN"));

        mockMvc.perform(get("/transactions/" + tx.getId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenAdmin))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.amount").value(300.00));
    }

    @Autowired
    private com.dftp.common.outbox.OutboxEventRepository outboxEventRepository;

    @Autowired
    private com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    @Test
    @DisplayName("BOLA-TX-05: Non-existent transaction returns 404 Not Found")
    void testUnknownTransactionReturns404() throws Exception {
        String tokenUserA = jwtTokenService.generateToken("user-a", List.of("ROLE_USER"));

        mockMvc.perform(get("/transactions/" + UUID.randomUUID())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserA))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("BOLA-TX-06: USER_A initiating transfer from own source account is ALLOWED (201 Created)")
    void testUserCanInitiateTransferFromOwnSourceAccount() throws Exception {
        UUID srcId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();
        accountReferenceRepository.save(new AccountReference(srcId, Instant.now(), "user-a"));
        accountReferenceRepository.save(new AccountReference(destId, Instant.now(), "user-b"));

        String txId = "tx-init-own-" + UUID.randomUUID();
        String idempotencyKey = UUID.randomUUID().toString();
        String tokenUserA = jwtTokenService.generateToken("user-a", List.of("ROLE_USER"));

        com.dftp.transaction.api.dto.CreateTransactionRequest request =
                new com.dftp.transaction.api.dto.CreateTransactionRequest(
                        txId, srcId, destId, new BigDecimal("150.00"), "USD");

        mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserA)
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.transactionId").value(txId))
                .andExpect(jsonPath("$.status").value("PENDING"));

        org.assertj.core.api.Assertions.assertThat(transactionRepository.findByTransactionId(txId)).isPresent();
    }

    @Test
    @DisplayName("BOLA-TX-07: USER_A initiating transfer from USER_B source account is REJECTED (403 Forbidden) with ZERO financial side effects")
    void testUserCannotInitiateTransferFromForeignSourceAccountZeroSideEffects() throws Exception {
        UUID srcId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();
        accountReferenceRepository.save(new AccountReference(srcId, Instant.now(), "user-b")); // Owned by user-b
        accountReferenceRepository.save(new AccountReference(destId, Instant.now(), "user-c"));

        String txId = "tx-steal-attempt-" + UUID.randomUUID();
        String idempotencyKey = UUID.randomUUID().toString();
        String tokenUserA = jwtTokenService.generateToken("user-a", List.of("ROLE_USER")); // Attacker: user-a

        com.dftp.transaction.api.dto.CreateTransactionRequest request =
                new com.dftp.transaction.api.dto.CreateTransactionRequest(
                        txId, srcId, destId, new BigDecimal("999.00"), "USD");

        mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserA)
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isForbidden());

        // CRITICAL FINANCIAL INVARIANT: Zero database row, zero outbox event, zero hold requested
        org.assertj.core.api.Assertions.assertThat(transactionRepository.findByTransactionId(txId)).isEmpty();
        long outboxEventsForTx = outboxEventRepository.findAll().stream()
                .filter(e -> e.getPayload() != null && e.getPayload().contains(txId))
                .count();
        org.assertj.core.api.Assertions.assertThat(outboxEventsForTx).isZero();
    }

    @Test
    @DisplayName("BOLA-TX-08: Initiating transfer from non-existent source account returns 400 Bad Request")
    void testInitiatingTransferFromNonExistentSourceAccountReturnsBadRequest() throws Exception {
        UUID randomSrcId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();
        accountReferenceRepository.save(new AccountReference(destId, Instant.now(), "user-b"));

        String txId = "tx-unknown-src-" + UUID.randomUUID();
        String tokenUserA = jwtTokenService.generateToken("user-a", List.of("ROLE_USER"));

        com.dftp.transaction.api.dto.CreateTransactionRequest request =
                new com.dftp.transaction.api.dto.CreateTransactionRequest(
                        txId, randomSrcId, destId, new BigDecimal("100.00"), "USD");

        mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserA)
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("BOLA-TX-09: ROLE_ADMIN initiating transfer from any account is ALLOWED by operational policy (201 Created)")
    void testAdminCanInitiateTransferByPolicy() throws Exception {
        UUID srcId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();
        accountReferenceRepository.save(new AccountReference(srcId, Instant.now(), "user-client"));
        accountReferenceRepository.save(new AccountReference(destId, Instant.now(), "user-counterparty"));

        String txId = "tx-admin-init-" + UUID.randomUUID();
        String tokenAdmin = jwtTokenService.generateToken("admin-ops", List.of("ROLE_ADMIN"));

        com.dftp.transaction.api.dto.CreateTransactionRequest request =
                new com.dftp.transaction.api.dto.CreateTransactionRequest(
                        txId, srcId, destId, new BigDecimal("50.00"), "USD");

        mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenAdmin)
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.transactionId").value(txId));
    }

    @Test
    @DisplayName("BOLA-TX-10: ROLE_SERVICE initiating transfer is ALLOWED via service-to-service trust (201 Created)")
    void testServiceCanInitiateTransfer() throws Exception {
        UUID srcId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();
        accountReferenceRepository.save(new AccountReference(srcId, Instant.now(), "user-client"));
        accountReferenceRepository.save(new AccountReference(destId, Instant.now(), "user-counterparty"));

        String txId = "tx-service-init-" + UUID.randomUUID();
        String tokenService = jwtTokenService.generateToken("recurring-billing-service", List.of("ROLE_SERVICE"));

        com.dftp.transaction.api.dto.CreateTransactionRequest request =
                new com.dftp.transaction.api.dto.CreateTransactionRequest(
                        txId, srcId, destId, new BigDecimal("25.00"), "USD");

        mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenService)
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.transactionId").value(txId));
    }

    @Test
    @DisplayName("BOLA-TX-11: Anonymous caller initiating transfer is REJECTED (401 Unauthorized)")
    void testAnonymousCannotInitiateTransaction() throws Exception {
        UUID srcId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();

        com.dftp.transaction.api.dto.CreateTransactionRequest request =
                new com.dftp.transaction.api.dto.CreateTransactionRequest(
                        "tx-anon-" + UUID.randomUUID(), srcId, destId, new BigDecimal("10.00"), "USD");

        mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/transactions")
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("BOLA-TX-12: Principal claiming subject system-unassigned cannot initiate transfer from unassigned account (403 Forbidden)")
    void testSystemUnassignedSubjectCannotInitiateTransaction() throws Exception {
        UUID srcId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();
        accountReferenceRepository.save(new AccountReference(srcId, Instant.now(), "system-unassigned"));
        accountReferenceRepository.save(new AccountReference(destId, Instant.now(), "user-b"));

        String txId = "tx-spoof-unassigned-" + UUID.randomUUID();
        String spoofedToken = jwtTokenService.generateToken("system-unassigned", List.of("ROLE_USER"));

        com.dftp.transaction.api.dto.CreateTransactionRequest request =
                new com.dftp.transaction.api.dto.CreateTransactionRequest(
                        txId, srcId, destId, new BigDecimal("100.00"), "USD");

        mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + spoofedToken)
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isForbidden());

        org.assertj.core.api.Assertions.assertThat(transactionRepository.findByTransactionId(txId)).isEmpty();
    }

    @Test
    @DisplayName("BOLA-TX-13: Principal claiming subject system-unassigned cannot read unassigned transaction (404 Concealed)")
    void testSystemUnassignedSubjectCannotReadUnassignedTransaction() throws Exception {
        UUID srcId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();
        accountReferenceRepository.save(new AccountReference(srcId, Instant.now(), "system-unassigned"));
        accountReferenceRepository.save(new AccountReference(destId, Instant.now(), "system-unassigned"));

        Transaction tx = Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId("tx-legacy-" + UUID.randomUUID())
                .ownerId("system-unassigned")
                .sourceAccountId(srcId)
                .destinationAccountId(destId)
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .status("COMPLETED")
                .build();
        transactionRepository.save(tx);

        String spoofedToken = jwtTokenService.generateToken("system-unassigned", List.of("ROLE_USER"));

        mockMvc.perform(get("/transactions/" + tx.getId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + spoofedToken))
                .andExpect(status().isNotFound());
    }
}
