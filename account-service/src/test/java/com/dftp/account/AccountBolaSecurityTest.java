package com.dftp.account;

import com.dftp.account.domain.Account;
import com.dftp.account.domain.AccountRepository;
import com.dftp.common.security.JwtTokenService;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.springframework.http.MediaType;

@SpringBootTest
@AutoConfigureMockMvc
class AccountBolaSecurityTest extends AbstractAccountIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JwtTokenService jwtTokenService;

    @Autowired
    private AccountRepository accountRepository;

    @Test
    @DisplayName("BOLA-ACC-01: USER_A accessing Account_A (owned by USER_A) is ALLOWED (200 OK)")
    void testUserCanAccessOwnAccount() throws Exception {
        Account accountA = Account.builder()
                .id(UUID.randomUUID())
                .transactionId("tx-user-a-acc-" + UUID.randomUUID())
                .ownerId("user-a")
                .status("ACTIVE")
                .settledBalance(new BigDecimal("1000.00"))
                .heldFunds(BigDecimal.ZERO)
                .createdAt(Instant.now())
                .build();
        accountRepository.save(accountA);

        String tokenUserA = jwtTokenService.generateToken("user-a", List.of("ROLE_USER"));

        mockMvc.perform(get("/accounts/" + accountA.getId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserA))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accountId").value(accountA.getId().toString()))
                .andExpect(jsonPath("$.ownerId").value("user-a"))
                .andExpect(jsonPath("$.settledBalance").value(1000.00));
    }

    @Test
    @DisplayName("BOLA-ACC-02: USER_A accessing Account_B (owned by USER_B) is CONCEALED (404 Not Found)")
    void testUserCannotAccessForeignAccountConcealedWith404() throws Exception {
        Account accountB = Account.builder()
                .id(UUID.randomUUID())
                .transactionId("tx-user-b-acc-" + UUID.randomUUID())
                .ownerId("user-b")
                .status("ACTIVE")
                .settledBalance(new BigDecimal("5000.00"))
                .heldFunds(BigDecimal.ZERO)
                .createdAt(Instant.now())
                .build();
        accountRepository.save(accountB);

        String tokenUserA = jwtTokenService.generateToken("user-a", List.of("ROLE_USER"));

        // Must return 404 to prevent resource existence enumeration (BOLA mitigation with resource concealment)
        mockMvc.perform(get("/accounts/" + accountB.getId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserA))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("BOLA-ACC-03: ROLE_ADMIN accessing any account is ALLOWED (200 OK)")
    void testAdminCanAccessAnyAccount() throws Exception {
        Account accountB = Account.builder()
                .id(UUID.randomUUID())
                .transactionId("tx-user-b-admin-" + UUID.randomUUID())
                .ownerId("user-b")
                .status("ACTIVE")
                .settledBalance(new BigDecimal("2000.00"))
                .heldFunds(BigDecimal.ZERO)
                .createdAt(Instant.now())
                .build();
        accountRepository.save(accountB);

        String tokenAdmin = jwtTokenService.generateToken("admin-operator", List.of("ROLE_ADMIN"));

        mockMvc.perform(get("/accounts/" + accountB.getId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenAdmin))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accountId").value(accountB.getId().toString()))
                .andExpect(jsonPath("$.ownerId").value("user-b"));
    }

    @Test
    @DisplayName("BOLA-ACC-04: ROLE_SERVICE accessing any account is ALLOWED (200 OK)")
    void testServiceCanAccessAnyAccount() throws Exception {
        Account accountB = Account.builder()
                .id(UUID.randomUUID())
                .transactionId("tx-user-b-svc-" + UUID.randomUUID())
                .ownerId("user-b")
                .status("ACTIVE")
                .settledBalance(new BigDecimal("3000.00"))
                .heldFunds(BigDecimal.ZERO)
                .createdAt(Instant.now())
                .build();
        accountRepository.save(accountB);

        String tokenService = jwtTokenService.generateToken("reconciliation-service", List.of("ROLE_SERVICE"));

        mockMvc.perform(get("/accounts/" + accountB.getId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenService))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accountId").value(accountB.getId().toString()));
    }

    @Test
    @DisplayName("BOLA-ACC-05: Non-existent account returns 404 Not Found")
    void testUnknownAccountReturns404() throws Exception {
        String tokenUserA = jwtTokenService.generateToken("user-a", List.of("ROLE_USER"));

        mockMvc.perform(get("/accounts/" + UUID.randomUUID())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserA))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("BOLA-ACC-06: USER_A submitting ownerId=victim-user has ownership forced to USER_A")
    void testUserCannotSpoofOwnerIdOnAccountCreation() throws Exception {
        String tokenUserA = jwtTokenService.generateToken("user-a", List.of("ROLE_USER"));
        String txId = "tx-spoof-" + UUID.randomUUID();

        mockMvc.perform(post("/accounts")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserA)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(String.format("{\"transactionId\":\"%s\",\"ownerId\":\"victim-user\"}", txId)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.ownerId").value("user-a")); // Forced to authenticated principal
    }

    @Test
    @DisplayName("BOLA-ACC-07: ROLE_ADMIN submitting ownerId=victim-user is permitted to assign owner")
    void testAdminCanAssignExplicitOwnerIdOnAccountCreation() throws Exception {
        String tokenAdmin = jwtTokenService.generateToken("admin-ops", List.of("ROLE_ADMIN"));
        String txId = "tx-admin-create-" + UUID.randomUUID();

        mockMvc.perform(post("/accounts")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenAdmin)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(String.format("{\"transactionId\":\"%s\",\"ownerId\":\"assigned-client\"}", txId)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.ownerId").value("assigned-client"));
    }

    @Test
    @DisplayName("BOLA-ACC-08: Caller claiming subject system-unassigned cannot access unassigned account (404 Concealed)")
    void testSystemUnassignedSubjectCannotAccessUnassignedAccount() throws Exception {
        Account unassignedAcc = Account.builder()
                .id(UUID.randomUUID())
                .transactionId("tx-legacy-" + UUID.randomUUID())
                .ownerId("system-unassigned")
                .status("ACTIVE")
                .settledBalance(new BigDecimal("1000.00"))
                .heldFunds(BigDecimal.ZERO)
                .createdAt(Instant.now())
                .build();
        accountRepository.save(unassignedAcc);

        String spoofedToken = jwtTokenService.generateToken("system-unassigned", List.of("ROLE_USER"));

        mockMvc.perform(get("/accounts/" + unassignedAcc.getId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + spoofedToken))
                .andExpect(status().isNotFound());
    }
}
