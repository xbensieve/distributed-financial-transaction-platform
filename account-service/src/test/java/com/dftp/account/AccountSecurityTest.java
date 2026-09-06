package com.dftp.account;

import com.dftp.account.api.dto.CreateAccountRequest;
import com.dftp.account.domain.Account;
import com.dftp.account.domain.AccountRepository;
import com.dftp.common.security.JwtTokenService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class AccountSecurityTest extends AbstractAccountIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JwtTokenService jwtTokenService;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("SEC-ACC-01: Anonymous request to /accounts/{id} returns 401 Unauthorized")
    void testAnonymousGetAccountRejected() throws Exception {
        mockMvc.perform(get("/accounts/" + UUID.randomUUID()))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value(401))
                .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Test
    @DisplayName("SEC-ACC-02: Anonymous request to POST /accounts/{id}/credit returns 401 Unauthorized")
    void testAnonymousCreditAccountRejected() throws Exception {
        mockMvc.perform(post("/accounts/" + UUID.randomUUID() + "/credit?amount=100.00"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value(401))
                .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Test
    @DisplayName("SEC-ACC-03: Authenticated ROLE_USER cannot credit account (returns 403 Forbidden)")
    void testRoleUserCannotCreditAccount() throws Exception {
        String userToken = jwtTokenService.generateToken("regular-user", List.of("ROLE_USER"));
        UUID accountId = UUID.randomUUID();

        mockMvc.perform(post("/accounts/" + accountId + "/credit?amount=1000000.00")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.status").value(403))
                .andExpect(jsonPath("$.error").value("Forbidden"))
                .andExpect(jsonPath("$.message").value("Access denied: insufficient privileges"));
    }

    @Test
    @DisplayName("SEC-ACC-03B: Authenticated ROLE_SERVICE cannot credit account (returns 403 Forbidden)")
    void testRoleServiceCannotCreditAccount() throws Exception {
        String serviceToken = jwtTokenService.generateToken("service-worker", List.of("ROLE_SERVICE"));
        UUID accountId = UUID.randomUUID();

        mockMvc.perform(post("/accounts/" + accountId + "/credit?amount=1000000.00")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + serviceToken))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.status").value(403))
                .andExpect(jsonPath("$.error").value("Forbidden"))
                .andExpect(jsonPath("$.message").value("Access denied: insufficient privileges"));
    }

    @Test
    @DisplayName("SEC-ACC-04: Authenticated ROLE_ADMIN can invoke creditAccount (for administrative/test seed)")
    void testRoleAdminCanCreditAccount() throws Exception {
        Account account = Account.builder()
                .id(UUID.randomUUID())
                .transactionId(UUID.randomUUID().toString())
                .status("ACTIVE")
                .settledBalance(BigDecimal.ZERO)
                .heldFunds(BigDecimal.ZERO)
                .createdAt(Instant.now())
                .build();
        accountRepository.save(account);

        String adminToken = jwtTokenService.generateToken("admin-operator", List.of("ROLE_ADMIN"));

        mockMvc.perform(post("/accounts/" + account.getId() + "/credit?amount=500.00")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken))
                .andExpect(status().isOk());

        Account updated = accountRepository.findById(account.getId()).orElseThrow();
        assertThat(updated.getSettledBalance()).isEqualByComparingTo("500.00");
    }

    @Test
    @DisplayName("SEC-ACC-05: Object-Level Authorization Remediation — foreign account is concealed with 404 Not Found")
    void testObjectLevelAuthorizationAudit() throws Exception {
        // Given: An account exists in the platform owned by user-b
        Account accountB = Account.builder()
                .id(UUID.randomUUID())
                .transactionId("tx-user-b-account-" + UUID.randomUUID())
                .ownerId("user-b")
                .status("ACTIVE")
                .settledBalance(new BigDecimal("1500.00"))
                .heldFunds(BigDecimal.ZERO)
                .createdAt(Instant.now())
                .build();
        accountRepository.save(accountB);

        // When: user-a with ROLE_USER attempts to read accountB
        String tokenUserA = jwtTokenService.generateToken("user-a", List.of("ROLE_USER"));

        // Remediation Verified:
        // Under Phase 10 BOLA mitigation, foreign account access is concealed with 404 Not Found.
        mockMvc.perform(get("/accounts/" + accountB.getId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUserA))
                .andExpect(status().isNotFound());
    }
}
