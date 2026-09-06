package com.dftp.transaction.api;

import com.dftp.common.security.JwtTokenService;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class RestApiSecurityTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JwtTokenService jwtTokenService;

    @Test
    @DisplayName("SEC-REST-01: Anonymous POST to /transactions is rejected with 401 Unauthorized")
    void testAnonymousCreateTransactionRejected() throws Exception {
        mockMvc.perform(post("/transactions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Idempotency-Key", "test-key-123")
                        .content("{\"transactionId\":\"tx-123\",\"sourceAccountId\":\"00000000-0000-0000-0000-000000000001\",\"destinationAccountId\":\"00000000-0000-0000-0000-000000000002\",\"amount\":100.00,\"currency\":\"USD\"}"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value(401))
                .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Test
    @DisplayName("SEC-REST-02: Anonymous GET to /transactions/{id} is rejected with 401 Unauthorized")
    void testAnonymousGetTransactionRejected() throws Exception {
        mockMvc.perform(get("/transactions/" + UUID.randomUUID()))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value(401))
                .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Autowired
    private com.dftp.transaction.domain.TransactionRepository transactionRepository;

    @Test
    @DisplayName("SEC-REST-04: Object-Level Authorization Remediation — foreign transaction is concealed with 404 Not Found")
    void testTransactionObjectLevelAuthorizationAudit() throws Exception {
        // Given: A transaction created in the system owned by user-b
        com.dftp.transaction.domain.Transaction txB = com.dftp.transaction.domain.Transaction.builder()
                .id(UUID.randomUUID())
                .transactionId("tx-user-b-transfer-999")
                .ownerId("user-b")
                .sourceAccountId(UUID.randomUUID())
                .destinationAccountId(UUID.randomUUID())
                .amount(new java.math.BigDecimal("250.00"))
                .currency("USD")
                .status("COMPLETED")
                .build();
        transactionRepository.save(txB);

        // When: user-a with ROLE_USER attempts to read txB
        String userAToken = jwtTokenService.generateToken("user-a", List.of("ROLE_USER"));

        // Remediation Verified:
        // Under Phase 10 BOLA mitigation, foreign transaction is concealed with 404 Not Found.
        mockMvc.perform(get("/transactions/" + txB.getId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userAToken))
                .andExpect(status().isNotFound());

        mockMvc.perform(get("/transactions/by-transaction-id/" + txB.getTransactionId())
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userAToken))
                .andExpect(status().isNotFound());
    }
}
