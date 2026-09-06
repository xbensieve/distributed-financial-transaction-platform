package com.dftp.transaction.api;

import com.dftp.common.security.JwtTokenService;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import com.dftp.transaction.application.DltReplayService;
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
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class AdminControllerSecurityTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JwtTokenService jwtTokenService;

    @Autowired
    private SecurityAuditLogRepository securityAuditLogRepository;

    @MockBean
    private DltReplayService dltReplayService;

    @BeforeEach
    void cleanLogs() {
        securityAuditLogRepository.deleteAll();
    }

    @Test
    @DisplayName("SEC-ADMIN-01: Anonymous request to DLT replay is rejected with 401 Unauthorized")
    void testAnonymousAccessRejected() throws Exception {
        mockMvc.perform(post("/admin/dlt/replay/transaction-events"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value(401))
                .andExpect(jsonPath("$.error").value("Unauthorized"));

        verifyNoInteractions(dltReplayService);
        assertThat(securityAuditLogRepository.count()).isEqualTo(0);
    }

    @Test
    @DisplayName("SEC-ADMIN-02: Non-admin user (ROLE_USER) is rejected with 403 Forbidden")
    void testNonAdminAccessForbidden() throws Exception {
        String userToken = jwtTokenService.generateToken("regular-user", List.of("ROLE_USER"));

        mockMvc.perform(post("/admin/dlt/replay/transaction-events")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.status").value(403))
                .andExpect(jsonPath("$.error").value("Forbidden"));

        verifyNoInteractions(dltReplayService);
        assertThat(securityAuditLogRepository.count()).isEqualTo(0);
    }

    @Test
    @DisplayName("SEC-ADMIN-03: Authorized admin (ROLE_ADMIN) succeeds with 200 OK and persists audit log")
    void testAdminAccessAllowed() throws Exception {
        String adminToken = jwtTokenService.generateToken("admin-user", List.of("ROLE_ADMIN"));
        when(dltReplayService.replayDlt(eq("transaction-events.DLT"), eq("transaction-events"), anyInt()))
                .thenReturn(5);

        mockMvc.perform(post("/admin/dlt/replay/transaction-events")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .header("X-Correlation-Id", "corr-admin-01")
                        .param("maxRecords", "50"))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Successfully replayed 5 messages")));

        verify(dltReplayService, times(1)).replayDlt("transaction-events.DLT", "transaction-events", 50);

        List<SecurityAuditLog> logs = securityAuditLogRepository.findByTargetResourceOrderByCreatedAtDesc("transaction-events");
        assertThat(logs).hasSize(1);
        SecurityAuditLog log = logs.get(0);
        assertThat(log.getPrincipal()).isEqualTo("admin-user");
        assertThat(log.getAction()).isEqualTo("DLT_REPLAY");
        assertThat(log.getStatus()).isEqualTo("SUCCESS");
        assertThat(log.getCorrelationId()).isEqualTo("corr-admin-01");
        assertThat(log.getDetails()).contains("\"replayedCount\": 5");
    }

    @Test
    @DisplayName("SEC-ADMIN-04: Non-allowlisted topic is rejected with 400 Bad Request and logged")
    void testInvalidTopicRejected() throws Exception {
        String adminToken = jwtTokenService.generateToken("admin-user", List.of("ROLE_ADMIN"));

        mockMvc.perform(post("/admin/dlt/replay/malicious-internal-topic")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(dltReplayService);

        List<SecurityAuditLog> logs = securityAuditLogRepository.findByTargetResourceOrderByCreatedAtDesc("malicious-internal-topic");
        assertThat(logs).hasSize(1);
        SecurityAuditLog log = logs.get(0);
        assertThat(log.getPrincipal()).isEqualTo("admin-user");
        assertThat(log.getStatus()).isEqualTo("REJECTED_INVALID_TOPIC");
    }

    @Test
    @DisplayName("SEC-ADMIN-05: Oversized batch (> 100) is rejected with 400 Bad Request and logged")
    void testOversizedBatchRejected() throws Exception {
        String adminToken = jwtTokenService.generateToken("admin-user", List.of("ROLE_ADMIN"));

        mockMvc.perform(post("/admin/dlt/replay/account-events")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .param("maxRecords", "500"))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(dltReplayService);

        List<SecurityAuditLog> logs = securityAuditLogRepository.findByTargetResourceOrderByCreatedAtDesc("account-events");
        assertThat(logs).hasSize(1);
        SecurityAuditLog log = logs.get(0);
        assertThat(log.getPrincipal()).isEqualTo("admin-user");
        assertThat(log.getStatus()).isEqualTo("REJECTED_INVALID_BATCH_SIZE");
    }

    @Test
    @DisplayName("SEC-ADMIN-06: Rapid repeated replay of same topic is blocked with 429 and logged")
    void testReplayAbuseCooldown() throws Exception {
        String adminToken = jwtTokenService.generateToken("admin-user", List.of("ROLE_ADMIN"));
        when(dltReplayService.replayDlt(anyString(), anyString(), anyInt())).thenReturn(1);

        // First call succeeds
        mockMvc.perform(post("/admin/dlt/replay/ledger-events")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken))
                .andExpect(status().isOk());

        // Immediate second call should hit cooldown (429)
        mockMvc.perform(post("/admin/dlt/replay/ledger-events")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken))
                .andExpect(status().isTooManyRequests());

        List<SecurityAuditLog> logs = securityAuditLogRepository.findByTargetResourceOrderByCreatedAtDesc("ledger-events");
        assertThat(logs).hasSize(2);
        assertThat(logs.stream().map(SecurityAuditLog::getStatus)).containsExactlyInAnyOrder("SUCCESS", "REJECTED_COOLDOWN_ACTIVE");
    }
}
