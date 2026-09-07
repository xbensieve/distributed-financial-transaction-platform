package com.dftp.transaction.api;

import com.dftp.common.security.JwtTokenService;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import com.dftp.transaction.security.SecurityAuditLogRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class BatchAndReconciliationSecurityTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JwtTokenService jwtTokenService;

    @Autowired
    private SecurityAuditLogRepository securityAuditLogRepository;

    @BeforeEach
    void cleanLogs() {
        securityAuditLogRepository.deleteAll();
    }

    @Test
    @DisplayName("SEC-BATCH-RECON-01: Anonymous requests to batch settlement and reconciliation are rejected with 401")
    void testAnonymousAccessRejected() throws Exception {
        mockMvc.perform(post("/admin/batches"))
                .andExpect(status().isUnauthorized());

        mockMvc.perform(post("/admin/reconciliation/cross-service/run"))
                .andExpect(status().isUnauthorized());

        mockMvc.perform(get("/admin/reconciliation/anomalies"))
                .andExpect(status().isUnauthorized());

        mockMvc.perform(post("/admin/retention/purge"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("SEC-BATCH-RECON-02: Regular users with ROLE_USER are rejected with 403 Forbidden")
    void testNonAdminAccessForbidden() throws Exception {
        String userToken = jwtTokenService.generateToken("ordinary-user", List.of("ROLE_USER"));

        mockMvc.perform(post("/admin/batches")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"batchId\":\"TEST\"}"))
                .andExpect(status().isForbidden());

        mockMvc.perform(post("/admin/reconciliation/cross-service/run")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken))
                .andExpect(status().isForbidden());

        mockMvc.perform(get("/admin/reconciliation/anomalies")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken))
                .andExpect(status().isForbidden());

        mockMvc.perform(post("/admin/retention/purge")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken))
                .andExpect(status().isForbidden());
    }

    @Test
    @DisplayName("SEC-BATCH-RECON-03: Administrators with ROLE_ADMIN are permitted and generate audit trail")
    void testAdminAccessPermittedWithAudit() throws Exception {
        String adminToken = jwtTokenService.generateToken("admin-operator", List.of("ROLE_ADMIN"));

        mockMvc.perform(post("/admin/batches")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"batchId\":\"BATCH-SEC-TEST\",\"transactionIds\":[]}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.batchId").value("BATCH-SEC-TEST"));

        // Verify security audit log record
        assertThat(securityAuditLogRepository.count()).isGreaterThanOrEqualTo(1);
        var log = securityAuditLogRepository.findAll().getFirst();
        assertThat(log.getPrincipal()).isEqualTo("admin-operator");
        assertThat(log.getAction()).isEqualTo("CREATE_BATCH");
    }
}
