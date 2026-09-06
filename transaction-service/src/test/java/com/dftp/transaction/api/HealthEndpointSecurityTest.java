package com.dftp.transaction.api;

import com.dftp.common.observability.DftpMetrics;
import com.dftp.transaction.AbstractTransactionIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {"dftp.outbox.relay.enabled=false", "management.endpoints.web.exposure.include=*"})
class HealthEndpointSecurityTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private DftpMetrics dftpMetrics;

    @Test
    @DisplayName("HEALTH-01: Public /actuator/health conceals sensitive internal connection strings from unauthenticated callers")
    void testPublicHealthHidesSensitiveDetails() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"))
                // Ensure sensitive DB / Kafka internals are NOT exposed publicly
                .andExpect(content().string(not(containsString("jdbc:postgresql"))))
                .andExpect(content().string(not(containsString("password"))))
                .andExpect(content().string(not(containsString("HikariPool"))));
    }

    @Test
    @DisplayName("HEALTH-02: Kubernetes liveness probe /actuator/health/liveness returns UP")
    void testLivenessProbe() throws Exception {
        mockMvc.perform(get("/actuator/health/liveness"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    @DisplayName("HEALTH-03: Kubernetes readiness probe /actuator/health/readiness returns UP when dependencies are ready")
    void testReadinessProbe() throws Exception {
        mockMvc.perform(get("/actuator/health/readiness"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    @DisplayName("HEALTH-04: Metrics endpoint /actuator/metrics exposes financial metrics without high-cardinality tags")
    void testMetricsEndpoint() throws Exception {
        dftpMetrics.recordTransactionStarted("TRANSFER");

        mockMvc.perform(get("/actuator/metrics/transactions.started"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("transactions.started"))
                .andExpect(jsonPath("$.measurements[0].value").value(1.0))
                .andExpect(jsonPath("$.availableTags[*].tag").value(org.hamcrest.Matchers.hasItems("type", "application")))
                .andExpect(jsonPath("$.availableTags[*].tag").value(not(org.hamcrest.Matchers.hasItem("transactionId"))));
    }
}
